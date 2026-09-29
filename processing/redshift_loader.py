"""Loads new Parquet files from S3 into the warehouse (Redshift or Postgres, see WAREHOUSE)."""
import io
import logging
import time

import boto3
import pyarrow.parquet as pq

from config import settings
from processing import warehouse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("redshift_loader")

# table name -> (S3 prefix, columns, CREATE statement). Column order must match the Parquet files.
TABLES = {
    "crypto_trades": (settings.S3_CRYPTO_PREFIX, ("symbol", "price", "quantity", "timestamp", "anomaly"), """
        CREATE TABLE IF NOT EXISTS crypto_trades (
            symbol      VARCHAR(20),
            price       FLOAT,
            quantity    FLOAT,
            timestamp   BIGINT,
            anomaly     VARCHAR(10)
        );
    """),
    "stock_prices": (settings.S3_STOCK_PREFIX, ("symbol", "price", "volume", "timestamp"), """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol      VARCHAR(10),
            price       FLOAT,
            volume      BIGINT,
            timestamp   VARCHAR(20)
        );
    """),
}

# Remembers which S3 files are already in the warehouse, so each run only loads new ones.
LOADED_FILES_DDL = """
    CREATE TABLE loaded_files (
        s3_key      VARCHAR(1024) PRIMARY KEY,
        table_name  VARCHAR(64),
        loaded_at   TIMESTAMP DEFAULT {now}
    );
""".format(now="GETDATE()" if settings.WAREHOUSE == "redshift" else "CURRENT_TIMESTAMP")


def is_data_file(key):
    """True for finished Parquet data files; False for Spark's metadata and in-progress output."""
    if not key.endswith(".parquet"):
        return False
    return not any(part.startswith("_") for part in key.split("/"))


def list_parquet_keys(s3, prefix):
    """List every Parquet file under prefix.

    list_objects_v2 returns at most 1,000 keys per call, so use a paginator
    to walk through all the pages.
    """
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []) if is_data_file(obj["Key"]))
    return keys


def parquet_rows(data, columns):
    """Read a Parquet file's bytes into row tuples, in the given column order."""
    table = pq.read_table(io.BytesIO(data), columns=list(columns))
    return [tuple(row[c] for c in columns) for row in table.to_pylist()]


def copy_credentials():
    if settings.REDSHIFT_IAM_ROLE:
        return f"IAM_ROLE '{settings.REDSHIFT_IAM_ROLE}'"
    return (
        f"ACCESS_KEY_ID '{settings.AWS_ACCESS_KEY_ID}' "
        f"SECRET_ACCESS_KEY '{settings.AWS_SECRET_ACCESS_KEY}'"
    )


def load_file(cursor, s3, table, columns, key):
    if settings.WAREHOUSE == "redshift":
        # Redshift reads the file from S3 itself.
        cursor.execute(f"""
            COPY {table}
            FROM 's3://{settings.S3_BUCKET}/{key}'
            {copy_credentials()}
            FORMAT AS PARQUET
            REGION '{settings.AWS_REGION}';
        """)
        return

    # Postgres can't read S3, so download the file and stream its rows in.
    data = s3.get_object(Bucket=settings.S3_BUCKET, Key=key)["Body"].read()
    with cursor.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in parquet_rows(data, columns):
            copy.write_row(row)


def ensure_tables(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = 'loaded_files'"
    )
    if cursor.fetchone() is None:
        # First run after upgrading from the old drop-and-reload loader: its tables
        # have no record of which files they contain, so start them over once.
        log.info("Setting up incremental loading (one-time table rebuild)...")
        for table in TABLES:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        cursor.execute(LOADED_FILES_DDL)
    for _, _, ddl in TABLES.values():
        cursor.execute(ddl)
    conn.commit()
    cursor.close()


def load_new_files(conn, s3, table, prefix, columns):
    cursor = conn.cursor()
    cursor.execute("SELECT s3_key FROM loaded_files WHERE table_name = %s", (table,))
    already_loaded = {row[0] for row in cursor.fetchall()}
    new_keys = [key for key in list_parquet_keys(s3, prefix) if key not in already_loaded]
    log.info("📦 %s: %d new file(s)", table, len(new_keys))

    for key in new_keys:
        load_file(cursor, s3, table, columns, key)
        cursor.execute("INSERT INTO loaded_files (s3_key, table_name) VALUES (%s, %s)", (key, table))
        # Commit the data and its loaded_files row together, so a crash can't
        # leave a file loaded-but-unrecorded (which would load it twice).
        conn.commit()
    cursor.close()
    return len(new_keys)


def refresh_warehouse(s3):
    conn = warehouse.connect()
    try:
        ensure_tables(conn)
        for table, (prefix, columns, _) in TABLES.items():
            load_new_files(conn, s3, table, prefix, columns)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crypto_trades;")
        crypto_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM stock_prices;")
        stock_count = cursor.fetchone()[0]
        log.info("📊 Crypto trades: %s | Stock records: %s", f"{crypto_count:,}", f"{stock_count:,}")
        cursor.close()
    finally:
        conn.close()


def main():
    if not settings.S3_BUCKET:
        raise SystemExit("S3_BUCKET_NAME is not set — add it to your .env file.")
    if settings.WAREHOUSE == "redshift" and not settings.REDSHIFT_IAM_ROLE:
        log.warning(
            "REDSHIFT_IAM_ROLE is not set, so COPY sends your AWS access keys to Redshift. "
            "Attach an IAM role to your Redshift namespace and set REDSHIFT_IAM_ROLE instead."
        )

    s3 = boto3.client("s3", region_name=settings.AWS_REGION, endpoint_url=settings.S3_ENDPOINT_URL)
    log.info("🚀 Starting loader (warehouse: %s)...", settings.WAREHOUSE)
    while True:
        try:
            refresh_warehouse(s3)
        except Exception:
            log.exception("❌ Refresh failed, will retry")
        time.sleep(settings.LOADER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
