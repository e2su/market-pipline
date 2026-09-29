import logging
import time

import boto3
import redshift_connector

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("redshift_loader")

# table name -> (S3 prefix, CREATE statement). Column order must match the Parquet files.
TABLES = {
    "crypto_trades": (settings.S3_CRYPTO_PREFIX, """
        CREATE TABLE IF NOT EXISTS crypto_trades (
            symbol      VARCHAR(20),
            price       FLOAT,
            quantity    FLOAT,
            timestamp   BIGINT,
            anomaly     VARCHAR(10)
        );
    """),
    "stock_prices": (settings.S3_STOCK_PREFIX, """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol      VARCHAR(10),
            price       FLOAT,
            volume      BIGINT,
            timestamp   VARCHAR(20)
        );
    """),
}

# Remembers which S3 files are already in Redshift, so each run only loads new ones.
LOADED_FILES_DDL = """
    CREATE TABLE loaded_files (
        s3_key      VARCHAR(1024) PRIMARY KEY,
        table_name  VARCHAR(64),
        loaded_at   TIMESTAMP DEFAULT GETDATE()
    );
"""


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


def copy_credentials():
    if settings.REDSHIFT_IAM_ROLE:
        return f"IAM_ROLE '{settings.REDSHIFT_IAM_ROLE}'"
    return (
        f"ACCESS_KEY_ID '{settings.AWS_ACCESS_KEY_ID}' "
        f"SECRET_ACCESS_KEY '{settings.AWS_SECRET_ACCESS_KEY}'"
    )


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
    for _, ddl in TABLES.values():
        cursor.execute(ddl)
    conn.commit()
    cursor.close()


def load_new_files(conn, s3, table, prefix):
    cursor = conn.cursor()
    cursor.execute("SELECT s3_key FROM loaded_files WHERE table_name = %s", (table,))
    already_loaded = {row[0] for row in cursor.fetchall()}
    new_keys = [key for key in list_parquet_keys(s3, prefix) if key not in already_loaded]
    log.info("📦 %s: %d new file(s)", table, len(new_keys))

    for key in new_keys:
        cursor.execute(f"""
            COPY {table}
            FROM 's3://{settings.S3_BUCKET}/{key}'
            {copy_credentials()}
            FORMAT AS PARQUET
            REGION '{settings.AWS_REGION}';
        """)
        cursor.execute("INSERT INTO loaded_files (s3_key, table_name) VALUES (%s, %s)", (key, table))
        # Commit the data and its loaded_files row together, so a crash can't
        # leave a file loaded-but-unrecorded (which would load it twice).
        conn.commit()
    cursor.close()
    return len(new_keys)


def refresh_redshift(s3):
    conn = redshift_connector.connect(**settings.redshift_connection_args())
    try:
        ensure_tables(conn)
        for table, (prefix, _) in TABLES.items():
            load_new_files(conn, s3, table, prefix)

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
    if not settings.REDSHIFT_IAM_ROLE:
        log.warning(
            "REDSHIFT_IAM_ROLE is not set, so COPY sends your AWS access keys to Redshift. "
            "Attach an IAM role to your Redshift namespace and set REDSHIFT_IAM_ROLE instead."
        )

    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    log.info("🚀 Starting Redshift auto-loader...")
    while True:
        try:
            refresh_redshift(s3)
        except Exception:
            log.exception("❌ Refresh failed, will retry")
        time.sleep(settings.LOADER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
