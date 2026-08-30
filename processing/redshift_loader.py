import redshift_connector
import boto3
import os
import time
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def get_connection():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT")),
        database=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        ssl=True,
        sslmode="require"
    )

def get_parquet_files(s3_client, prefix):
    """List all parquet files in S3 folder, skip metadata."""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet') and '_spark_metadata' not in key:
            files.append(f's3://{S3_BUCKET}/{key}')
    return files

def refresh_redshift():
    print("🔄 Connecting to Redshift...")
    conn = get_connection()
    cursor = conn.cursor()

    # Drop and recreate tables for fresh data
    cursor.execute("DROP TABLE IF EXISTS crypto_trades;")
    cursor.execute("DROP TABLE IF EXISTS stock_prices;")

    cursor.execute("""
        CREATE TABLE crypto_trades (
            symbol      VARCHAR(20),
            price       FLOAT,
            quantity    FLOAT,
            timestamp   BIGINT,
            anomaly     VARCHAR(10)
        );
    """)

    cursor.execute("""
        CREATE TABLE stock_prices (
            symbol      VARCHAR(10),
            price       FLOAT,
            volume      BIGINT,
            timestamp   VARCHAR(20)
        );
    """)

    conn.commit()
    print("✅ Tables recreated!")

    # Connect to S3 and list parquet files
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='eu-north-1'
    )

    # Load crypto data
    crypto_files = get_parquet_files(s3, 'crypto/')
    print(f"📦 Found {len(crypto_files)} crypto files")
    for file_path in crypto_files:
        cursor.execute(f"""
            COPY crypto_trades
            FROM '{file_path}'
            ACCESS_KEY_ID '{AWS_ACCESS_KEY}'
            SECRET_ACCESS_KEY '{AWS_SECRET_KEY}'
            FORMAT AS PARQUET
            REGION 'eu-north-1';
        """)
    conn.commit()
    print("✅ Crypto data loaded!")

    # Load stock data
    stock_files = get_parquet_files(s3, 'stocks/')
    print(f"📦 Found {len(stock_files)} stock files")
    for file_path in stock_files:
        cursor.execute(f"""
            COPY stock_prices
            FROM '{file_path}'
            ACCESS_KEY_ID '{AWS_ACCESS_KEY}'
            SECRET_ACCESS_KEY '{AWS_SECRET_KEY}'
            FORMAT AS PARQUET
            REGION 'eu-north-1';
        """)
    conn.commit()
    print("✅ Stock data loaded!")

    # Verify counts
    cursor.execute("SELECT COUNT(*) FROM crypto_trades;")
    crypto_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM stock_prices;")
    stock_count = cursor.fetchone()[0]

    print(f"📊 Crypto trades: {crypto_count:,} | Stock records: {stock_count}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    print("🚀 Starting Redshift auto-loader...")
    while True:
        try:
            refresh_redshift()
            print("⏳ Waiting 60 seconds before next refresh...\n")
            time.sleep(60)
        except Exception as e:
            print(f"❌ Error: {e}")
            print("⏳ Retrying in 30 seconds...\n")
            time.sleep(30)