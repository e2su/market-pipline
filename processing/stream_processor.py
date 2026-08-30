import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# ── 1. Create Spark Session ──────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MarketDataProcessor") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Define Schemas ────────────────────────────────────────────────────────
crypto_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("quantity", FloatType()),
    StructField("timestamp", LongType())
])

stock_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("volume", IntegerType()),
    StructField("timestamp", StringType())
])

# ── 3. Read from Kafka ───────────────────────────────────────────────────────
crypto_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .load()

stock_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .load()

# ── 4. Parse JSON ────────────────────────────────────────────────────────────
crypto_df = crypto_raw \
    .select(from_json(col("value").cast("string"), crypto_schema).alias("data")) \
    .select("data.*") \
    .filter(col("price") > 0)

stock_df = stock_raw \
    .select(from_json(col("value").cast("string"), stock_schema).alias("data")) \
    .select("data.*") \
    .filter(col("price") > 0)

# ── 5. Anomaly Detection ─────────────────────────────────────────────────────
crypto_df = crypto_df.withColumn(
    "anomaly",
    when(col("price") > 100000, "SPIKE").otherwise("NORMAL")
)

# ── 6. Write to S3 as Parquet ────────────────────────────────────────────────
# Crypto data → s3a://your-bucket/crypto/
crypto_query = crypto_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/crypto/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/crypto/") \
    .trigger(processingTime="30 seconds") \
    .start()

# Stock data → s3a://your-bucket/stocks/
stock_query = stock_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/stocks/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/stocks/") \
    .trigger(processingTime="60 seconds") \
    .start()

print("✅ Stream processor running — writing to S3...")

spark.streams.awaitAnyTermination()