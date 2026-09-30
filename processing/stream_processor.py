import logging
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stream_processor")

# The jars below are built for Spark 3.5 (Scala 2.12, Hadoop 3.3.4). Mixing them with
# another Spark version fails at runtime with errors like
# "NoSuchMethodError: scala.Predef$.wrapRefArray", so check up front.
SPARK_PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

# ── Schemas ──────────────────────────────────────────────────────────────────
# DoubleType, not FloatType: a 32-bit float only has ~7 significant digits,
# which turns a BTC price like 64592.01 into 64592.0078.
crypto_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", LongType()),
])

stock_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("timestamp", StringType()),
])

CRYPTO_COLUMNS = ["symbol", "price", "quantity", "timestamp", "anomaly"]


def use_bundled_spark():
    """Make PySpark use the Spark that ships with the pip package.

    If SPARK_HOME points at a separately installed Spark (e.g. 4.x), PySpark
    launches that JVM instead, and it won't match the jars above.
    """
    if not pyspark.__version__.startswith("3.5."):
        raise SystemExit(
            f"PySpark {pyspark.__version__} found, but this job needs 3.5.x. "
            "Run: pip install -r requirements.txt"
        )
    bundled = os.path.dirname(pyspark.__file__)
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home and os.path.normcase(os.path.abspath(spark_home)) != os.path.normcase(bundled):
        log.warning("Ignoring SPARK_HOME=%s and using the Spark bundled with PySpark", spark_home)
        os.environ["SPARK_HOME"] = bundled


def build_spark():
    # S3 credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the
    # environment (loaded from .env), or from the EC2 instance's IAM role,
    # via S3A's default credential chain.
    builder = (
        SparkSession.builder
        .appName("MarketDataProcessor")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    if settings.S3_ENDPOINT_URL:
        # An S3-compatible server such as MinIO (local testing).
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", settings.S3_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
    else:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.AWS_REGION}.amazonaws.com")
    return builder.getOrCreate()


def flag_anomalies(df, reference_price, threshold_pct):
    """Mark trades more than threshold_pct percent away from reference_price as SPIKE."""
    deviation_pct = F.abs(F.col("price") - F.lit(reference_price)) / F.lit(reference_price) * 100
    return (
        df.withColumn("anomaly", F.when(deviation_pct > threshold_pct, "SPIKE").otherwise("NORMAL"))
        .select(*CRYPTO_COLUMNS)
    )


def make_crypto_batch_writer(output_path, threshold_pct):
    """Build the foreachBatch function that flags anomalies and writes one batch to S3.

    Each trade is compared against the median price of the *previous* batch, so a
    sudden jump stands out while the normal drift of the price does not. For the
    very first batch we fall back to that batch's own median.
    """
    state = {"reference_price": None}

    def write_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            stats = batch_df.agg(
                F.count("*").alias("trades"),
                F.percentile_approx("price", 0.5).alias("median_price"),
            ).first()
            if stats["trades"] == 0:
                return

            reference = state["reference_price"] or stats["median_price"]
            flagged = flag_anomalies(batch_df, reference, threshold_pct)
            # One folder per batch + overwrite = a retried batch replaces its own output
            # instead of writing duplicates.
            flagged.write.mode("overwrite").parquet(f"{output_path}batch_id={batch_id:012d}/")
            state["reference_price"] = stats["median_price"]
        finally:
            batch_df.unpersist()

    return write_batch


def read_topic(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        # The checkpoint in S3 outlives Kafka: a fresh Kafka (new machine, wiped
        # volume) starts its offsets at 0 again. Carry on from there instead of
        # crashing with "offset was changed from N to 0".
        .option("failOnDataLoss", "false")
        .load()
    )


def parse(raw_df, schema):
    return (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .filter(F.col("price") > 0)
    )


def main():
    if not settings.S3_BUCKET:
        raise SystemExit("S3_BUCKET_NAME is not set — add it to your .env file.")

    use_bundled_spark()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bucket = f"s3a://{settings.S3_BUCKET}/"
    crypto_df = parse(read_topic(spark, settings.CRYPTO_TOPIC), crypto_schema)
    stock_df = parse(read_topic(spark, settings.STOCK_TOPIC), stock_schema)

    # Crypto → s3a://<bucket>/crypto/batch_id=.../  (anomaly detection needs foreachBatch)
    # "crypto_v2": the previous version used a plain file sink, and Spark doesn't
    # support switching an existing checkpoint from a file sink to foreachBatch.
    (
        crypto_df.writeStream
        .foreachBatch(make_crypto_batch_writer(bucket + settings.S3_CRYPTO_PREFIX, settings.ANOMALY_THRESHOLD_PCT))
        .option("checkpointLocation", bucket + "checkpoints/crypto_v2/")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Stocks → s3a://<bucket>/stocks/
    (
        stock_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", bucket + settings.S3_STOCK_PREFIX)
        .option("checkpointLocation", bucket + "checkpoints/stocks/")
        .trigger(processingTime="60 seconds")
        .start()
    )

    log.info("✅ Stream processor running — writing to S3...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
