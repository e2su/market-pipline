import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from processing.stream_processor import (  # noqa: E402
    CRYPTO_COLUMNS,
    crypto_schema,
    flag_anomalies,
    make_crypto_batch_writer,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def trades(spark, prices):
    rows = [("BTCUSDT", p, 0.01, 1720000000000 + i) for i, p in enumerate(prices)]
    return spark.createDataFrame(rows, crypto_schema)


def test_price_keeps_double_precision(spark):
    df = trades(spark, [64592.01])
    assert df.first()["price"] == 64592.01


def test_flag_anomalies_uses_relative_threshold(spark):
    # Normal BTC prices above 100k must NOT be flagged; only big jumps are.
    df = flag_anomalies(trades(spark, [120000.0, 120100.0, 125000.0]), reference_price=120000.0, threshold_pct=0.5)
    assert df.columns == CRYPTO_COLUMNS
    assert [r["anomaly"] for r in df.collect()] == ["NORMAL", "NORMAL", "SPIKE"]


def test_batch_writer_compares_against_previous_batch(spark, tmp_path):
    output = f"{tmp_path.as_uri()}/crypto/"
    write_batch = make_crypto_batch_writer(output, threshold_pct=0.5)

    write_batch(trades(spark, [100000.0, 100010.0, 99990.0]), 0)
    write_batch(trades(spark, [100020.0, 103000.0]), 1)
    write_batch(trades(spark, []), 2)  # empty batches write nothing

    batch0 = spark.read.parquet(f"{output}batch_id=000000000000/").collect()
    batch1 = spark.read.parquet(f"{output}batch_id=000000000001/").orderBy("price").collect()
    assert {r["anomaly"] for r in batch0} == {"NORMAL"}
    assert [(r["price"], r["anomaly"]) for r in batch1] == [(100020.0, "NORMAL"), (103000.0, "SPIKE")]
    assert not (tmp_path / "crypto" / "batch_id=000000000002").exists()


def test_batch_writer_is_idempotent_on_retry(spark, tmp_path):
    output = f"{tmp_path.as_uri()}/crypto/"
    write_batch = make_crypto_batch_writer(output, threshold_pct=0.5)
    write_batch(trades(spark, [100000.0, 100001.0]), 5)
    write_batch(trades(spark, [100000.0, 100001.0]), 5)  # Spark retries a failed batch with the same id
    assert spark.read.parquet(f"{output}batch_id=000000000005/").count() == 2
