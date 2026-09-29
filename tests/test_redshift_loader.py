import pytest

from processing.redshift_loader import is_data_file, list_parquet_keys


@pytest.mark.parametrize("key, expected", [
    ("crypto/part-00000-abc.snappy.parquet", True),
    ("crypto/batch_id=000000000001/part-00000-abc.snappy.parquet", True),
    ("crypto/_spark_metadata/0", False),
    ("crypto/batch_id=000000000002/_temporary/0/_temporary/attempt_1/part-00000.parquet", False),
    ("crypto/batch_id=000000000001/_SUCCESS", False),
    ("crypto/batch_id=000000000001/.part-00000.parquet.crc", False),
])
def test_is_data_file(key, expected):
    assert is_data_file(key) is expected


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return iter(self.pages)


class FakeS3:
    def __init__(self, pages):
        self.pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self.pages)


def test_list_parquet_keys_reads_every_page():
    # S3 returns at most 1,000 keys per page; files on later pages must not be lost.
    page1 = {"Contents": [{"Key": f"crypto/part-{i:05d}.parquet"} for i in range(1000)]}
    page2 = {"Contents": [{"Key": "crypto/part-99999.parquet"}, {"Key": "crypto/_spark_metadata/7"}]}
    keys = list_parquet_keys(FakeS3([page1, page2]), "crypto/")
    assert len(keys) == 1001
    assert keys[-1] == "crypto/part-99999.parquet"


def test_parquet_rows_follow_table_column_order():
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    from processing.redshift_loader import TABLES, parquet_rows

    buffer = io.BytesIO()
    table = pa.table({
        "symbol": ["BTCUSDT"], "price": [64592.01], "quantity": [0.5],
        "timestamp": [1720000000123], "anomaly": ["NORMAL"],
    })
    pq.write_table(table.select(["anomaly", "price", "symbol", "timestamp", "quantity"]), buffer)
    columns = TABLES["crypto_trades"][1]
    assert parquet_rows(buffer.getvalue(), columns) == [("BTCUSDT", 64592.01, 0.5, 1720000000123, "NORMAL")]
