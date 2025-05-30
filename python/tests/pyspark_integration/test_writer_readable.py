"""Test that pyspark can read tables written by deltalake(delta-rs)."""

import pathlib
from typing import TYPE_CHECKING

import pytest

from deltalake import DeltaTable, write_deltalake

from .utils import assert_spark_read_equal, get_spark

if TYPE_CHECKING:
    import pyarrow as pa

try:
    import delta
    import delta.pip_utils
    import delta.tables
    import pyspark.pandas as ps

    spark = get_spark()
except ModuleNotFoundError:
    pass


@pytest.mark.pyspark
@pytest.mark.integration
def test_basic_read(sample_data_pyarrow: "pa.Table", existing_table: DeltaTable):
    uri = existing_table._table.table_uri() + "/"

    assert_spark_read_equal(sample_data_pyarrow, uri)

    dt = delta.tables.DeltaTable.forPath(spark, uri)
    history = dt.history().collect()
    assert len(history) == 1
    assert history[0].version == 0


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_partitioned(tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table"):
    partition_cols = ["date32", "utf8", "timestamp", "bool"]
    import pyarrow as pa

    # Add null values to sample data to verify we can read null partitions
    sample_data_with_null = sample_data_pyarrow
    for col in partition_cols:
        i = sample_data_pyarrow.schema.get_field_index(col)
        field = sample_data_pyarrow.schema.field(i)
        nulls = pa.array([None] * sample_data_pyarrow.num_rows, type=field.type)
        sample_data_with_null = sample_data_with_null.set_column(i, field, nulls)
    data = pa.concat_tables([sample_data_pyarrow, sample_data_with_null])

    write_deltalake(str(tmp_path), data, partition_by=partition_cols)

    assert_spark_read_equal(data, str(tmp_path), sort_by=["utf8", "int32"])


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_overwrite(
    tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table", existing_table: DeltaTable
):
    import pyarrow as pa

    path = str(tmp_path)

    write_deltalake(path, sample_data_pyarrow, mode="append")
    expected = pa.concat_tables([sample_data_pyarrow, sample_data_pyarrow])
    assert_spark_read_equal(expected, path)

    write_deltalake(path, sample_data_pyarrow, mode="overwrite")
    assert_spark_read_equal(sample_data_pyarrow, path)


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_issue_1591_roundtrip_special_characters(tmp_path: pathlib.Path):
    import pyarrow as pa

    test_string = r'$%&/()=^"[]#*?.:_-{=}|`<>~/\r\n+'
    poisoned = "}|`<>~"
    for char in poisoned:
        test_string = test_string.replace(char, "")

    data = pa.table(
        {
            "string": pa.array([test_string], type=pa.utf8()),
            "data": pa.array(["python-module-test-write"]),
        }
    )

    deltalake_path = tmp_path / "deltalake"
    write_deltalake(
        table_or_uri=deltalake_path, mode="append", data=data, partition_by=["string"]
    )

    loaded = ps.read_delta(str(deltalake_path), index_col=None).to_pandas()
    assert loaded.shape == data.shape

    spark_path = tmp_path / "spark"
    spark_df = spark.createDataFrame(data.to_pandas())
    spark_df.write.format("delta").partitionBy(["string"]).save(str(spark_path))

    loaded = DeltaTable(spark_path).to_pandas()
    assert loaded.shape == data.shape


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_read_checkpointed_table(tmp_path: pathlib.Path):
    import pyarrow as pa

    data = pa.table(
        {
            "int": pa.array([1]),
        }
    )
    write_deltalake(tmp_path, data)

    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()

    assert_spark_read_equal(data, str(tmp_path), ["int"])


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_read_checkpointed_features_table(tmp_path: pathlib.Path):
    from datetime import datetime

    import pyarrow as pa

    data = pa.table(
        {
            "timestamp": pa.array([datetime(2010, 1, 1)]),
        }
    )
    write_deltalake(tmp_path, data)

    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()

    assert_spark_read_equal(data, str(tmp_path), ["timestamp"])
