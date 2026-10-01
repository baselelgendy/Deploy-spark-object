import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("test-clean-data")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def make_df(spark, rows):
    return spark.createDataFrame(rows, "id int, name string, amount double")


def test_valid_records_are_kept(spark):
    df = make_df(spark, [(1, "Alice", 100.0), (2, "Bob", 50.0)])
    assert clean_data(df).count() == 2


def test_amount_less_or_equal_zero_removed(spark):
    df = make_df(spark, [(1, "Alice", 100.0), (2, "Bob", 0.0), (3, "Sara", -5.0)])
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_null_names_removed(spark):
    df = make_df(spark, [(1, "Alice", 100.0), (2, None, 80.0)])
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_amount_with_tax_calculated(spark):
    df = make_df(spark, [(1, "Alice", 100.0)])
    row = clean_data(df).collect()[0]
    assert row["amount_with_tax"] == pytest.approx(120.0)
