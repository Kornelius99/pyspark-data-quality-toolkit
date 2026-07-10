"""Unit tests for pyspark_dq.checks, run against a local SparkSession."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from pyspark_dq import DataQualityChecker


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-pyspark-dq")
        .master("local[2]")
        .getOrCreate()
    )


@pytest.fixture
def orders_df(spark):
    data = [
        (1, 101, "2024-01-01", 20.0),
        (2, 102, "2024-01-01", 15.5),
        (3, None, "2024-01-02", 9.0),
        (4, 103, "2024-01-02", 42.0),
    ]
    return spark.createDataFrame(data, ["order_id", "customer_id", "order_date", "total_amount"])


@pytest.fixture
def customers_df(spark):
    return spark.createDataFrame([(101,), (102,), (103,)], ["customer_id"])


def test_completeness_detects_nulls(orders_df):
    checker = DataQualityChecker("orders")
    result = checker.check_completeness(orders_df, "customer_id", max_null_rate=0.1)
    assert result.passed is False
    assert result.metric_value == pytest.approx(0.25)


def test_uniqueness_passes_on_distinct_keys(orders_df):
    checker = DataQualityChecker("orders")
    result = checker.check_uniqueness(orders_df, ["order_id"])
    assert result.passed is True
    assert result.metric_value == 0


def test_uniqueness_fails_on_duplicate_keys(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b")], ["order_id", "value"])
    checker = DataQualityChecker("orders")
    result = checker.check_uniqueness(df, ["order_id"])
    assert result.passed is False
    assert result.metric_value == 1


def test_referential_integrity_flags_orphans(orders_df, customers_df):
    checker = DataQualityChecker("orders")
    result = checker.check_referential_integrity(orders_df, "customer_id", customers_df, "customer_id")
    assert result.metric_value == 0
    assert result.passed is True


def test_freshness_flags_stale_data(spark):
    stale_time = datetime.utcnow() - timedelta(hours=48)
    df = spark.createDataFrame([(1, stale_time)], ["id", "loaded_at"])
    checker = DataQualityChecker("orders")
    result = checker.check_freshness(df, "loaded_at", max_age_hours=24)
    assert result.passed is False


def test_row_count_anomaly_within_tolerance(orders_df):
    checker = DataQualityChecker("orders")
    result = checker.check_row_count_anomaly(orders_df, expected_avg=4, max_deviation_pct=0.3)
    assert result.passed is True


def test_report_summary_reports_overall_failure(orders_df):
    checker = DataQualityChecker("orders")
    checker.check_completeness(orders_df, "customer_id", max_null_rate=0.1)
    checker.check_uniqueness(orders_df, ["order_id"])
    report = checker.get_report()
    assert report.passed is False
    assert "FAIL" in report.summary()
