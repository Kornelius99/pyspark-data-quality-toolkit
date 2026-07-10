# pyspark-data-quality-toolkit

[![CI](https://github.com/Kornelius99/pyspark-data-quality-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/Kornelius99/pyspark-data-quality-toolkit/actions/workflows/ci.yml)

A small, dependency-light data quality library for PySpark DataFrames. Unlike heavier frameworks, it has no external dependencies beyond PySpark itself, so it can be dropped into any Databricks notebook, Airflow task or Spark job to gate a pipeline on data quality in a few lines of code.

## Why this exists

Most teams end up writing the same five checks (nulls, duplicates, orphaned foreign keys, stale data, unexpected volume) by hand in every project. This package packages them once, tested and documented, so you (or anyone else) can `pip install` it and reuse it instead of re-writing it per pipeline.

## Installation

```bash
pip install git+https://github.com/Kornelius99/pyspark-data-quality-toolkit.git
# or, if published to PyPI:
# pip install pyspark-data-quality-toolkit
```

## Quick start

```python
from pyspark_dq import DataQualityChecker

checker = DataQualityChecker(entity_name="orders")

checker.check_completeness(orders_df, column="customer_id", max_null_rate=0.01)
checker.check_uniqueness(orders_df, key_columns=["order_id"])
checker.check_referential_integrity(orders_df, "customer_id", customers_df, "customer_id")
checker.check_freshness(orders_df, timestamp_column="loaded_at", max_age_hours=24)
checker.check_row_count_anomaly(orders_df, expected_avg=50_000, max_deviation_pct=0.3)

report = checker.get_report()
print(report.summary())

if not report.passed:
    raise ValueError("Data quality checks failed - see report above.")
```

## Checks included

- **Completeness** - null rate of a column against a configurable threshold
- **Uniqueness** - duplicate detection on one or more key columns
- **Referential integrity** - orphaned foreign keys between a child and parent DataFrame
- **Freshness** - flags data older than an expected SLA window
- **Row-count anomaly detection** - flags unexpected volume swings vs. an expected average, useful for catching silent upstream failures

Every check returns a structured `CheckResult` (name, pass/fail, metric value, threshold), and all results for an entity are aggregated into a single `QualityReport` that can gate a pipeline before it promotes data downstream.

## Project structure

```text
pyspark-data-quality-toolkit/
├── pyproject.toml
├── src/
│   └── pyspark_dq/
│       ├── __init__.py
│       └── checks.py
├── tests/
│   └── test_checks.py
└── .github/workflows/ci.yml
```

## Running the tests locally

```bash
git clone https://github.com/Kornelius99/pyspark-data-quality-toolkit.git
cd pyspark-data-quality-toolkit
pip install -e ".[dev]"
pytest tests/ -v
```

## Design principles

Zero non-PySpark dependencies, so it works anywhere Spark already runs (Databricks, EMR, Dataproc, local). Every check is a small, independently testable function. Results are structured data, not just print statements, so they can be logged, stored, or turned into pipeline gates.

## Extending this project

- Add a Great Expectations-compatible export so results can feed an existing GE-based governance dashboard.
- Add a `check_schema_drift` function comparing an incoming DataFrame's schema against an expected baseline.
- Publish to PyPI so it can be installed with a plain `pip install` instead of a Git URL.

## License

MIT - see [LICENSE](LICENSE). Contributions and forks welcome.
