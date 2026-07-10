"""pyspark_dq: a lightweight data quality library for PySpark DataFrames."""

from .checks import CheckResult, DataQualityChecker, QualityReport

__all__ = ["DataQualityChecker", "CheckResult", "QualityReport"]
__version__ = "0.1.0"
