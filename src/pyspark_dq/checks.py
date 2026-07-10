"""
Core data quality checks for PySpark DataFrames: completeness, uniqueness,
referential integrity, freshness and row-count anomaly detection.

Each check returns a CheckResult, and all results for a run are aggregated
into a QualityReport that callers can use to gate a pipeline.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    details: str = ""


@dataclass
class QualityReport:
    entity: str
    results: List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        lines = [f"Data quality report for '{self.entity}': {'PASS' if self.passed else 'FAIL'}"]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{status}] {r.check_name}: {r.metric_value} (threshold {r.threshold}) {r.details}")
        return "\n".join(lines)


class DataQualityChecker:
    """Collection of PySpark-native data quality checks for a single entity."""

    def __init__(self, entity_name: str):
        self.entity_name = entity_name
        self.report = QualityReport(entity=entity_name)

    def check_completeness(self, df: DataFrame, column: str, max_null_rate: float = 0.01) -> CheckResult:
        total = df.count()
        nulls = df.filter(F.col(column).isNull()).count()
        null_rate = (nulls / total) if total else 0.0
        result = CheckResult(
            check_name=f"completeness:{column}",
            passed=null_rate <= max_null_rate,
            metric_value=round(null_rate, 4),
            threshold=max_null_rate,
            details=f"{nulls}/{total} rows null",
        )
        self.report.results.append(result)
        return result

    def check_uniqueness(self, df: DataFrame, key_columns: List[str]) -> CheckResult:
        total = df.count()
        distinct = df.select(*key_columns).distinct().count()
        duplicates = total - distinct
        result = CheckResult(
            check_name=f"uniqueness:{','.join(key_columns)}",
            passed=duplicates == 0,
            metric_value=duplicates,
            threshold=0,
            details=f"{duplicates} duplicate key(s) found",
        )
        self.report.results.append(result)
        return result

    def check_referential_integrity(
        self, child_df: DataFrame, child_key: str, parent_df: DataFrame, parent_key: str
    ) -> CheckResult:
        orphans = (
            child_df.select(child_key).distinct()
            .join(parent_df.select(parent_key).distinct(), child_df[child_key] == parent_df[parent_key], "left_anti")
            .count()
        )
        result = CheckResult(
            check_name=f"referential_integrity:{child_key}->{parent_key}",
            passed=orphans == 0,
            metric_value=orphans,
            threshold=0,
            details=f"{orphans} orphaned key(s) with no matching parent row",
        )
        self.report.results.append(result)
        return result

    def check_freshness(self, df: DataFrame, timestamp_column: str, max_age_hours: int = 24) -> CheckResult:
        max_ts = df.agg(F.max(timestamp_column)).collect()[0][0]
        age_hours = 9999.0
        if max_ts is not None:
            age_hours = (datetime.utcnow() - max_ts).total_seconds() / 3600
        result = CheckResult(
            check_name=f"freshness:{timestamp_column}",
            passed=age_hours <= max_age_hours,
            metric_value=round(age_hours, 2),
            threshold=max_age_hours,
            details=f"latest record is {round(age_hours, 2)}h old",
        )
        self.report.results.append(result)
        return result

    def check_row_count_anomaly(
        self, df: DataFrame, expected_avg: float, max_deviation_pct: float = 0.3
    ) -> CheckResult:
        actual = df.count()
        deviation = abs(actual - expected_avg) / expected_avg if expected_avg else 0.0
        result = CheckResult(
            check_name="row_count_anomaly",
            passed=deviation <= max_deviation_pct,
            metric_value=round(deviation, 4),
            threshold=max_deviation_pct,
            details=f"actual={actual}, expected_avg={expected_avg}",
        )
        self.report.results.append(result)
        return result

    def get_report(self) -> QualityReport:
        return self.report
