"""Declarative data-quality rules.

Each rule is a Spark SQL boolean expression that is TRUE when the row is valid.
Rows failing an `error` rule are routed to quarantine; `warn` rules are recorded only.
"""
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    valid_when: str
    severity: str = "error"  # "error" -> quarantine, "warn" -> flag only


def _failed(rules: Iterable[Rule]) -> F.Column:
    checks = [
        F.when(~F.coalesce(F.expr(r.valid_when), F.lit(False)), F.lit(r.name))
        for r in rules
    ]
    if not checks:
        return F.array().cast("array<string>")
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def apply_rules(df: DataFrame, rules: List[Rule]) -> DataFrame:
    errors = [r for r in rules if r.severity == "error"]
    warns = [r for r in rules if r.severity == "warn"]
    return df.withColumn("_dq_errors", _failed(errors)).withColumn("_dq_warnings", _failed(warns))


def split_valid_quarantine(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    valid = df.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    quarantine = df.filter(F.size("_dq_errors") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return valid, quarantine


def summarize(spark: SparkSession, checked: DataFrame, rules: List[Rule], dataset: str, batch_id: str) -> DataFrame:
    """One row per rule with failure counts and pass rate, for a DQ monitoring table."""
    total = checked.count()
    exploded = (
        checked.select(F.explode(F.concat("_dq_errors", "_dq_warnings")).alias("rule"))
        .groupBy("rule").count()
    )
    counts = {r["rule"]: r["count"] for r in exploded.collect()}
    rows = [
        (dataset, batch_id, r.name, r.severity, total, counts.get(r.name, 0),
         round(100.0 * (total - counts.get(r.name, 0)) / total, 3) if total else 100.0)
        for r in rules
    ]
    return spark.createDataFrame(
        rows,
        "dataset string, batch_id string, rule string, severity string, "
        "total_rows long, failed_rows long, pass_rate_pct double",
    ).withColumn("checked_at", F.current_timestamp())
