"""Silver: typed, standardized, deduplicated, quality-checked and de-identified data."""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import quality
from .quality import Rule
from .storage import lake_path, overwrite, overwrite_partitions, read_table
from .transforms import latest_per_key, mask_phi, normalize_ndc

FILL_RULES = [
    Rule("member_id_present", "member_id IS NOT NULL AND trim(member_id) <> ''"),
    Rule("ndc_valid_11_digit", "ndc11 IS NOT NULL"),
    Rule("quantity_positive", "quantity > 0"),
    Rule("prescriber_npi_10_digit", "prescriber_npi RLIKE '^[0-9]{10}$'"),
    Rule("paid_amount_non_negative", "paid_amount >= 0"),
    Rule("fill_date_present", "fill_ts IS NOT NULL"),
    Rule("copay_not_above_paid", "copay <= paid_amount", "warn"),
    Rule("days_supply_1_to_365", "days_supply BETWEEN 1 AND 365", "warn"),
]

MEMBER_RULES = [
    Rule("member_id_present", "member_id IS NOT NULL AND trim(member_id) <> ''"),
    Rule("date_of_birth_valid", "to_date(date_of_birth) IS NOT NULL"),
    Rule("zip_5_digit", "zip_code RLIKE '^[0-9]{5}$'"),
    Rule("plan_type_known", "plan_type IN ('COMMERCIAL','MEDICARE_D','MEDICAID','CASH')", "warn"),
]


def phi_salt() -> str:
    """In Databricks the salt comes from a Key Vault-backed secret scope; locally from an env var."""
    try:  # pragma: no cover - only available on Databricks
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession as _S

        return DBUtils(_S.getActiveSession()).secrets.get("kv-pharmacy", "phi-hash-salt")
    except Exception:
        return os.getenv("PHI_HASH_SALT", "local-dev-salt-change-me")


def standardize_fills(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumn("ndc11", normalize_ndc(F.col("ndc")))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("days_supply", F.col("days_supply").cast("int"))
        .withColumn("paid_amount", F.col("paid_amount").cast("decimal(12,2)"))
        .withColumn("copay", F.col("copay").cast("decimal(12,2)"))
        .withColumn("fill_ts", F.to_timestamp("fill_date"))
        .withColumn("fill_date", F.to_date("fill_ts"))
        .withColumn("store_id", F.upper(F.trim("store_id")))
        .withColumn("prescriber_npi", F.trim("prescriber_npi"))
    )


def build_fills(spark: SparkSession, bronze: DataFrame, salt: str, batch_id: str):
    std = latest_per_key(standardize_fills(bronze), ["fill_id"], "_ingested_at")
    checked = quality.apply_rules(std, FILL_RULES)
    valid, quarantine = quality.split_valid_quarantine(checked)
    valid = (
        valid.withColumn("member_key", F.sha2(F.concat(F.col("member_id"), F.lit(salt)), 256))
        .drop("member_id", "ndc", "_source_file")
    )
    dq = quality.summarize(spark, checked, FILL_RULES, "fills", batch_id)
    return valid, quarantine, dq


def build_members(spark: SparkSession, bronze: DataFrame, salt: str, batch_id: str):
    std = latest_per_key(bronze, ["member_id"], "updated_at")
    checked = quality.apply_rules(std, MEMBER_RULES)
    valid, quarantine = quality.split_valid_quarantine(checked)
    valid = mask_phi(valid, salt).select(
        "member_key", "plan_type", "zip3", "birth_year", "updated_at", "_batch_id"
    )
    # quarantine keeps PHI in a restricted table; mask names before persisting
    quarantine = quarantine.drop("first_name", "last_name")
    dq = quality.summarize(spark, checked, MEMBER_RULES, "members", batch_id)
    return valid, quarantine, dq


def run(spark: SparkSession, data_dir: str, batch_id: str) -> dict:
    salt = phi_salt()
    bronze_fills = read_table(spark, lake_path(data_dir, "bronze", "fills")).filter(F.col("_batch_id") == batch_id)
    bronze_members = read_table(spark, lake_path(data_dir, "bronze", "members")).filter(F.col("_batch_id") == batch_id)
    bronze_providers = read_table(spark, lake_path(data_dir, "bronze", "providers")).filter(F.col("_batch_id") == batch_id)

    fills, fills_q, fills_dq = build_fills(spark, bronze_fills, salt, batch_id)
    members, members_q, members_dq = build_members(spark, bronze_members, salt, batch_id)
    providers = latest_per_key(bronze_providers, ["npi"], "_ingested_at").drop("_source_file")

    overwrite_partitions(fills, lake_path(data_dir, "silver", "fills"), "_batch_id")
    overwrite_partitions(fills_q, lake_path(data_dir, "quarantine", "fills"), "_batch_id")
    overwrite_partitions(members, lake_path(data_dir, "silver", "members"), "_batch_id")
    overwrite_partitions(members_q, lake_path(data_dir, "quarantine", "members"), "_batch_id")
    overwrite(providers, lake_path(data_dir, "silver", "providers"))
    overwrite_partitions(
        fills_dq.unionByName(members_dq), lake_path(data_dir, "quality", "dq_results"), "batch_id"
    )
    return {
        "fills_valid": fills.count(),
        "fills_quarantined": fills_q.count(),
        "members_valid": members.count(),
        "members_quarantined": members_q.count(),
    }


def main() -> None:
    """Databricks wheel-task entry point: <data_dir> <batch_id>."""
    import sys

    from .spark import get_spark

    data_dir, batch_id = sys.argv[1], sys.argv[2]
    run(get_spark(), data_dir, batch_id)
