"""Gold: SCD2 member dimension, prescription fill fact, and daily store/drug aggregates."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import scd2
from .spark import delta_enabled
from .storage import lake_path, overwrite, overwrite_partitions, path_exists, read_table

MEMBER_TRACKED = ["plan_type", "zip3", "birth_year"]


def update_dim_member(spark: SparkSession, data_dir: str, batch_id: str) -> None:
    path = lake_path(data_dir, "gold", "dim_member")
    incoming = (
        read_table(spark, lake_path(data_dir, "silver", "members"))
        .filter(F.col("_batch_id") == batch_id)
        .drop("_batch_id", "_dq_warnings")
    )
    if not path_exists(spark, path):
        overwrite(scd2.prepare_incoming(incoming, MEMBER_TRACKED, "updated_at"), path)
    elif delta_enabled():
        scd2.merge_delta(spark, path, incoming, "member_key", MEMBER_TRACKED, "updated_at")
    else:
        current = read_table(spark, path)
        merged = scd2.merge_dataframe(current, incoming, "member_key", MEMBER_TRACKED, "updated_at")
        overwrite(merged.localCheckpoint(eager=True), path)


def update_facts(spark: SparkSession, data_dir: str, batch_id: str) -> None:
    fills = (
        read_table(spark, lake_path(data_dir, "silver", "fills"))
        .filter(F.col("_batch_id") == batch_id)
        .select("fill_id", "rx_number", "member_key", "prescriber_npi", "ndc11", "drug_name",
                "store_id", "fill_date", "fill_ts", "quantity", "days_supply",
                "paid_amount", "copay", "_batch_id")
    )
    overwrite_partitions(fills, lake_path(data_dir, "gold", "fact_rx_fill"), "_batch_id")

    fact = read_table(spark, lake_path(data_dir, "gold", "fact_rx_fill"))
    daily = (
        fact.groupBy("fill_date", "store_id", "ndc11", "drug_name")
        .agg(
            F.count("*").alias("fill_count"),
            F.countDistinct("member_key").alias("unique_members"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("paid_amount").alias("total_paid"),
            F.sum("copay").alias("total_copay"),
        )
    )
    overwrite(daily, lake_path(data_dir, "gold", "agg_daily_store_drug"))


def run(spark: SparkSession, data_dir: str, batch_id: str) -> None:
    update_dim_member(spark, data_dir, batch_id)
    update_facts(spark, data_dir, batch_id)


def main() -> None:
    """Databricks wheel-task entry point: <data_dir> <batch_id>."""
    import sys

    from .spark import get_spark

    data_dir, batch_id = sys.argv[1], sys.argv[2]
    run(get_spark(), data_dir, batch_id)
