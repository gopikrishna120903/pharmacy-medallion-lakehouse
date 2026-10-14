"""Bronze: land raw files as-is (all strings) with audit columns for lineage and replay."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .storage import lake_path, overwrite_partitions

ENTITIES = ["members", "providers", "fills"]


def ingest_file(spark: SparkSession, file_path: str, batch_id: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)          # keep raw values; typing happens in Silver
        .csv(file_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def run(spark: SparkSession, data_dir: str, batch_id: str) -> dict:
    counts = {}
    for entity in ENTITIES:
        src = f"{data_dir}/landing/{batch_id}/{entity}.csv"
        df = ingest_file(spark, src, batch_id)
        overwrite_partitions(df, lake_path(data_dir, "bronze", entity), "_batch_id")
        counts[entity] = df.count()
    return counts


def main() -> None:
    """Databricks wheel-task entry point: <data_dir> <batch_id>."""
    import sys

    from .spark import get_spark

    data_dir, batch_id = sys.argv[1], sys.argv[2]
    run(get_spark(), data_dir, batch_id)
