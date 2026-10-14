"""Thin table I/O layer: Delta on Databricks / local Delta, Parquet fallback for lightweight local runs."""
import os

from pyspark.sql import DataFrame, SparkSession

from .spark import table_format


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


def read_table(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format(table_format()).load(path)


def overwrite_partitions(df: DataFrame, path: str, partition_col: str) -> None:
    """Idempotent write: re-running a batch replaces only that batch's partition."""
    (
        df.write.format(table_format())
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_col)
        .save(path)
    )


def overwrite(df: DataFrame, path: str) -> None:
    df.write.format(table_format()).mode("overwrite").option("overwriteSchema", "true").save(path)


def lake_path(base: str, layer: str, table: str) -> str:
    return os.path.join(base, "lakehouse", layer, table)
