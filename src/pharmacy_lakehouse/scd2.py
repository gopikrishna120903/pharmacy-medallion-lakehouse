"""SCD Type 2 for dimensions, with a Delta Lake MERGE path and a pure-DataFrame path.

Both produce the same result:
  * new keys                -> inserted as current
  * changed tracked columns -> current row expired (effective_to set, is_current=false) and new row inserted
  * unchanged               -> untouched
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .transforms import record_hash


def prepare_incoming(incoming: DataFrame, tracked_cols: list, effective_col: str) -> DataFrame:
    return (
        incoming.withColumn("record_hash", record_hash(tracked_cols))
        .withColumn("effective_from", F.col(effective_col).cast("timestamp"))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .drop(effective_col)
    )


def merge_dataframe(current: DataFrame, incoming: DataFrame, key: str,
                    tracked_cols: list, effective_col: str) -> DataFrame:
    """Return the full new dimension (used locally when Delta is unavailable)."""
    inc = prepare_incoming(incoming, tracked_cols, effective_col)
    active = current.filter("is_current")
    history = current.filter("NOT is_current")

    changed = (
        active.alias("a").join(inc.alias("i"), key)
        .filter(F.col("a.record_hash") != F.col("i.record_hash"))
        .select(F.col(key), F.col("i.effective_from").alias("_new_from"))
    )
    expired = (
        active.join(changed, key)
        .withColumn("effective_to", F.col("_new_from"))
        .withColumn("is_current", F.lit(False))
        .drop("_new_from")
    )
    unchanged_active = active.join(changed, key, "left_anti")
    new_rows = inc.join(active.select(key, "record_hash"), [key, "record_hash"], "left_anti")

    cols = current.columns
    return (
        history.select(cols)
        .unionByName(expired.select(cols))
        .unionByName(unchanged_active.select(cols))
        .unionByName(new_rows.select(cols))
    )


def merge_delta(spark: SparkSession, target_path: str, incoming: DataFrame, key: str,
                tracked_cols: list, effective_col: str) -> None:
    """Classic single-MERGE SCD2 using a staged union (merge_key NULL forces insert of new versions)."""
    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, target_path)
    inc = prepare_incoming(incoming, tracked_cols, effective_col)
    active = target.toDF().filter("is_current")

    changed = (
        inc.alias("i").join(active.alias("a"), key)
        .filter(F.col("i.record_hash") != F.col("a.record_hash"))
        .select("i.*")
    )
    staged = inc.withColumn("merge_key", F.col(key)).unionByName(
        changed.withColumn("merge_key", F.lit(None).cast(inc.schema[key].dataType))
    )
    insert_values = {c: f"s.{c}" for c in inc.columns}
    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={"is_current": "false", "effective_to": "s.effective_from"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
