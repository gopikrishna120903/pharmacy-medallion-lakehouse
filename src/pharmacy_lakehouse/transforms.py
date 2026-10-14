"""Pure DataFrame transformations used by the Silver and Gold layers (unit-testable)."""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def normalize_ndc(col: Column) -> Column:
    """Convert a 10-digit hyphenated NDC (4-4-2, 5-3-2, 5-4-1) or 11-digit NDC to 11-digit 5-4-2.

    Returns NULL when the value cannot be normalized, so a DQ rule can catch it.
    """
    c = F.trim(col)
    p = F.split(c, "-")
    return (
        F.when(c.rlike(r"^\d{11}$"), c)
        .when(c.rlike(r"^\d{5}-\d{4}-\d{2}$"), F.concat(p[0], p[1], p[2]))
        .when(c.rlike(r"^\d{4}-\d{4}-\d{2}$"), F.concat(F.lit("0"), p[0], p[1], p[2]))
        .when(c.rlike(r"^\d{5}-\d{3}-\d{2}$"), F.concat(p[0], F.lit("0"), p[1], p[2]))
        .when(c.rlike(r"^\d{5}-\d{4}-\d{1}$"), F.concat(p[0], p[1], F.lit("0"), p[2]))
        .otherwise(F.lit(None).cast("string"))
    )


def latest_per_key(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """Deduplicate: keep the most recent record per business key."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def mask_phi(df: DataFrame, salt: str) -> DataFrame:
    """De-identify member PHI: salted SHA-256 key, birth year only, 3-digit ZIP, names removed."""
    return (
        df.withColumn("member_key", F.sha2(F.concat(F.col("member_id"), F.lit(salt)), 256))
        .withColumn("birth_year", F.year(F.to_date("date_of_birth")))
        .withColumn("zip3", F.substring("zip_code", 1, 3))
        .drop("member_id", "first_name", "last_name", "date_of_birth", "zip_code")
    )


def record_hash(cols: list) -> Column:
    """Stable hash of tracked attributes, used for SCD2 change detection."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)
