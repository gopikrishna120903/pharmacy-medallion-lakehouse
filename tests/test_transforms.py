from pyspark.sql import functions as F

from pharmacy_lakehouse.transforms import latest_per_key, mask_phi, normalize_ndc


def test_normalize_ndc_all_formats(spark):
    cases = [
        ("0071-0155-23", "00071015523"),   # 4-4-2
        ("59762-322-01", "59762032201"),   # 5-3-2
        ("00378-1805-1", "00378180501"),   # 5-4-1
        ("00093-7180-56", "00093718056"),  # 5-4-2
        ("00591040401", "00591040401"),    # 11 digits
        (" 00591040401 ", "00591040401"),  # whitespace
        ("12345-67", None),                # invalid
        ("0071015523", None),              # ambiguous 10 digits without hyphens
        (None, None),
    ]
    df = spark.createDataFrame([(c[0],) for c in cases], "ndc string")
    out = [r["n"] for r in df.select(normalize_ndc(F.col("ndc")).alias("n")).collect()]
    assert out == [c[1] for c in cases]


def test_latest_per_key_keeps_newest(spark):
    df = spark.createDataFrame(
        [("A", "2026-01-01", 1), ("A", "2026-01-03", 3), ("B", "2026-01-02", 2)],
        "k string, ts string, v int",
    )
    out = {r["k"]: r["v"] for r in latest_per_key(df, ["k"], "ts").collect()}
    assert out == {"A": 3, "B": 2}


def test_mask_phi_removes_identifiers(spark):
    df = spark.createDataFrame(
        [("M1", "Ava", "Smith", "1980-05-01", "76201")],
        "member_id string, first_name string, last_name string, date_of_birth string, zip_code string",
    )
    row = mask_phi(df, "salt").collect()[0].asDict()
    assert set(row) == {"member_key", "birth_year", "zip3"}
    assert row["birth_year"] == 1980 and row["zip3"] == "762"
    assert len(row["member_key"]) == 64 and "M1" not in row["member_key"]
