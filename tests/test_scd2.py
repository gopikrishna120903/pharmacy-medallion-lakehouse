from pharmacy_lakehouse import scd2

TRACKED = ["plan", "zip3"]


def _incoming(spark, rows):
    return spark.createDataFrame(rows, "member_key string, plan string, zip3 string, updated_at string")


def test_scd2_new_changed_unchanged(spark):
    day1 = _incoming(spark, [("A", "COMMERCIAL", "762", "2026-01-01"), ("B", "MEDICAID", "752", "2026-01-01")])
    dim = scd2.prepare_incoming(day1, TRACKED, "updated_at")

    day2 = _incoming(spark, [
        ("A", "MEDICARE_D", "762", "2026-01-02"),   # changed
        ("B", "MEDICAID", "752", "2026-01-02"),     # unchanged
        ("C", "CASH", "600", "2026-01-02"),         # new
    ])
    out = scd2.merge_dataframe(dim, day2, "member_key", TRACKED, "updated_at").collect()

    a = sorted([r for r in out if r.member_key == "A"], key=lambda r: r.effective_from)
    assert len(a) == 2
    assert a[0].plan == "COMMERCIAL" and not a[0].is_current and a[0].effective_to is not None
    assert a[1].plan == "MEDICARE_D" and a[1].is_current and a[1].effective_to is None

    b = [r for r in out if r.member_key == "B"]
    assert len(b) == 1 and b[0].is_current and str(b[0].effective_from).startswith("2026-01-01")

    c = [r for r in out if r.member_key == "C"]
    assert len(c) == 1 and c[0].is_current


def test_scd2_is_idempotent(spark):
    day1 = _incoming(spark, [("A", "COMMERCIAL", "762", "2026-01-01")])
    dim = scd2.prepare_incoming(day1, TRACKED, "updated_at")
    again = scd2.merge_dataframe(dim, day1, "member_key", TRACKED, "updated_at")
    assert again.count() == 1
