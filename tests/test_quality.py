from pharmacy_lakehouse.quality import Rule, apply_rules, split_valid_quarantine, summarize

RULES = [
    Rule("qty_positive", "qty > 0"),
    Rule("id_present", "id IS NOT NULL"),
    Rule("small_qty", "qty < 100", "warn"),
]


def test_rules_route_errors_to_quarantine(spark):
    df = spark.createDataFrame([(1, "a"), (-5, "b"), (500, None), (None, "d")], "qty int, id string")
    valid, quarantine = split_valid_quarantine(apply_rules(df, RULES))
    assert valid.count() == 1
    errs = {r["id"]: sorted(r["_dq_errors"]) for r in quarantine.collect()}
    assert errs["b"] == ["qty_positive"]
    assert errs[None] == ["id_present"]
    assert errs["d"] == ["qty_positive"]  # NULL comparison treated as a failure


def test_warn_rules_do_not_quarantine(spark):
    df = spark.createDataFrame([(150, "x")], "qty int, id string")
    valid, quarantine = split_valid_quarantine(apply_rules(df, RULES))
    assert quarantine.count() == 0
    assert valid.collect()[0]["_dq_warnings"] == ["small_qty"]


def test_summary_pass_rates(spark):
    df = spark.createDataFrame([(1, "a"), (-5, "b")], "qty int, id string")
    rows = {r["rule"]: r for r in summarize(spark, apply_rules(df, RULES), RULES, "t", "b1").collect()}
    assert rows["qty_positive"]["failed_rows"] == 1
    assert rows["qty_positive"]["pass_rate_pct"] == 50.0
    assert rows["id_present"]["failed_rows"] == 0
