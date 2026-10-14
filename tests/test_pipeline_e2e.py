from pharmacy_lakehouse import pipeline
from pharmacy_lakehouse.generate_data import generate
from pharmacy_lakehouse.storage import lake_path, read_table


def test_end_to_end_two_batches(spark, tmp_path):
    data_dir = str(tmp_path)
    generate(data_dir, members=60, fills_per_day=400, seed=7)

    m1 = pipeline.run(data_dir, "day1")
    assert m1["silver"]["fills_quarantined"] > 0          # injected bad records are caught
    assert m1["silver"]["fills_valid"] < m1["bronze_rows"]["fills"]

    pipeline.run(data_dir, "day2")
    pipeline.run(data_dir, "day2")                         # re-run must be idempotent

    dim = read_table(spark, lake_path(data_dir, "gold", "dim_member"))
    assert dim.filter("is_current").groupBy("member_key").count().filter("count > 1").count() == 0
    assert dim.filter("NOT is_current").count() > 0         # day2 changes produced history

    fact = read_table(spark, lake_path(data_dir, "gold", "fact_rx_fill"))
    assert fact.groupBy("fill_id").count().filter("count > 1").count() == 0
    assert "member_id" not in fact.columns                  # PHI never reaches Gold
