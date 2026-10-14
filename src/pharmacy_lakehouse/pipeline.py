"""End-to-end batch runner: Landing -> Bronze -> Silver -> Gold.

Usage:
    python -m pharmacy_lakehouse.generate_data --out data
    python -m pharmacy_lakehouse.pipeline --data-dir data --batch day1
    python -m pharmacy_lakehouse.pipeline --data-dir data --batch day2
"""
import argparse
import json
import logging
import time

from . import bronze, gold, silver
from .spark import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pharmacy_lakehouse")


def run(data_dir: str, batch_id: str) -> dict:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    metrics = {"batch_id": batch_id}
    t0 = time.time()

    log.info("Bronze ingest for batch %s", batch_id)
    metrics["bronze_rows"] = bronze.run(spark, data_dir, batch_id)

    log.info("Silver standardize + data quality")
    metrics["silver"] = silver.run(spark, data_dir, batch_id)

    log.info("Gold dimensional model")
    gold.run(spark, data_dir, batch_id)

    metrics["runtime_seconds"] = round(time.time() - t0, 1)
    log.info("Run metrics: %s", json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="data")
    p.add_argument("--batch", required=True)
    a = p.parse_args()
    run(a.data_dir, a.batch)
