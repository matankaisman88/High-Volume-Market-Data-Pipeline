"""
Delta Lake VACUUM maintenance: remove old files no longer referenced by the table.

Run periodically (e.g. after pipeline or on a schedule) to reclaim storage.
RETAIN 168 HOURS (7 days) keeps recent history for time travel; adjust as needed.

Implementation note: uses the DeltaTable Python API instead of
  VACUUM delta.`<path>`  SQL, because with Hive support enabled the SQL form
  causes the catalog to misinterpret "delta" as a Hive database name and throw
  NoSuchObjectException immediately.
"""

import logging
from pathlib import Path
import sys

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.config.spark_manager import build_spark_session

BRONZE_PATH = "/opt/spark/data/bronze/crypto_prices"
SILVER_PATH = "/opt/spark/data/silver/crypto_prices"
GOLD_PATH = "/opt/spark/data/gold/crypto_market_stats"
# Keep 7 days of history (time travel); older files are removed.
RETAIN_HOURS = 168


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("delta_vacuum")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger


def run_vacuum(spark: SparkSession, table_path: str, retain_hours: int, logger: logging.Logger) -> None:
    try:
        DeltaTable.forPath(spark, table_path).vacuum(retain_hours)
        logger.info("VACUUM (RETAIN %s HOURS) completed for %s", retain_hours, table_path)
    except Exception as e:
        logger.warning("VACUUM failed for %s: %s", table_path, e)


def main() -> None:
    logger = _get_logger()
    logger.info("Starting Delta VACUUM maintenance.")

    spark = build_spark_session("DeltaVacuum", force_local=False)
    try:
        for path in (BRONZE_PATH, SILVER_PATH, GOLD_PATH):
            run_vacuum(spark, path, RETAIN_HOURS, logger)
        logger.info("Delta VACUUM maintenance completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
