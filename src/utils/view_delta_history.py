import logging
from pathlib import Path
import sys

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.config.spark_manager import build_spark_session

SILVER_PATH = "/opt/spark/data/silver/crypto_prices/"

# Hive metastore settings (same as pipeline)
HIVE_JDBC_URL = "jdbc:postgresql://metastore-db:5432/metastore"
HIVE_JDBC_USER = "metastore_user"
HIVE_JDBC_PASSWORD = "metastore_password"


def _build_spark_session() -> SparkSession:
    return build_spark_session(
        "ViewDeltaHistoryCryptoPrices",
        force_local=False,
        use_hive=True,
        hive_jdbc_url=HIVE_JDBC_URL,
        hive_jdbc_user=HIVE_JDBC_USER,
        hive_jdbc_password=HIVE_JDBC_PASSWORD,
    )


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("view_delta_history_crypto_prices")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def view_delta_history() -> None:
    """
    Load the Delta table and print the last 5 versions of its history.
    """
    logger = _get_logger()
    logger.info("Starting Delta history viewer for %s", SILVER_PATH)

    spark = _build_spark_session()
    try:
        if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
            logger.error("No Delta table found at path: %s", SILVER_PATH)
            return

        delta_table = DeltaTable.forPath(spark, SILVER_PATH)

        history_df = delta_table.history()
        logger.info("Fetched Delta history; printing last 5 versions.")

        (
            history_df.select(
                "version",
                "timestamp",
                "operation",
                "operationParameters",
            )
            .orderBy("version", ascending=False)
            .limit(5)
            .show(truncate=False)
        )
    finally:
        logger.info("Stopping SparkSession.")
        spark.stop()


if __name__ == "__main__":
    view_delta_history()

