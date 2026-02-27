import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession


SILVER_PATH = "/opt/spark/data/silver/crypto_prices/"


def _build_spark_session() -> SparkSession:
    """
    Build a SparkSession with Delta Lake and the same configuration
    used by the main bronze-to-silver processing job.
    """
    spark = (
        SparkSession.builder.appName("ViewDeltaHistoryCryptoPrices")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # PostgreSQL-backed Hive metastore
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:postgresql://metastore-db:5432/metastore",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.postgresql.Driver",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionUserName",
            "metastore_user",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionPassword",
            "metastore_password",
        )
        .config(
            "spark.hadoop.datanucleus.autoCreateSchema",
            "true",
        )
        .config(
            "spark.hadoop.datanucleus.fixedDatastore",
            "false",
        )
        .config(
            "spark.hadoop.hive.metastore.schema.verification",
            "false",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


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

