"""
Bronze → Silver transformation for crypto price data.

Changes for high-volume / stress-test mode:
  • Deduplication window now partitions by (id, last_updated) so that multiple
    time-series rows per coin are preserved.  Only exact (coin, timestamp)
    duplicates introduced by re-ingestion are collapsed.
  • Silver Delta table is partitioned by (partition_date, symbol) to exercise
    Spark's partition-pruning at scale.
  • Merge condition updated to (id AND last_updated) to match the new unique key.
"""
import logging
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from delta import configure_spark_with_delta_pip

BRONZE_PATH = "/opt/spark/data/bronze/crypto_prices"
SILVER_PATH = "/opt/spark/data/silver/crypto_prices"


def _build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("BronzeToSilverCrypto")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("bronze_to_silver_crypto")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _read_bronze(spark: SparkSession) -> DataFrame:
    """Read full Bronze table (all partition_dates). No filter by today; actual partition_date
    from the data is used so multiple days accumulate and flow through to Silver/Gold."""
    return spark.read.format("delta").load(BRONZE_PATH)


def _transform(df: DataFrame) -> DataFrame:
    typed_df = (
        df.withColumn("last_updated", to_timestamp(col("last_updated")))
          .withColumn("current_price", col("current_price").cast(DecimalType(18, 8)))
          .withColumn("market_cap",    col("market_cap").cast(DecimalType(20, 2)))
    )

    # Deduplicate on (id, last_updated) so every unique price snapshot per day
    # is retained; 7 days of data stay as 7 distinct timestamps, not collapsed.
    # Only re-ingested exact (id, last_updated) duplicates are collapsed.
    window_spec = Window.partitionBy("id", "last_updated").orderBy(
        col("_ingested_at").desc()
    )
    deduped_df = (
        typed_df.withColumn("_row_number", row_number().over(window_spec))
                .filter(col("_row_number") == 1)
                .drop("_row_number")
    )
    return deduped_df


def _upsert_to_silver(spark: SparkSession, df: DataFrame, logger: logging.Logger) -> None:
    """
    Write or merge silver data, partitioned by (partition_date, symbol).

    New table → full overwrite with schema evolution.
    Existing  → MERGE on (id, last_updated); update matched rows, insert new ones.
    Partitioning by (partition_date, symbol) enables efficient predicate push-down
    for time-range and coin-specific queries at scale.
    """
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver table exists — running MERGE on (id, last_updated).")
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                "t.id = s.id AND t.last_updated = s.last_updated",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        logger.info("Silver table not found — initial write with partitioning.")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("partition_date", "symbol")
            .save(SILVER_PATH)
        )


def run_bronze_to_silver(
    spark: Optional[SparkSession] = None,
    bronze_df: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Transform bronze → silver, write to Delta, return the silver DataFrame.

    bronze_df supplied → in-memory flow (avoids re-reading from disk).
    spark=None         → standalone mode (creates and stops its own session).
    """
    logger = _get_logger()
    logger.info("Starting bronze → silver transformation.")

    own_spark = spark is None
    if own_spark:
        spark = _build_spark_session()
    try:
        if bronze_df is not None:
            logger.info("Using in-memory bronze DF.")
            source_df = bronze_df
        else:
            source_df = _read_bronze(spark)
            logger.info("Read bronze from Delta: %s", BRONZE_PATH)

        transformed_df = _transform(source_df)
        logger.info("Transformation complete; writing silver.")

        _upsert_to_silver(spark, transformed_df, logger)
        logger.info("Bronze → silver complete.")
        return transformed_df
    finally:
        if own_spark:
            logger.info("Stopping SparkSession.")
            spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
