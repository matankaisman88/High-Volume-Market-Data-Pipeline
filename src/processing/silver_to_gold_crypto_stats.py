"""
Silver → Gold aggregation: crypto market stats.

Changes for high-volume / stress-test mode:
  • Market stats are now computed PER partition_date (not just the latest date),
    so the Gold table captures a full 30-day trend for all partitions ingested.
  • Gold Delta table is partitioned by partition_date.
  • Final_Report.csv contains the full per-date aggregate, not just one row.
"""
import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, avg as _avg, rank

from pyspark.sql.window import Window

from delta import configure_spark_with_delta_pip

SILVER_PATH = "/opt/spark/data/silver/crypto_prices"
GOLD_PATH   = "/opt/spark/data/gold/crypto_market_stats"


def _build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverToGoldCryptoMarketStats")
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
    logger = logging.getLogger("silver_to_gold_crypto_stats")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _read_silver(spark: SparkSession) -> DataFrame:
    """Read full Silver table (all partition_dates). Grouping by partition_date uses actual
    dates from the data so the Gold report shows all available days (e.g. 7 rows for 7 days)."""
    return spark.read.format("delta").load(SILVER_PATH)


def _compute_market_stats(df: DataFrame) -> DataFrame:
    """
    For EACH partition_date:
      1. Rank coins by total_volume (descending).
      2. Keep the top-10 by volume.
      3. Aggregate: total_market_cap = SUM(market_cap),
                    avg_price        = AVG(current_price).

    Returns one row per partition_date — a 30-day daily trend for stress-test runs.
    """
    if df.rdd.isEmpty():
        return df.limit(0)

    window_spec = Window.partitionBy("partition_date").orderBy(
        col("total_volume").desc()
    )
    ranked_df = df.withColumn("volume_rank", rank().over(window_spec))
    top_10_df = ranked_df.filter(col("volume_rank") <= 10)

    agg_df = (
        top_10_df.groupBy("partition_date")
        .agg(
            _sum("market_cap").alias("total_market_cap"),
            _avg("current_price").alias("avg_price"),
        )
        .orderBy("partition_date")
    )
    return agg_df


def _write_gold(spark: SparkSession, df: DataFrame, logger: logging.Logger) -> None:
    """Write gold stats, partitioned by partition_date. df has one row per partition_date
    present in Silver, so the report includes all available days (no filter by today)."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("partition_date")
        .save(GOLD_PATH)
    )
    logger.info("Gold stats written to %s", GOLD_PATH)


def _export_final_csv(df: DataFrame, logger: logging.Logger) -> None:
    """Export the gold DataFrame to a single CSV file (Final_Report.csv)."""
    import pandas as pd

    pdf = df.toPandas()
    out_path = "/opt/spark/data/Final_Report.csv"
    pdf.to_csv(out_path, index=False)
    logger.info("Final report exported to %s  (%d rows)", out_path, len(pdf))


def run_silver_to_gold(
    spark: Optional[SparkSession] = None,
    silver_df: Optional[DataFrame] = None,
) -> None:
    """
    Compute gold market stats and export Final_Report.csv.

    silver_df supplied → in-memory flow.
    spark=None         → standalone mode.
    """
    logger = _get_logger()
    logger.info("Starting silver → gold aggregation.")

    own_spark = spark is None
    if own_spark:
        spark = _build_spark_session()
    try:
        if silver_df is not None:
            logger.info("Using in-memory silver DF.")
            source_df = silver_df
        else:
            source_df = _read_silver(spark)
            logger.info("Read silver from Delta: %s", SILVER_PATH)

        stats_df = _compute_market_stats(source_df)
        logger.info("Market stats computed.")

        _write_gold(spark, stats_df, logger)
        _export_final_csv(stats_df, logger)
        logger.info("Silver → gold complete.")
    finally:
        if own_spark:
            logger.info("Stopping SparkSession.")
            spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()
