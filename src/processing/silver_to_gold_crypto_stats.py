import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, avg as _avg, rank, lit
from pyspark.sql.window import Window

SILVER_PATH = "/opt/spark/data/silver/crypto_prices"
GOLD_PATH = "/opt/spark/data/gold/crypto_market_stats"


from delta import configure_spark_with_delta_pip

def _build_spark_session() -> SparkSession:
    """
    Build a SparkSession configured to:
    - Use Delta Lake without a Hive Metastore.
    """
    builder = (
        SparkSession.builder.appName("SilverToGoldCryptoMarketStats")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("silver_to_gold_crypto_stats")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _read_silver(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load(SILVER_PATH)


def _filter_latest_partition(df: DataFrame) -> DataFrame:
    """
    Filter to only the most recent partition_date.
    Assumes partition_date column exists and is comparable.
    """
    if df.rdd.isEmpty():
        return df

    partition_field = df.schema["partition_date"]
    max_partition = df.agg({"partition_date": "max"}).collect()[0][0]
    return df.filter(
        col("partition_date") == lit(max_partition).cast(partition_field.dataType)
    )


def _compute_market_stats(df: DataFrame) -> DataFrame:
    """
    - Rank coins by volume (descending) using a window.
    - Take top 10 by volume.
    - Aggregate Total Market Cap and Average Price for those top 10.
    """
    if df.rdd.isEmpty():
        empty_schema_df = df.limit(0)
        return (
            empty_schema_df.select(
                col("partition_date"),
                col("market_cap"),
                col("current_price"),
                col("total_volume"),
            )
            .agg(
                _sum("market_cap").alias("total_market_cap"),
                _avg("current_price").alias("avg_price"),
            )
            .withColumn("partition_date", lit(None).cast(df.schema["partition_date"].dataType))
        )

    latest_date_value = df.select("partition_date").first()[0]
    partition_type = df.schema["partition_date"].dataType

    window_spec = Window.partitionBy("partition_date").orderBy(col("total_volume").desc())
    ranked_df = df.withColumn("volume_rank", rank().over(window_spec))

    top_10_df = ranked_df.filter(col("volume_rank") <= 10)

    agg_df = (
        top_10_df.agg(
            _sum("market_cap").alias("total_market_cap"),
            _avg("current_price").alias("avg_price"),
        )
        .withColumn(
            "partition_date",
            lit(latest_date_value).cast(partition_type),
        )
    )
    return agg_df


def _write_gold(spark: SparkSession, df: DataFrame, logger: logging.Logger) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )
    logger.info("Gold market stats written to %s", GOLD_PATH)

def _export_final_csv(df: DataFrame, logger: logging.Logger) -> None:
    """
    Export DataFrame to single CSV file Final_Report.csv in data directory.
    Overwrites existing file with headers.
    """
    import os
    import pandas as pd

    pdf = df.toPandas()
    out_path = "/opt/spark/data/Final_Report.csv"
    pdf.to_csv(out_path, index=False)
    logger.info("Exported final report to %s", out_path)


def run_silver_to_gold() -> None:
    logger = _get_logger()
    logger.info("Starting silver to gold crypto market stats job.")

    spark = _build_spark_session()
    try:
        silver_df = _read_silver(spark)
        logger.info("Read %d records from silver layer.", silver_df.count())

        latest_df = _filter_latest_partition(silver_df)
        logger.info(
            "Filtered to latest partition_date. Remaining records: %d",
            latest_df.count(),
        )

        stats_df = _compute_market_stats(latest_df)
        logger.info("Computed market stats for top 10 coins.")

        _write_gold(spark, stats_df, logger)
        _export_final_csv(stats_df, logger)
        logger.info("Silver to gold crypto market stats job completed successfully.")
    finally:
        logger.info("Stopping SparkSession.")
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()