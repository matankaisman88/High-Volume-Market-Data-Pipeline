import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window


BRONZE_PATH = "/opt/spark/data/bronze/crypto_prices"
SILVER_PATH = "/opt/spark/data/silver/crypto_prices"


from delta import configure_spark_with_delta_pip

def _build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("BronzeToSilverCrypto")
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
    logger = logging.getLogger("bronze_to_silver_crypto")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _read_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load(BRONZE_PATH)


def _transform(df: DataFrame) -> DataFrame:
    typed_df = (
        df.withColumn("last_updated", to_timestamp(col("last_updated")))
        .withColumn(
            "current_price",
            col("current_price").cast(DecimalType(18, 8)),
        )
        .withColumn(
            "market_cap",
            col("market_cap").cast(DecimalType(20, 2)),
        )
    )

    window_spec = Window.partitionBy("id").orderBy(col("_ingested_at").desc())

    deduped_df = typed_df.withColumn("_row_number", row_number().over(window_spec)).filter(
        col("_row_number") == 1
    )

    return deduped_df.drop("_row_number")


def _upsert_to_silver(spark: SparkSession, df: DataFrame, logger: logging.Logger) -> None:
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info("Silver Delta table found. Running MERGE (upsert).")
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(df.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        logger.info("Silver Delta table not found. Writing initial Delta table.")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )


def run_bronze_to_silver() -> None:
    logger = _get_logger()
    logger.info("Starting bronze to silver crypto processing job.")

    spark = _build_spark_session()
    try:
        bronze_df = _read_bronze(spark)
        logger.info("Read %d records from bronze layer.", bronze_df.count())

        transformed_df = _transform(bronze_df)
        logger.info("Transformed dataset has %d records after deduplication.", transformed_df.count())

        _upsert_to_silver(spark, transformed_df, logger)
        logger.info("Bronze to silver crypto processing job completed successfully.")
    finally:
        logger.info("Stopping SparkSession.")
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()

