import logging
from datetime import datetime
from typing import List, Dict, Any

import requests
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)
from pyspark.sql.functions import current_timestamp, current_date


from delta import configure_spark_with_delta_pip

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
TARGET_PATH = "/opt/spark/data/bronze/crypto_prices"
TOP_N = 50


def _build_spark_session() -> SparkSession:
    """
    Build and return a SparkSession configured for the crypto ingestion job.
    """
    builder = (
        SparkSession.builder.appName("CryptoIngestion")
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
    logger = logging.getLogger("crypto_ingestion")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def fetch_top_cryptocurrencies(logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Fetch the top N cryptocurrencies by market cap from CoinGecko.
    """
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "price_change_percentage": "24h",
    }

    logger.info("Requesting data from CoinGecko: %s", COINGECKO_MARKETS_URL)

    try:
        response = requests.get(COINGECKO_MARKETS_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Unexpected response structure from CoinGecko API")

        logger.info("Fetched %d records from CoinGecko.", len(data))
        return data
    except Exception as exc:
        logger.exception("Failed to fetch data from CoinGecko: %s", exc)
        raise


def get_crypto_schema() -> StructType:
    """
    Define and return the explicit schema for the crypto markets data.
    """
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("symbol", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("current_price", DoubleType(), nullable=True),
            StructField("market_cap", LongType(), nullable=True),
            StructField("total_volume", LongType(), nullable=True),
            StructField("last_updated", StringType(), nullable=True),
        ]
    )


def json_to_dataframe(
    spark: SparkSession, data: List[Dict[str, Any]], logger: logging.Logger
) -> DataFrame:
    """
    Convert the raw JSON list into a Spark DataFrame enforcing the target schema.
    """
    schema = get_crypto_schema()

    def _safe_str(item: Dict[str, Any], key: str, default: Any = None) -> Any:
        value = item.get(key, default)
        if value is None:
            return default
        return str(value)

    def _safe_float(item: Dict[str, Any], key: str, default: float = 0.0) -> float:
        raw = item.get(key)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid float value for key '%s': %r. Using default %r.",
                key,
                raw,
                default,
            )
            return default

    def _safe_int(item: Dict[str, Any], key: str, default: int = 0) -> int:
        raw = item.get(key)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid int value for key '%s': %r. Using default %r.",
                key,
                raw,
                default,
            )
            return default

    # Project only required fields, normalizing numeric types and providing defaults.
    rows = [
        Row(
            id=_safe_str(item, "id"),
            symbol=_safe_str(item, "symbol"),
            name=_safe_str(item, "name"),
            current_price=_safe_float(item, "current_price", 0.0),
            market_cap=_safe_int(item, "market_cap", 0),
            total_volume=_safe_int(item, "total_volume", 0),
            last_updated=_safe_str(item, "last_updated"),
        )
        for item in data
    ]

    # Ensure we have at least one row with required identifiers present.
    valid_rows = [
        row for row in rows if row.id is not None and row.symbol is not None and row.name is not None  # type: ignore[attr-defined]  # noqa: E501
    ]

    if not valid_rows:
        raise ValueError("No valid records to convert into DataFrame.")

    df = spark.createDataFrame(valid_rows, schema=schema)

    logger.info("Created DataFrame with %d rows.", df.count())
    return df


def enrich_dataframe(df: DataFrame) -> DataFrame:
    """
    Add ingestion metadata columns.
    - _ingested_at: current timestamp
    - partition_date: current date
    """
    return (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("partition_date", current_date())
    )


def write_to_bronze(df: DataFrame, logger: logging.Logger) -> None:
    """
    Write the DataFrame to the bronze layer in Delta format, partitioned by partition_date.
    Overwrites only the specific partition for the current run using replaceWhere.
    """
    partition_dates = [row["partition_date"] for row in df.select("partition_date").distinct().collect()]  # noqa: E501
    logger.info("Writing data for partition_date(s): %s", partition_dates)

    # Construct replaceWhere condition to only overwrite the partitions we are writing to
    replace_where = " OR ".join([f"partition_date = '{d}'" for d in partition_dates])

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("partition_date")
        .option("replaceWhere", replace_where)
        .save(TARGET_PATH)
    )

    logger.info("Successfully wrote data to %s", TARGET_PATH)


def run_ingestion() -> None:
    logger = _get_logger()
    logger.info("Starting crypto data ingestion job.")

    spark = _build_spark_session()
    try:
        raw_data = fetch_top_cryptocurrencies(logger)
        df = json_to_dataframe(spark, raw_data, logger)
        enriched_df = enrich_dataframe(df)
        write_to_bronze(enriched_df, logger)
        logger.info("Crypto data ingestion job completed successfully.")
    finally:
        logger.info("Stopping SparkSession.")
        spark.stop()


if __name__ == "__main__":
    run_ingestion()

