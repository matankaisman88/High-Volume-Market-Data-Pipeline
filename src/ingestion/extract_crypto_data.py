"""
Crypto data ingestion: CoinGecko API + synthetic high-volume generator.

Set environment variable INGESTION_MODE=generate to bypass the CoinGecko API
and produce a large synthetic dataset for stress testing (see data_generator.py).
Default (INGESTION_MODE=api) fetches the top N coins from CoinGecko.

Additional env vars for API mode:
    TOP_N_COINS   – coins per page (default 50, max 250 per CoinGecko)
    API_PAGES     – number of pages to fetch (default 1)

Additional env vars for generate mode:
    STRESS_TEST_COINS – number of unique coins (default 200)
    STRESS_TEST_DAYS  – days of hourly history to generate (default 30)
"""
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

from delta import configure_spark_with_delta_pip

COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
TARGET_PATH = "/opt/spark/data/bronze/crypto_prices"
TOP_N = int(os.environ.get("TOP_N_COINS", 50))
API_PAGES = int(os.environ.get("API_PAGES", 1))


def _build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("CryptoIngestion")
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


# ---------------------------------------------------------------------------
# API ingestion path
# ---------------------------------------------------------------------------

def fetch_top_cryptocurrencies(logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Fetch TOP_N × API_PAGES cryptocurrencies by market cap from CoinGecko.
    Multiple pages are fetched sequentially to avoid hitting per-page limits.
    """
    all_data: List[Dict[str, Any]] = []
    for page in range(1, API_PAGES + 1):
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N,
            "page": page,
            "price_change_percentage": "24h",
        }
        logger.info("Requesting CoinGecko page %d/%d …", page, API_PAGES)
        try:
            response = requests.get(COINGECKO_MARKETS_URL, params=params, timeout=15)
            response.raise_for_status()
            page_data = response.json()
            if not isinstance(page_data, list):
                raise ValueError("Unexpected response structure from CoinGecko API")
            all_data.extend(page_data)
            logger.info("Page %d: fetched %d records.", page, len(page_data))
        except Exception as exc:
            logger.exception("Failed to fetch CoinGecko page %d: %s", page, exc)
            raise

    logger.info("Total API records fetched: %d", len(all_data))
    return all_data


def get_crypto_schema() -> StructType:
    return StructType(
        [
            StructField("id",            StringType(), nullable=False),
            StructField("symbol",        StringType(), nullable=False),
            StructField("name",          StringType(), nullable=False),
            StructField("current_price", DoubleType(), nullable=True),
            StructField("market_cap",    LongType(),   nullable=True),
            StructField("total_volume",  LongType(),   nullable=True),
            StructField("last_updated",  StringType(), nullable=True),
        ]
    )


def json_to_dataframe(
    spark: SparkSession, data: List[Dict[str, Any]], logger: logging.Logger
) -> DataFrame:
    schema = get_crypto_schema()

    def _safe_str(item, key, default=None):
        v = item.get(key, default)
        return None if v is None else str(v)

    def _safe_float(item, key, default=0.0):
        raw = item.get(key)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid float for '%s': %r. Using %r.", key, raw, default)
            return default

    def _safe_int(item, key, default=0):
        raw = item.get(key)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid int for '%s': %r. Using %r.", key, raw, default)
            return default

    rows = [
        Row(
            id=_safe_str(item, "id"),
            symbol=_safe_str(item, "symbol"),
            name=_safe_str(item, "name"),
            current_price=_safe_float(item, "current_price"),
            market_cap=_safe_int(item, "market_cap"),
            total_volume=_safe_int(item, "total_volume"),
            last_updated=_safe_str(item, "last_updated"),
        )
        for item in data
    ]
    valid_rows = [
        r for r in rows
        if r.id is not None and r.symbol is not None and r.name is not None  # type: ignore[attr-defined]
    ]
    if not valid_rows:
        raise ValueError("No valid records to convert into DataFrame.")

    df = spark.createDataFrame(valid_rows, schema=schema)
    logger.info("Created DataFrame with %d rows.", df.count())
    return df


def enrich_dataframe(df: DataFrame) -> DataFrame:
    """Add _ingested_at and partition_date. Partitioning is always derived from data:
    partition_date = to_date(last_updated). Never use current_date() so that API mode
    and generator mode behave the same; historical data is stored in the correct date folder.
    If last_updated is null (e.g. API omits it), set it from ingestion time so partition_date
    can still be derived from the row."""
    return (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn(
            "last_updated",
            F.coalesce(
                F.col("last_updated"),
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            ),
        )
        .withColumn("partition_date", F.to_date(F.col("last_updated")))
    )


# ---------------------------------------------------------------------------
# Bronze writer (shared between API and generator paths)
# ---------------------------------------------------------------------------

def write_to_bronze(
    df: DataFrame,
    logger: logging.Logger,
    overwrite_all: bool = False,
) -> None:
    """
    Write DataFrame to the Bronze Delta layer, partitioned by partition_date.

    overwrite_all=True  → full overwrite with schema evolution (generator mode,
                          where data spans many historical dates).
    overwrite_all=False → replaceWhere overwrite for the current run's partition_date(s)
                          only. Other partition_dates in the table are left untouched,
                          so the Delta table grows over time and keeps multiple days
                          (e.g. last 7 days of API snapshots). Does not delete previous days.
    """
    if overwrite_all:
        logger.info("Writing Bronze (full overwrite, historical data).")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("partition_date")
            .save(TARGET_PATH)
        )
    else:
        partition_dates = [
            row["partition_date"]
            for row in df.select("partition_date").distinct().collect()
        ]
        logger.info("Writing Bronze for partition_date(s): %s", partition_dates)
        replace_where = " OR ".join(
            [f"partition_date = '{d}'" for d in partition_dates]
        )
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("partition_date")
            .option("replaceWhere", replace_where)
            .save(TARGET_PATH)
        )

    logger.info("Bronze write complete → %s", TARGET_PATH)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_ingestion(spark: Optional[SparkSession] = None) -> DataFrame:
    """
    Ingest crypto data into the Bronze Delta table and return the enriched DF.

    INGESTION_MODE=api (default) – fetches from CoinGecko.
    INGESTION_MODE=generate      – uses the synthetic data generator.
    """
    logger = _get_logger()
    mode = os.environ.get("INGESTION_MODE", "api").lower()
    logger.info("Starting crypto ingestion (mode=%s).", mode)

    own_spark = spark is None
    if own_spark:
        spark = _build_spark_session()
    try:
        if mode == "generate":
            from src.ingestion.data_generator import generate_historical_data
            enriched_df = generate_historical_data(spark, logger)
            write_to_bronze(enriched_df, logger, overwrite_all=True)
        else:
            raw_data = fetch_top_cryptocurrencies(logger)
            df = json_to_dataframe(spark, raw_data, logger)
            enriched_df = enrich_dataframe(df)
            write_to_bronze(enriched_df, logger, overwrite_all=False)

        logger.info("Crypto ingestion complete.")
        return enriched_df
    finally:
        if own_spark:
            logger.info("Stopping SparkSession.")
            spark.stop()


if __name__ == "__main__":
    run_ingestion()
