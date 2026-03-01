"""
Single-driver pipeline: Bronze → Silver → Gold with in-memory DataFrame flow.

Additions for high-volume stress testing:
  • stage_timer context manager — logs wall-clock duration of each stage.
  • Spark log level set to INFO so AQE decisions are visible in the driver log.
  • OPTIMIZE + ZORDER BY run on Silver and Gold Delta tables after each write.
  • with_delta_retry decorator — retries Delta maintenance ops up to 3 times with
    a 5-second back-off to survive transient transaction conflicts.

Root-cause fix (stress_test_run2 failure):
  When use_hive=True is set, the SQL form  OPTIMIZE delta.`<path>`  causes the
  Hive catalog to look up "delta" as a database name and throws
  NoSuchObjectException immediately.  We now use the DeltaTable Python API
  (DeltaTable.forPath) which bypasses the Hive catalog entirely.
"""
import functools
import logging
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from delta.tables import DeltaTable

from src.config.spark_manager import build_spark_session
from src.ingestion.extract_crypto_data import run_ingestion
from src.processing.bronze_to_silver_crypto import run_bronze_to_silver
from src.processing.silver_to_gold_crypto_stats import run_silver_to_gold

# Paths used for OPTIMIZE/ZORDER (must match the processing modules)
SILVER_PATH = "/opt/spark/data/silver/crypto_prices"
GOLD_PATH   = "/opt/spark/data/gold/crypto_market_stats"

# Hive metastore connection (cluster)
HIVE_JDBC_URL      = "jdbc:postgresql://metastore-db:5432/metastore"
HIVE_JDBC_USER     = "metastore_user"
HIVE_JDBC_PASSWORD = "metastore_password"
GOLD_TABLE_PATH    = "/opt/spark/data/gold/crypto_market_stats/"

# ---------------------------------------------------------------------------
# Pipeline logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
)
_log = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Stage timer
# ---------------------------------------------------------------------------
@contextmanager
def stage_timer(name: str):
    """
    Context manager that logs the wall-clock duration of a pipeline stage.
    Always re-raises exceptions so the caller receives a non-zero exit code.
    """
    bar = "=" * 60
    _log.info("%s", bar)
    _log.info(">>> STAGE START : %s", name)
    _log.info("%s", bar)
    t0 = time.perf_counter()
    try:
        yield
        elapsed = time.perf_counter() - t0
        _log.info("%s", bar)
        _log.info(">>> STAGE COMPLETE : %s  [%.2f s]", name, elapsed)
        _log.info("%s", bar)
    except Exception:
        elapsed = time.perf_counter() - t0
        _log.error(">>> STAGE FAILED : %s  [%.2f s]", name, elapsed)
        raise


# ---------------------------------------------------------------------------
# Retry decorator for Delta maintenance operations
# ---------------------------------------------------------------------------
def with_delta_retry(max_retries: int = 3, wait_seconds: int = 5):
    """
    Decorator that retries a Delta maintenance operation (OPTIMIZE, VACUUM)
    up to `max_retries` times with `wait_seconds` delay between attempts.
    Handles transient Delta transaction conflicts gracefully.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        _log.warning(
                            "Delta maintenance [%s] attempt %d/%d failed: %s "
                            "— retrying in %ds",
                            func.__name__, attempt, max_retries, exc, wait_seconds,
                        )
                        time.sleep(wait_seconds)
                    else:
                        _log.error(
                            "Delta maintenance [%s] failed after %d attempts: %s",
                            func.__name__, max_retries, exc,
                        )
            raise last_exc
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Delta OPTIMIZE helpers  (DeltaTable API — avoids Hive catalog lookup)
# ---------------------------------------------------------------------------
@with_delta_retry(max_retries=3, wait_seconds=5)
def _optimize_silver(spark) -> None:
    """
    OPTIMIZE + ZORDER on the Silver table.

    Partition columns are (partition_date, symbol) — Delta forbids ZORDERing
    by partition columns.  We ZORDER by the non-partition data columns that are
    most commonly used in downstream range filters: current_price and market_cap.
    """
    _log.info("Running OPTIMIZE + ZORDER on Silver: %s", SILVER_PATH)
    DeltaTable.forPath(spark, SILVER_PATH).optimize().executeZOrderBy(
        "current_price", "market_cap"
    )
    _log.info("Silver OPTIMIZE complete.")


@with_delta_retry(max_retries=3, wait_seconds=5)
def _optimize_gold(spark) -> None:
    """
    OPTIMIZE + ZORDER on the Gold table.

    Partition column is (partition_date) — Delta forbids ZORDERing by it.
    We ZORDER by total_market_cap so that range queries on market cap within
    a partition benefit from data skipping.
    """
    _log.info("Running OPTIMIZE + ZORDER on Gold: %s", GOLD_PATH)
    DeltaTable.forPath(spark, GOLD_PATH).optimize().executeZOrderBy(
        "total_market_cap"
    )
    _log.info("Gold OPTIMIZE complete.")


# ---------------------------------------------------------------------------
# Metastore registration + verification
# ---------------------------------------------------------------------------
def _register_and_verify_metastore(spark) -> None:
    """
    Register the Gold Delta table in the Hive metastore, then verify the
    table actually appears in the catalog.  Raises RuntimeError on failure.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS crypto_analytics")
    spark.sql("USE crypto_analytics")
    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold_stats
        USING DELTA
        LOCATION '{GOLD_TABLE_PATH}'
        """
    )

    tables = spark.sql("SHOW TABLES IN crypto_analytics").collect()
    table_names = [row.tableName.lower() for row in tables]
    if "gold_stats" not in table_names:
        raise RuntimeError(
            "Metastore verification FAILED: gold_stats not found in "
            "crypto_analytics after CREATE TABLE — catalog=[%s]"
            % ", ".join(table_names)
        )
    _log.info(
        "Metastore verification PASSED: gold_stats exists in crypto_analytics."
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    pipeline_start = time.perf_counter()
    ingestion_mode = os.environ.get("INGESTION_MODE", "api")
    _log.info("Pipeline starting  (INGESTION_MODE=%s)", ingestion_mode)

    spark = build_spark_session(
        "CryptoPipeline-StressTest",
        force_local=False,
        use_hive=True,
        hive_jdbc_url=HIVE_JDBC_URL,
        hive_jdbc_user=HIVE_JDBC_USER,
        hive_jdbc_password=HIVE_JDBC_PASSWORD,
    )

    spark.sparkContext.setLogLevel(
        os.environ.get("SPARK_LOG_LEVEL", "INFO")
    )

    exit_code = 0
    try:
        # ------------------------------------------------------------------
        # Stage 1 – Bronze ingestion
        # ------------------------------------------------------------------
        with stage_timer("Bronze Ingestion"):
            bronze_df = run_ingestion(spark)

        # ------------------------------------------------------------------
        # Stage 2 – Silver transformation
        # ------------------------------------------------------------------
        with stage_timer("Silver Transformation"):
            silver_df = run_bronze_to_silver(spark, bronze_df=bronze_df)

        # ------------------------------------------------------------------
        # Stage 3 – Silver OPTIMIZE / ZORDER
        # ------------------------------------------------------------------
        with stage_timer("Silver OPTIMIZE + ZORDER"):
            _optimize_silver(spark)

        # ------------------------------------------------------------------
        # Stage 4 – Gold aggregation
        # ------------------------------------------------------------------
        with stage_timer("Gold Aggregation"):
            run_silver_to_gold(spark, silver_df=silver_df)

        # ------------------------------------------------------------------
        # Stage 5 – Gold OPTIMIZE / ZORDER
        # ------------------------------------------------------------------
        with stage_timer("Gold OPTIMIZE + ZORDER"):
            _optimize_gold(spark)

        # ------------------------------------------------------------------
        # Stage 6 – Metastore registration + verification
        # ------------------------------------------------------------------
        with stage_timer("Metastore Registration"):
            _register_and_verify_metastore(spark)

    except Exception as exc:
        _log.error("=" * 60)
        _log.error("PIPELINE FAILED — aborting with exit code 1")
        _log.error("Cause: %s", exc, exc_info=True)
        _log.error("=" * 60)
        exit_code = 1

    finally:
        spark.stop()

    total = time.perf_counter() - pipeline_start
    if exit_code == 0:
        _log.info("=" * 60)
        _log.info(
            "PIPELINE COMPLETE  —  total wall clock: %.2f s  (%.1f min)",
            total, total / 60,
        )
        _log.info("=" * 60)
    else:
        _log.error(
            "PIPELINE ABORTED after %.2f s  (exit code %d)", total, exit_code
        )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
