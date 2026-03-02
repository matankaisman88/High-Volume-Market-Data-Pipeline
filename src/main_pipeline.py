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
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from delta.tables import DeltaTable

from src.config.paths import SILVER_PATH, GOLD_PATH, GOLD_TABLE_PATH
from src.config.spark_manager import build_spark_session, force_stop
from src.ingestion.extract_crypto_data import run_ingestion
from src.processing.bronze_to_silver_crypto import run_bronze_to_silver
from src.processing.silver_to_gold_crypto_stats import run_silver_to_gold

# Metastore registration timeout (seconds); if exceeded, pipeline proceeds to finally
METASTORE_REGISTRATION_TIMEOUT = int(os.environ.get("METASTORE_REGISTRATION_TIMEOUT", "120"))

# Hive metastore connection (cluster); timeouts in seconds to avoid indefinite hangs
HIVE_JDBC_URL      = (
    "jdbc:postgresql://metastore-db:5432/metastore"
    "?connectTimeout=10&socketTimeout=120"
)
HIVE_JDBC_USER     = "metastore_user"
HIVE_JDBC_PASSWORD = "metastore_password"

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
def _optimize_silver(spark, partition_dates: Optional[List[str]] = None) -> None:
    """
    OPTIMIZE + ZORDER on the Silver table.

    When `partition_dates` is provided, only those partitions are re-clustered
    via an incremental OPTIMIZE with a partition_date filter to avoid
    write amplification and unnecessary DBU spend.

    Silver is partitioned by partition_date; we ZORDER by the non-partition
    columns most commonly used in downstream filters and joins.
    """
    dt = DeltaTable.forPath(spark, SILVER_PATH)
    optimize_builder = dt.optimize()

    if partition_dates:
        formatted_dates = ", ".join(f"'{d}'" for d in partition_dates)
        predicate = f"partition_date IN ({formatted_dates})"
        _log.info(
            "Running incremental OPTIMIZE + ZORDER on Silver: %s  (predicate: %s)",
            SILVER_PATH,
            predicate,
        )
        optimize_builder = optimize_builder.where(predicate)
    else:
        _log.info("Running full-table OPTIMIZE + ZORDER on Silver: %s", SILVER_PATH)

    optimize_builder.executeZOrderBy("symbol", "current_price", "market_cap")
    _log.info("Silver OPTIMIZE complete.")


@with_delta_retry(max_retries=3, wait_seconds=5)
def _optimize_gold(spark, partition_dates: Optional[List[str]] = None) -> None:
    """
    OPTIMIZE + ZORDER on the Gold table.

    When `partition_dates` is provided, only those partitions are re-clustered
    via an incremental OPTIMIZE with a partition_date filter to avoid
    write amplification and unnecessary DBU spend.

    Gold is partitioned by partition_date; we ZORDER by total_market_cap so that
    range queries on market cap within a partition benefit from data skipping.
    """
    dt = DeltaTable.forPath(spark, GOLD_PATH)
    optimize_builder = dt.optimize()

    if partition_dates:
        formatted_dates = ", ".join(f"'{d}'" for d in partition_dates)
        predicate = f"partition_date IN ({formatted_dates})"
        _log.info(
            "Running incremental OPTIMIZE + ZORDER on Gold: %s  (predicate: %s)",
            GOLD_PATH,
            predicate,
        )
        optimize_builder = optimize_builder.where(predicate)
    else:
        _log.info("Running full-table OPTIMIZE + ZORDER on Gold: %s", GOLD_PATH)

    optimize_builder.executeZOrderBy("total_market_cap")
    _log.info("Gold OPTIMIZE complete.")


# ---------------------------------------------------------------------------
# Metastore registration + verification
# ---------------------------------------------------------------------------
def _register_and_verify_metastore(spark) -> None:
    """
    Register the Gold Delta table in the Hive metastore, then verify the
    table actually appears in the catalog.  Raises RuntimeError on failure.

    Uses CREATE EXTERNAL TABLE ... USING DELTA LOCATION only; does not run
    RECOVER PARTITIONS or ANALYZE TABLE, so stats/partition discovery do not
    block registration (automatic stats are disabled via Spark config).
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
# Dynamic application name for Spark UI (reflects execution mode)
# ---------------------------------------------------------------------------
def _get_app_name() -> str:
    """App name for Spark UI: CryptoPipeline-StressTest-{coins}c-{days}d or CryptoPipeline-Prod."""
    mode = os.environ.get("INGESTION_MODE", "api").lower()
    if mode == "generate":
        coins = os.environ.get("STRESS_TEST_COINS", "200")
        days = os.environ.get("STRESS_TEST_DAYS", "30")
        return f"CryptoPipeline-StressTest-{coins}c-{days}d"
    return "CryptoPipeline-Prod"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    pipeline_start = time.perf_counter()
    ingestion_mode = os.environ.get("INGESTION_MODE", "api")
    _log.info("Pipeline starting  (INGESTION_MODE=%s)", ingestion_mode)

    exit_code = 0
    spark = None
    try:
        app_name = _get_app_name()
        use_hive = os.environ.get("USE_HIVE", "true").lower() == "true"
        _log.info("[LIFECYCLE] Building SparkSession (app_name=%s).", app_name)
        spark = build_spark_session(
            app_name,
            force_local=False,
            use_hive=use_hive,
            hive_jdbc_url=HIVE_JDBC_URL,
            hive_jdbc_user=HIVE_JDBC_USER,
            hive_jdbc_password=HIVE_JDBC_PASSWORD,
        )

        spark.sparkContext.setLogLevel(
            os.environ.get("SPARK_LOG_LEVEL", "INFO")
        )
        _log.info("[LIFECYCLE] SparkSession created (master=%s).", spark.sparkContext.master)

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
            silver_df.cache()
            silver_df.count()  # Break lazy lineage so Gold does not re-run ingestion

        # Capture the set of partition_date values touched in this run so that
        # downstream OPTIMIZE/ZORDER only reclusters modified partitions.
        partition_dates = [
            row["partition_date"]
            for row in silver_df.select("partition_date").distinct().collect()
        ]
        _log.info("Partition dates for incremental OPTIMIZE: %s", partition_dates)

        # ------------------------------------------------------------------
        # Stage 3 – Silver OPTIMIZE / ZORDER
        # ------------------------------------------------------------------
        with stage_timer("Silver OPTIMIZE + ZORDER"):
            _optimize_silver(spark, partition_dates=partition_dates)

        # ------------------------------------------------------------------
        # Stage 4 – Gold aggregation (read from SILVER_PATH to use Z-ORDER/compacted files)
        # ------------------------------------------------------------------
        with stage_timer("Gold Aggregation"):
            run_silver_to_gold(spark, silver_path=SILVER_PATH)

        # ------------------------------------------------------------------
        # Stage 5 – Gold OPTIMIZE / ZORDER
        # ------------------------------------------------------------------
        with stage_timer("Gold OPTIMIZE + ZORDER"):
            _optimize_gold(spark, partition_dates=partition_dates)

        # ------------------------------------------------------------------
        # Stage 6 – Metastore registration + verification (optional)
        # ------------------------------------------------------------------
        if os.environ.get("USE_HIVE", "true").lower() == "true":
            with stage_timer("Metastore Registration"):
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_register_and_verify_metastore, spark)
                    try:
                        future.result(timeout=METASTORE_REGISTRATION_TIMEOUT)
                    except FuturesTimeoutError:
                        _log.warning(
                            "Metastore registration timed out after %ds — "
                            "proceeding to shutdown (table may not be registered).",
                            METASTORE_REGISTRATION_TIMEOUT,
                        )
                    except Exception as exc:
                        raise exc
        else:
            _log.info("[LIFECYCLE] Skipping Metastore registration as USE_HIVE is false.")

    except Exception as exc:
        _log.error("=" * 60)
        _log.error("PIPELINE FAILED — aborting with exit code 1")
        _log.error("Cause: %s", exc, exc_info=True)
        _log.error("=" * 60)
        exit_code = 1

    finally:
        _log.info("[LIFECYCLE] Entering finally block — stopping Spark session.")
        try:
            from src.config.spark_manager import force_stop
            force_stop(spark)
            _log.info("[LIFECYCLE] force_stop() returned successfully.")
        except Exception as stop_exc:
            _log.warning("Spark shutdown raised: %s", stop_exc)
        _log.info("[LIFECYCLE] Finally block complete.")

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

    _log.info("[LIFECYCLE] Entering finally block — stopping Spark session.")
    try:
        if spark is not None:
            force_stop(spark)
            _log.info("[LIFECYCLE] force_stop() returned successfully.")
    except Exception as stop_exc:
        _log.warning("Spark shutdown raised: %s", stop_exc)

    _log.info("[LIFECYCLE] Calling os._exit(%d) to force process termination.", exit_code)
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(exit_code)


if __name__ == "__main__":
    main()
