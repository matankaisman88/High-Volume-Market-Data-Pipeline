"""
Centralized Spark session builder for the Crypto Data Pipeline.

Cluster defaults are sized for the 22-core / high-memory worker observed in
the Spark UI.  All resource knobs can be overridden through environment variables
so the same image runs comfortably in both dev (laptop) and the full cluster.

Key environment variables
-------------------------
SPARK_MASTER              – master URL (e.g. spark://spark-master:7077).
                            Absence → local[*] when force_local=True.
SPARK_TOTAL_CORES         – total executor cores visible to the driver
                            (used to compute shuffle partitions). Default: 22.
SPARK_CORES_MAX           – hard cap on cores Spark may request from the cluster.
                            Leave unset to let Spark use all available cores.
SPARK_EXECUTOR_MEMORY     – explicit executor memory string (e.g. "8g").
                            Takes precedence over SPARK_CONTAINER_MEMORY_GB.
SPARK_CONTAINER_MEMORY_GB – container RAM in GB; executor gets 80 % of this.
SPARK_SHUFFLE_MULTIPLIER  – shuffle-partitions = cores × multiplier (default 4).
"""

import os
from typing import Optional

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------
SPARK_TOTAL_CORES_ENV         = "SPARK_TOTAL_CORES"
SPARK_CORES_MAX_ENV           = "SPARK_CORES_MAX"
SPARK_CONTAINER_MEMORY_GB_ENV = "SPARK_CONTAINER_MEMORY_GB"
SPARK_EXECUTOR_MEMORY_ENV     = "SPARK_EXECUTOR_MEMORY"
SPARK_SHUFFLE_MULTIPLIER_ENV  = "SPARK_SHUFFLE_MULTIPLIER"

# ---------------------------------------------------------------------------
# Defaults sized for the 22-core worker cluster
# ---------------------------------------------------------------------------
DEFAULT_TOTAL_CORES        = 22   # full worker core count
DEFAULT_CONTAINER_MEMORY_GB = 8   # generous default; override via env
SHUFFLE_MULTIPLIER          = 4   # cores × 4 → 88 default partitions
MIN_SHUFFLE_PARTITIONS      = 8
MAX_SHUFFLE_PARTITIONS      = 512
EXECUTOR_MEMORY_FRACTION    = 0.8

DEV_SHUFFLE_PARTITIONS = 4        # minimal overhead for tiny local runs


def _get_shuffle_partitions(dev_mode: bool = False) -> int:
    """
    Compute target shuffle partitions.

    dev_mode → fixed 4 partitions (minimal scheduling overhead).
    cluster  → cores × SHUFFLE_MULTIPLIER, clamped to [MIN, MAX].
               Default with 22 cores and multiplier=4 → 88 partitions.
               Spark AQE will coalesce further if shuffles are small.
    """
    if dev_mode:
        return DEV_SHUFFLE_PARTITIONS
    cores = int(os.environ.get(SPARK_TOTAL_CORES_ENV, DEFAULT_TOTAL_CORES))
    mult  = int(os.environ.get(SPARK_SHUFFLE_MULTIPLIER_ENV, SHUFFLE_MULTIPLIER))
    return max(MIN_SHUFFLE_PARTITIONS, min(MAX_SHUFFLE_PARTITIONS, cores * mult))


def _get_executor_memory() -> Optional[str]:
    """
    Resolve executor memory string with the following priority:
    1. SPARK_EXECUTOR_MEMORY  (explicit, e.g. "8g")
    2. SPARK_CONTAINER_MEMORY_GB × 0.8
    3. None → let Spark use its built-in default (1g)
    """
    explicit = os.environ.get(SPARK_EXECUTOR_MEMORY_ENV)
    if explicit:
        return explicit

    gb_str = os.environ.get(SPARK_CONTAINER_MEMORY_GB_ENV)
    if not gb_str:
        return None
    try:
        gb = float(gb_str)
        executor_gb = max(1.0, gb * EXECUTOR_MEMORY_FRACTION)
        return f"{int(executor_gb)}g"
    except (TypeError, ValueError):
        return None


def get_cluster_mode() -> bool:
    """True when running on a remote cluster (spark-submit --master spark://...)."""
    master = os.environ.get("SPARK_MASTER", "")
    return "spark://" in master or "k8s://" in master or "yarn" in master.lower()


def build_spark_session(
    app_name: str,
    *,
    force_local: bool = False,
    use_delta: bool = True,
    use_hive: bool = False,
    hive_jdbc_url: Optional[str] = None,
    hive_jdbc_user: Optional[str] = None,
    hive_jdbc_password: Optional[str] = None,
    extra_config: Optional[dict] = None,
) -> SparkSession:
    """
    Build a production-grade SparkSession.

    Cluster mode (force_local=False, spark-submit --master spark://...):
      • No cores.max cap by default → uses ALL worker cores.
        Set SPARK_CORES_MAX env var to re-introduce a cap for specific runs.
      • Executor memory from SPARK_EXECUTOR_MEMORY or SPARK_CONTAINER_MEMORY_GB.
      • Shuffle partitions = SPARK_TOTAL_CORES × SPARK_SHUFFLE_MULTIPLIER.

    Local mode (force_local=True):
      • master("local[*]") – dev/utility scripts.
      • Fixed 4 shuffle partitions to reduce overhead on tiny datasets.
    """
    shuffle_partitions = _get_shuffle_partitions(dev_mode=force_local)
    executor_memory    = _get_executor_memory()

    builder = SparkSession.builder.appName(app_name)

    if force_local:
        builder = builder.master("local[*]")
    else:
        # Respect optional SPARK_CORES_MAX env var; omit config entirely when
        # the variable is absent so Spark claims all available cluster cores.
        cores_max = os.environ.get(SPARK_CORES_MAX_ENV)
        if cores_max:
            builder = builder.config("spark.cores.max", cores_max)
        builder = builder.config("spark.task.cpus", "1")

    # Derby for local metastore
    builder = builder.config(
        "spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby"
    )

    # ----- Adaptive Query Execution (AQE) -----
    builder = builder.config("spark.sql.adaptive.enabled",                     "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled",  "true")
    builder = builder.config("spark.sql.adaptive.skewJoin.enabled",            "true")
    # Let AQE coalesce tiny post-shuffle partitions automatically
    builder = builder.config(
        "spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB"
    )
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))

    # ----- Memory tuning -----
    # 80 % of JVM heap for execution+storage; 30 % of that reserved for storage.
    builder = builder.config("spark.memory.fraction",        "0.8")
    builder = builder.config("spark.memory.storageFraction", "0.3")
    if executor_memory and not force_local:
        builder = builder.config("spark.executor.memory", executor_memory)

    # ----- Broadcast join threshold (generous for cluster) -----
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "64MB")

    # ----- Driver memory guardrail -----
    # The driver plans OPTIMIZE and collects Delta transaction metadata.
    # With 22 executor cores active, the default 1g driver heap can be
    # exhausted.  2g gives comfortable headroom; maxResultSize prevents a
    # single large collect() from OOM-ing the driver.
    builder = builder.config("spark.driver.memory",        "2g")
    builder = builder.config("spark.driver.maxResultSize", "1g")

    # ----- Delta maintenance settings -----
    # Cap per-file size during OPTIMIZE to avoid creating giant files that
    # slow subsequent reads.  128 MB is a good balance for medium tables.
    builder = builder.config(
        "spark.databricks.delta.optimize.maxFileSize",
        str(128 * 1024 * 1024),  # 128 MB in bytes
    )
    # Skip the retention duration safety check during development so
    # VACUUM can run with any retention value without an extra guard.
    builder = builder.config(
        "spark.databricks.delta.retentionDurationCheck.enabled", "false"
    )

    # ----- Delta Lake -----
    if use_delta:
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        builder = builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    # ----- Hive metastore (optional) -----
    if use_hive:
        builder = builder.enableHiveSupport()
        builder = builder.config("spark.sql.catalogImplementation", "hive")
        if hive_jdbc_url:
            builder = builder.config(
                "spark.hadoop.javax.jdo.option.ConnectionURL", hive_jdbc_url
            )
        if hive_jdbc_user:
            builder = builder.config(
                "spark.hadoop.javax.jdo.option.ConnectionUserName", hive_jdbc_user
            )
        if hive_jdbc_password:
            builder = builder.config(
                "spark.hadoop.javax.jdo.option.ConnectionPassword", hive_jdbc_password
            )
        builder = builder.config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.postgresql.Driver",
        )
        # Auto-create Hive metastore schema (DataNucleus 4.x property names)
        builder = builder.config("spark.hadoop.datanucleus.schema.autoCreateAll",    "true")
        builder = builder.config("spark.hadoop.datanucleus.schema.autoCreateTables", "true")
        builder = builder.config(
            "spark.hadoop.hive.metastore.schema.verification", "false"
        )

    if extra_config:
        for k, v in extra_config.items():
            builder = builder.config(k, str(v))

    try:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        spark = builder.getOrCreate()

    # INFO in cluster mode so AQE decisions are visible in driver logs.
    # WARN in local/dev mode to keep output clean.
    log_level = os.environ.get("SPARK_LOG_LEVEL", "INFO" if not force_local else "WARN")
    spark.sparkContext.setLogLevel(log_level)

    return spark


def get_spark_session(
    app_name: str = "CryptoPipeline",
    **kwargs,
) -> SparkSession:
    """Convenience wrapper: build a Delta-enabled SparkSession."""
    return build_spark_session(app_name, use_delta=True, **kwargs)
