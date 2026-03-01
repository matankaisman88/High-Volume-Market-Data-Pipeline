# Performance Optimizations Summary

This document summarizes the production-grade performance optimizations applied to the Crypto Data Pipeline (PySpark 3.5.0 + Delta Lake 3.1.0) and the expected performance gains.

---

## 1. Spark Engine Tuning

### Adaptive Query Execution (AQE)
- **Config:** `spark.sql.adaptive.enabled = true`
- **Location:** `src/config/spark_manager.py`
- **Effect:** Spark dynamically coalesces shuffle partitions, handles skew joins, and optimizes joins at runtime. Reduces unnecessary shuffles and balances load.

### Shuffle Partitions
- **Config:** `spark.sql.shuffle.partitions` = dynamic value (2–4× total cores), clamped between 8 and 256.
- **Default:** `SPARK_TOTAL_CORES=4` → 12 partitions (4×3). Override with env `SPARK_TOTAL_CORES` (e.g. in `run_pipeline.ps1` or Docker).
- **Effect:** Avoids over-parallelism (default 200) on small clusters and under-parallelism on larger ones; better shuffle I/O and memory use.

### Memory Management
- **Config:** `spark.memory.fraction = 0.8`, `spark.memory.storageFraction = 0.3`
- **Executor memory:** When `SPARK_CONTAINER_MEMORY_GB` is set (e.g. in Docker), executor memory is set to **80%** of that value to stay within container limits.
- **Effect:** More memory for execution and caching while leaving headroom for OS and off-heap.

**Expected gains:** 10–30% faster shuffles and joins from AQE; fewer OOMs and more stable runs from memory tuning.

---

## 2. Delta Lake Best Practices

### OPTIMIZE + ZORDER
- **Silver:** After each upsert, `OPTIMIZE delta.`silver_path` ZORDER BY (symbol, id)`.
- **Gold:** After each write, `OPTIMIZE delta.`gold_path` ZORDER BY (partition_date)`.
- **Effect:** Compacts small files and clusters data by filter columns, improving read performance and reducing the small-file problem.

### Compaction (Small File Problem)
- **Bronze:** Repartition by `partition_date` before write (max 4 partitions per run) to limit number of files per partition.
- **Silver/Gold:** OPTIMIZE compacts files after writes.
- **Effect:** Fewer small files → less metadata and faster scans.

### VACUUM
- **Script:** `src/maintenance/delta_vacuum.py`
- **Schedule:** Run after the pipeline (Step 5 in `run_pipeline.ps1`) or on a cron.
- **Config:** `RETAIN 168 HOURS` (7 days) for time travel; older files are deleted.
- **Effect:** Reclaims disk space while keeping 7 days of history.

**Expected gains:** 20–50% faster reads on Silver/Gold for filtered queries; significant storage savings over time from VACUUM.

---

## 3. Data Partitioning & Schema

### Partitioning
- **Bronze:** Already partitioned by `partition_date`; kept as-is with replaceWhere for incremental overwrite.
- **Silver:** Initial write uses `partitionBy("partition_date")` to align with Bronze and reduce skew.
- **Gold:** Writes use `partitionBy("partition_date")` for efficient time-based access.

### Schema Enforcement
- **Bronze:** Explicit `StructType` in `get_crypto_schema()`; no schema inference on JSON (saves CPU and ensures consistent types).

**Expected gains:** Fewer full-table scans; predictable file layout; no schema inference cost on read.

---

## 4. Code-Level Optimizations

### No Python UDFs
- **Status:** Confirmed no Python UDFs; only native Spark SQL / columnar functions (e.g. `to_timestamp`, `rank`, `sum`, `avg`).

### Caching
- **Silver in Gold job:** `_read_silver()` now calls `.cache()` on the Silver DataFrame. `unpersist()` is called in a `finally` block after Gold write and CSV export.
- **Effect:** If you add more Gold aggregations from the same Silver read, they will reuse the cached data.

### Broadcast Joins
- **Status:** No small-table ↔ large-table joins in the current pipeline. If you add metadata (e.g. symbol → name), use `broadcast(small_df)` or `BROADCAST` hint when joining to the large Silver/Bronze DataFrame.

**Expected gains:** Caching avoids re-reading Silver when multiple Gold outputs are added; keeping logic in native Spark keeps processing efficient.

---

## 5. Resource Awareness

### Centralized SparkSession
- **Module:** `src/config/spark_manager.py`
- **Behavior:** Does not set `master` when `force_local=False`, so `spark-submit --master spark://...` is used and executors run on the cluster.
- **Executor memory:** Capped at 80% of container memory when `SPARK_CONTAINER_MEMORY_GB` is set.
- **Usage:** All pipeline jobs and maintenance use `build_spark_session(app_name, force_local=False)`. Local/dev scripts (e.g. `fast_pipeline.py`) use `force_local=True`.

### Environment Variables (optional)
| Variable | Purpose | Example |
|----------|---------|---------|
| `SPARK_TOTAL_CORES` | Total executor cores for shuffle partition calculation | `4` |
| `SPARK_CONTAINER_MEMORY_GB` | Container memory in GB; 80% used for executor memory | `4` |
| `SPARK_SHUFFLE_MULTIPLIER` | Multiplier for shuffle partitions (default 3) | `3` |

**Expected gains:** Correct cluster usage (no accidental local[*] in production); no OOM from requesting more memory than the container has.

---

## Files Changed / Added

| File | Change |
|------|--------|
| `src/config/spark_manager.py` | **New.** Central Spark builder with AQE, shuffle, memory, Delta, Hive, cluster awareness. |
| `src/config/__init__.py` | **New.** Package init. |
| `src/ingestion/extract_crypto_data.py` | Uses `spark_manager`; repartition before Bronze write for compaction. |
| `src/processing/bronze_to_silver_crypto.py` | Uses `spark_manager`; Silver initial write `partitionBy("partition_date")`; OPTIMIZE ZORDER after write. |
| `src/processing/silver_to_gold_crypto_stats.py` | Uses `spark_manager`; Silver cached and unpersisted; Gold `partitionBy("partition_date")`; OPTIMIZE ZORDER after write. |
| `src/maintenance/delta_vacuum.py` | **New.** VACUUM Bronze/Silver/Gold with 7-day retention. |
| `src/maintenance/__init__.py` | **New.** Package init. |
| `src/utils/view_delta_history.py` | Uses `spark_manager` with Hive. |
| `src/processing/fix_metastore_registration.py` | Uses `spark_manager` with Hive + Delta. |
| `run_pipeline.ps1` | Passes `SPARK_TOTAL_CORES=4`; adds Step 5: Delta VACUUM. |
| `fast_pipeline.py` | Uses `spark_manager` with `force_local=True`. |

---

## Summary of Expected Performance Gains

| Area | Expected impact |
|------|------------------|
| **AQE + shuffle tuning** | 10–30% faster shuffles and joins; better resource use. |
| **Memory tuning** | Fewer OOMs; stable runs under load. |
| **OPTIMIZE + ZORDER** | 20–50% faster reads on filtered Silver/Gold queries. |
| **Compaction / partitioning** | Fewer small files; faster scans and less metadata. |
| **VACUUM** | Lower storage over time; same query performance. |
| **Explicit schema** | No inference cost; consistent types. |
| **Cluster-aware session** | Correct use of worker resources; no over-subscription. |

Overall, the pipeline is tuned for production: cluster-aware, Delta best practices, and ready for higher volume and additional Gold tables with minimal code changes.
