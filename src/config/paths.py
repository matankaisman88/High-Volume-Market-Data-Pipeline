"""
Centralized Delta Lake paths for the Crypto Data Pipeline.

All modules should import from here instead of hard-coding filesystem paths so
that maintenance and environment overrides remain single-sourced.
"""

# Bronze – raw crypto prices
BRONZE_PATH = "/opt/spark/data/bronze/crypto_prices"

# Silver – cleaned, deduplicated prices
SILVER_PATH = "/opt/spark/data/silver/crypto_prices"

# Gold – aggregated market statistics
GOLD_PATH = "/opt/spark/data/gold/crypto_market_stats"

# Location used when registering the Gold external table in the Hive metastore.
GOLD_TABLE_PATH = GOLD_PATH

# Final CSV export containing the per-day gold aggregates.
FINAL_REPORT_CSV_PATH = "/opt/spark/data/Final_Report.csv"

