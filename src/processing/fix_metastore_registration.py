from pathlib import Path
import sys

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pyspark.sql import SparkSession

from src.config.spark_manager import build_spark_session


def _build_spark_session() -> SparkSession:
    return build_spark_session(
        "FixMetastoreRegistration",
        force_local=False,
        use_delta=True,
        use_hive=True,
        hive_jdbc_url="jdbc:postgresql://metastore-db:5432/metastore",
        hive_jdbc_user="metastore_user",
        hive_jdbc_password="metastore_password",
    )


def main() -> None:
    spark = _build_spark_session()

    try:
        # 1) Ensure the analytics database exists and is active
        spark.sql("CREATE DATABASE IF NOT EXISTS crypto_analytics")
        spark.sql("USE crypto_analytics")

        # 2) Register the external Delta table pointing at the gold stats location
        spark.sql(
            """
            CREATE EXTERNAL TABLE IF NOT EXISTS gold_stats
            USING DELTA
            LOCATION '/opt/spark/data/gold/crypto_market_stats/'
            """
        )

        # 3) Verification: list all tables in the crypto_analytics database
        print("Tables in database 'crypto_analytics':")
        spark.sql("SHOW TABLES IN crypto_analytics").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

