from pyspark.sql import SparkSession


def _build_spark_session() -> SparkSession:
    """
    Build a SparkSession with Hive support enabled and configured
    to use a PostgreSQL-backed metastore.
    """
    spark = (
        SparkSession.builder.appName("FixMetastoreRegistration")
        .enableHiveSupport()
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:postgresql://metastore-db:5432/metastore",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.postgresql.Driver",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionUserName",
            "metastore_user",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionPassword",
            "metastore_password",
        )
        .getOrCreate()
    )

    # Keep the output reasonably quiet when running in the container.
    spark.sparkContext.setLogLevel("WARN")
    return spark


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

