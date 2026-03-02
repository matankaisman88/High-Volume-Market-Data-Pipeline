from pyspark.sql import SparkSession


def _build_local_spark_session() -> SparkSession:
    """
    Build and return a local SparkSession for quick data checks.
    """
    spark = (
        SparkSession.builder.appName("CheckBronzeCryptoData")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = _build_local_spark_session()

    try:
        bronze_path = "/opt/spark/data/bronze/crypto_prices/"
        df = spark.read.parquet(bronze_path)

        # Print schema
        df.printSchema()

        # Show first 10 rows
        df.show(10, truncate=False)

        # Print total record count
        count = df.count()
        print(f"Total records in bronze crypto_prices: {count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

