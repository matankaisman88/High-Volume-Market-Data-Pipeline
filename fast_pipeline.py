import requests
import json
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# הגדרת סשן שכוללת את ה-Extensions של Delta באופן מפורש
spark = SparkSession.builder \
    .appName("CryptoPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50, "page": 1, "sparkline": False}
    return requests.get(url, params=params).json()

print("\n>>> STARTING PIPELINE")

# 1. Extract
raw_data = fetch_crypto_data()
json_rdd = spark.sparkContext.parallelize([json.dumps(d) for d in raw_data])
bronze_df = spark.read.json(json_rdd)

# 2. Bronze
print(">>> WRITING BRONZE...")
bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/opt/spark/data/bronze/crypto_prices")

# 3. Silver
print(">>> WRITING SILVER...")
silver_df = bronze_df.select(
    F.col("id"),
    F.col("symbol"),
    F.col("current_price").cast("double"),
    F.col("total_volume").cast("double")
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/opt/spark/data/silver/crypto_cleaned")

# 4. Gold & CSV
print(">>> WRITING GOLD & CSV...")
top_1 = silver_df.orderBy(F.col("total_volume").desc()).limit(1)
top_1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/opt/spark/data/gold/top_volume_coins")

temp_csv_path = "/opt/spark/data/temp_csv"
top_1.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(temp_csv_path)

try:
    files = os.listdir(temp_csv_path)
    csv_file = [f for f in files if f.endswith('.csv') and f.startswith('part-')][0]
    
    shutil.copy(os.path.join(temp_csv_path, csv_file), "/opt/spark/data/Final_Report.csv")
    
    shutil.rmtree(temp_csv_path)
    
    print(">>> SUCCESS: 'Final_Report.csv' created and temp_csv removed!")
except Exception as e:
    print(f">>> CSV Cleanup failed: {e}")

spark.stop()