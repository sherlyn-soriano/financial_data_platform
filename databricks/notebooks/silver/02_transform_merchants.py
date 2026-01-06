from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, current_timestamp
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from silver_check import add_quality_flags_merchants

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/merchants"
silver_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/merchants"

bronze_df = spark.read.format("delta").load(bronze_path)

silver_merchants = (bronze_df
    .filter(col("is_valid") == True)
    .filter(col("merchant_id").isNotNull())
    .filter(col("category").isNotNull())
    .withColumn("merchant_name", trim(col("merchant_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("city", trim(col("city")))
    .withColumn("country", when(col("country") == "Peruss", "Peru").otherwise(upper(trim(col("country")))))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "merchant_id", "merchant_name", "category", "mcc_code",
        "city", "country", "is_verified", "registration_date", "processed_timestamp"
    ))

silver_merchants_with_flags = add_quality_flags_merchants(silver_merchants)

(silver_merchants_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(silver_path))
