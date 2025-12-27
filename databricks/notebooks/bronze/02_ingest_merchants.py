from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from data_quality import add_quality_flags_merchants, generate_quality_summary, print_quality_summary

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("mcc_code",IntegerType(),True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_verified",BooleanType(), True),
    StructField("registration_date", DateType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/merchants/merchants.json"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/merchants"

merchants_df = (spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(merchant_schema)
    .load(source_path))

bronze_merchants = (merchants_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_json")))

bronze_merchants_with_quality = add_quality_flags_merchants(bronze_merchants)

quality_summary = generate_quality_summary(bronze_merchants_with_quality)
print_quality_summary(quality_summary)
(bronze_merchants_with_quality.write
    .format('delta')
    .mode('overwrite')
    .option('mergeSchema', 'true')
    .save(target_path))
