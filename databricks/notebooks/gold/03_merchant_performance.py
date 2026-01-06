from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, countDistinct, current_timestamp, when
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from gold_check import add_quality_flags_aggregations

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

transactions_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/transactions"
merchants_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/merchants"
gold_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/merchant_performance"

transactions_df = spark.read.format("delta").load(transactions_path)
merchants_df = spark.read.format("delta").load(merchants_path)

merchant_performance = (transactions_df
    .join(merchants_df, "merchant_id", "inner")
    .groupBy("merchant_id", "merchant_name", "category", "country")
    .agg(
        count("transaction_id").alias("transaction_count"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction_value"),
        sum(when(col("status") == "completed", 1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_transactions")
    )
    .withColumn("success_rate", col("successful_transactions") / col("transaction_count"))
    .withColumn("calculated_at", current_timestamp())
    .orderBy(col("total_revenue").desc()))

metric_columns = ["transaction_count", "unique_customers", "total_revenue", "avg_transaction_value", "successful_transactions", "failed_transactions", "success_rate"]
merchant_performance_with_flags = add_quality_flags_aggregations(merchant_performance, metric_columns)

(merchant_performance_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
