from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, countDistinct, current_timestamp, date_trunc
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
gold_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/transaction_metrics"

transactions_df = spark.read.format("delta").load(transactions_path)

transaction_metrics = (transactions_df
    .withColumn("transaction_day", date_trunc("day", col("transaction_date")))
    .groupBy("transaction_day", "status", "channel")
    .agg(
        count("transaction_id").alias("transaction_count"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        sum(col("is_fraud").cast("int")).alias("fraud_count")
    )
    .withColumn("fraud_rate", col("fraud_count") / col("transaction_count"))
    .withColumn("calculated_at", current_timestamp())
    .orderBy("transaction_day", "status"))

metric_columns = ["transaction_count", "unique_customers", "total_amount", "avg_amount", "max_amount", "fraud_count", "fraud_rate"]
transaction_metrics_with_flags = add_quality_flags_aggregations(transaction_metrics, metric_columns)

(transaction_metrics_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
