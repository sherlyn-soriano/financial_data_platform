from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, current_timestamp, when
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
customers_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/customers"
gold_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/fraud_analysis"

transactions_df = spark.read.format("delta").load(transactions_path)
customers_df = spark.read.format("delta").load(customers_path)

fraud_analysis = (transactions_df
    .join(customers_df, "customer_id", "inner")
    .groupBy("customer_segment", "channel")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum(col("is_fraud").cast("int")).alias("fraud_transactions"),
        sum(when(col("is_fraud"), col("amount")).otherwise(0)).alias("fraud_amount"),
        avg("risk_score").alias("avg_customer_risk")
    )
    .withColumn("fraud_rate", col("fraud_transactions") / col("total_transactions"))
    .withColumn("calculated_at", current_timestamp())
    .orderBy(col("fraud_rate").desc()))

metric_columns = ["total_transactions", "fraud_transactions", "fraud_amount", "avg_customer_risk", "fraud_rate"]
fraud_analysis_with_flags = add_quality_flags_aggregations(fraud_analysis, metric_columns)

(fraud_analysis_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
