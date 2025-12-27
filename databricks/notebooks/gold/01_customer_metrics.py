from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, current_timestamp
import os
from dotenv import load_dotenv

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

customers_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/customers"
gold_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/customer_metrics"

customers_df = spark.read.format("delta").load(customers_path)

customer_metrics = (customers_df
    .groupBy("customer_segment", "city")
    .agg(
        count("customer_id").alias("customer_count"),
        avg("risk_score").alias("avg_risk_score"),
        avg("account_balance").alias("avg_balance"),
        avg("credit_limit").alias("avg_credit_limit"),
        sum("account_balance").alias("total_balance")
    )
    .withColumn("calculated_at", current_timestamp())
    .orderBy("customer_segment", "city"))

(customer_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
