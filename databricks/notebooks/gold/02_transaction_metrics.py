from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, countDistinct, current_timestamp, date_trunc
import os
from dotenv import load_dotenv

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

(transaction_metrics.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
