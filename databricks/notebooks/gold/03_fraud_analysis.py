from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, current_timestamp, when
import os
from dotenv import load_dotenv

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

(fraud_analysis.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
