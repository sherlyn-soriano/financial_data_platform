from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, current_timestamp
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

metric_columns = ["customer_count", "avg_risk_score", "avg_balance", "avg_credit_limit", "total_balance"]
customer_metrics_with_flags = add_quality_flags_aggregations(customer_metrics, metric_columns)

(customer_metrics_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(gold_path))
