# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

bronze_path = "abfss://bronze@<storage_account>.dfs.core.windows.net/"
silver_path = "abfss://silver@<storage_account>.dfs.core.windows.net/"

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df.limit(10))

# COMMAND ----------

silver_df = bronze_df.dropDuplicates()
silver_df = silver_df.filter(col("transaction_id").isNotNull())
silver_df = silver_df.withColumn("processed_timestamp", current_timestamp())

display(silver_df.limit(10))

# COMMAND ----------

silver_df.write.format("delta").mode("append").save(silver_path)

# COMMAND ----------

spark.read.format("delta").load(silver_path).count()
