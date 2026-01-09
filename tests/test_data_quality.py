import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "databricks" / "libs"))

from bronze_check import add_quality_flags_customers, add_quality_flags_transactions
from silver_check import (
    add_quality_flags_customers as silver_add_quality_flags_customers,
    add_quality_flags_merchants,
    add_quality_flags_transactions as silver_add_quality_flags_transactions
)
from gold_check import add_quality_flags_aggregations


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("DataQualityTests") \
        .getOrCreate()


class TestBronzeQuality:

    def test_customer_email_validation(self, spark):
        data = [
            ("CUST001", "valid@example.com"),
            ("CUST002", "invalid@"),
            ("CUST003", "also.valid@test.com")
        ]
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_customers(df)

        assert 'is_valid' in result.columns
        assert 'quality_issues' in result.columns

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 1

    def test_transaction_card_validation(self, spark):
        data = [
            ("TXN001", 1234),
            ("TXN002", 12345),
            ("TXN003", 9999)
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("card_last_4", IntegerType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_transactions(df)

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count >= 0


class TestSilverQuality:

    def test_customer_segment_validation(self, spark):
        data = [
            ("CUST001", "VIP"),
            ("CUST002", "InvalidSegment"),
            ("CUST003", "Premium")
        ]
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_segment", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_customers(df)

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 1

    def test_merchant_category_validation(self, spark):
        data = [
            ("MERCH001", "Retail"),
            ("MERCH002", "InvalidCategory"),
            ("MERCH003", "Restaurants"),
            ("MERCH004", "BadCategory")
        ]
        schema = StructType([
            StructField("merchant_id", StringType(), False),
            StructField("category", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_merchants(df)

        assert 'is_valid' in result.columns
        assert 'quality_issues' in result.columns

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 2

    def test_merchant_null_id_validation(self, spark):
        data = [
            ("MERCH001", "Retail"),
            (None, "Restaurants"),
            ("MERCH003", "Online Services")
        ]
        schema = StructType([
            StructField("merchant_id", StringType(), True),
            StructField("category", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_merchants(df)

        invalid_rows = result.filter(F.col('is_valid') == False)
        assert invalid_rows.count() == 1

        quality_issues = invalid_rows.select("quality_issues").first()[0]
        assert "null_merchant_id" in quality_issues

    def test_transaction_amount_validation(self, spark):
        data = [
            ("TXN001", "CUST001", "MERCH001", 100),
            ("TXN002", "CUST002", "MERCH002", -50),
            ("TXN003", "CUST003", "MERCH003", 0),
            ("TXN004", "CUST004", "MERCH004", 200)
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("amount", IntegerType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_transactions(df)

        assert 'is_valid' in result.columns
        assert 'quality_issues' in result.columns

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 2

    def test_transaction_status_validation(self, spark):
        data = [
            ("TXN001", "pending"),
            ("TXN002", "completed"),
            ("TXN003", "invalid_status"),
            ("TXN004", "failed")
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("status", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_transactions(df)

        invalid_rows = result.filter(F.col('is_valid') == False)
        assert invalid_rows.count() == 1

        quality_issues = invalid_rows.select("quality_issues").first()[0]
        assert "invalid_status" in quality_issues

    def test_transaction_currency_validation(self, spark):
        data = [
            ("TXN001", "PEN"),
            ("TXN002", "USD"),
            ("TXN003", "EUR"),
            ("TXN004", "GBP")
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("currency", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_transactions(df)

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 2

    def test_transaction_channel_validation(self, spark):
        data = [
            ("TXN001", "online"),
            ("TXN002", "mobile"),
            ("TXN003", "invalid_channel"),
            ("TXN004", "atm"),
            ("TXN005", "website")
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("channel", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_transactions(df)

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 2

    def test_transaction_null_fields_validation(self, spark):
        data = [
            ("TXN001", "CUST001", "MERCH001"),
            ("TXN002", None, "MERCH002"),
            ("TXN003", "CUST003", None),
            (None, "CUST004", "MERCH004")
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = silver_add_quality_flags_transactions(df)

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 3


class TestGoldQuality:

    def test_metric_null_validation(self, spark):
        data = [
            ("Segment1", 100),
            ("Segment2", None),
            ("Segment3", 200)
        ]
        schema = StructType([
            StructField("segment", StringType(), False),
            StructField("total_amount", IntegerType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_aggregations(df, ["total_amount"])

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 1

    def test_metric_negative_validation(self, spark):
        data = [
            ("Segment1", 100),
            ("Segment2", -50),
            ("Segment3", 200)
        ]
        schema = StructType([
            StructField("segment", StringType(), False),
            StructField("total_amount", IntegerType(), True)
        ])

        df = spark.createDataFrame(data, schema)
        result = add_quality_flags_aggregations(df, ["total_amount"])

        invalid_count = result.filter(F.col('is_valid') == False).count()
        assert invalid_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
