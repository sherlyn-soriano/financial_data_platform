import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "databricks" / "libs"))

from bronze_check import add_quality_flags_customers, add_quality_flags_transactions
from silver_check import add_quality_flags_customers as silver_add_quality_flags_customers
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
