from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List
from datetime import datetime


# SECTION 1: BASIC CHECKS
def check_null_counts(df: DataFrame, critical_columns: List[str]) -> Dict[str, int]:
    null_counts = {}

    for col in critical_columns:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        else:
            null_counts[col] = -1 

    return null_counts

def check_duplicate_keys(df: DataFrame, key_columns: List[str]) -> int:
    total_count = df.count()
    distinct_count = df.select(key_columns).distinct().count()

    duplicates = total_count - distinct_count
    return duplicates

# SECTION 2: BUSINESS LOGIC CHECKS

def check_value_ranges(df: DataFrame, column: str, min_val: float, max_val: float) -> int:
    out_of_range = df.filter(
        (F.col(column) < min_val) | (F.col(column) > max_val)
    ).count()

    return out_of_range


def check_date_freshness(df: DataFrame, date_column: str, max_age_days: int) -> int:
    cutoff_date = datetime.now().date()

    stale_records = df.filter(
        F.datediff(F.lit(cutoff_date), F.col(date_column)) > max_age_days
    ).count()

    return stale_records


def check_allowed_values(df: DataFrame, column: str, allowed_values: List) -> Dict:

    actual_values = [row[column] for row in df.select(column).distinct().collect()] 

    invalid_values = [v for v in actual_values if v not in allowed_values and v is not None]

    invalid_counts = {}
    for val in invalid_values:
        count = df.filter(F.col(column) == val).count()
        invalid_counts[val] = count

    return invalid_counts



