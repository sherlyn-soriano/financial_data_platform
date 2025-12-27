from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from typing import List, Dict
from datetime import datetime

def check_null_counts(df: DataFrame, columns: List[str]) -> int:
    total = 0
    for col in columns:
        if col in df.columns:
            total += df.filter(F.col(col).isNull()).count()
    return total

def check_duplicates(df: DataFrame, key_columns: List[str]) -> int:
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    return total - distinct

def check_value_range(df: DataFrame, column: str, min_val: float, max_val: float) -> int:
    return df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()

def check_allowed_values(df: DataFrame, column: str, allowed: List) -> int:
    return df.filter(~F.col(column).isin(allowed) & F.col(column).isNotNull()).count()

def calculate_completeness(df: DataFrame, columns: List[str]) -> float:
    total_cells = df.count() * len(columns)
    if total_cells == 0:
        return 100.0
    null_cells = 0
    for col in columns:
        if col in df.columns:
            null_cells += df.filter(F.col(col).isNull()).count()
    return ((total_cells - null_cells) / total_cells) * 100

class QualityReport:
    def __init__(self):
        self.checks = {}
        self.passed = True
        self.timestamp = datetime.now()

    def add_check(self, name: str, result: str, passed: bool):
        self.checks[name] = {'result': result, 'passed': passed}
        if not passed:
            self.passed = False

    def summary(self) -> str:
        lines = [f"Quality Report - {self.timestamp}", f"Status: {'PASSED' if self.passed else 'FAILED'}\n"]
        for name, data in self.checks.items():
            status = 'OK' if data['passed'] else 'FAIL'
            lines.append(f"[{status}] {name}: {data['result']}")
        return '\n'.join(lines)

def validate_bronze(df: DataFrame, key_columns: List[str]) -> QualityReport:
    report = QualityReport()

    row_count = df.count()
    report.add_check("Row Count", f"{row_count}", True)

    nulls = check_null_counts(df, key_columns)
    report.add_check("Null Keys", f"{nulls}", nulls == 0)

    dupes = check_duplicates(df, key_columns)
    report.add_check("Duplicates", f"{dupes}", dupes == 0)

    return report

def validate_silver_transactions(df: DataFrame) -> QualityReport:
    report = QualityReport()

    nulls = check_null_counts(df, ['transaction_id', 'amount', 'customer_id', 'merchant_id'])
    report.add_check("Critical Nulls", f"{nulls}", nulls == 0)

    dupes = check_duplicates(df, ['transaction_id'])
    report.add_check("Duplicates", f"{dupes}", dupes == 0)

    invalid_amounts = check_value_range(df, 'amount', 0.01, 10000000)
    report.add_check("Amount Range", f"{invalid_amounts} invalid", invalid_amounts == 0)

    invalid_status = check_allowed_values(df, 'status', ['pending', 'completed', 'failed', 'cancelled'])
    report.add_check("Status Values", f"{invalid_status} invalid", invalid_status == 0)

    completeness = calculate_completeness(df, ['transaction_id', 'amount', 'customer_id', 'transaction_date'])
    report.add_check("Completeness", f"{completeness:.1f}%", completeness >= 95.0)

    return report

def validate_silver_customers(df: DataFrame) -> QualityReport:
    report = QualityReport()

    nulls = check_null_counts(df, ['customer_id', 'dni'])
    report.add_check("Critical Nulls", f"{nulls}", nulls == 0)

    dupes = check_duplicates(df, ['customer_id'])
    report.add_check("Duplicates", f"{dupes}", dupes == 0)

    invalid_segment = check_allowed_values(df, 'customer_segment', ['VIP', 'Premium', 'Standard', 'Basic'])
    report.add_check("Segment Values", f"{invalid_segment} invalid", invalid_segment == 0)

    invalid_risk = check_value_range(df, 'risk_score', 0.0, 1.0)
    report.add_check("Risk Score Range", f"{invalid_risk} invalid", invalid_risk == 0)

    return report

def validate_silver_merchants(df: DataFrame) -> QualityReport:
    report = QualityReport()

    nulls = check_null_counts(df, ['merchant_id'])
    report.add_check("Critical Nulls", f"{nulls}", nulls == 0)

    dupes = check_duplicates(df, ['merchant_id'])
    report.add_check("Duplicates", f"{dupes}", dupes == 0)

    return report

def add_quality_flags_bronze(df: DataFrame, key_columns: List[str]) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for col in key_columns:
        if col in df.columns:
            quality_issues = F.when(
                F.col(col).isNull(),
                F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
            ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn(
        'is_valid',
        F.size(F.col('quality_issues')) == 0
    )
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags

def add_quality_flags_customers(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))
    for col in ['customer_id', 'dni', 'is_active']:
        if col in df.columns:
            quality_issues = F.when(
                F.col(col).isNull(),
                F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
            ).otherwise(quality_issues)

    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    quality_issues = F.when(
        (F.col('email').isNotNull()) & (~F.col('email').rlike(email_pattern)),
        F.array_union(quality_issues, F.array(F.lit('invalid_email_format')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('email').isNull()) | (F.col('email') == ''),
        F.array_union(quality_issues, F.array(F.lit('missing_email')))
    ).otherwise(quality_issues)

    valid_segments = ['VIP', 'Premium', 'Standard', 'Basic']
    quality_issues = F.when(
        (F.col('customer_segment').isNotNull()) & (~F.col('customer_segment').isin(valid_segments)),
        F.array_union(quality_issues, F.array(F.lit('invalid_customer_segment')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('risk_score').isNotNull()) & ((F.col('risk_score') < 0.0) | (F.col('risk_score') > 1.0)),
        F.array_union(quality_issues, F.array(F.lit('invalid_risk_score_range')))
    ).otherwise(quality_issues)

    known_typos = ['Limasse']
    quality_issues = F.when(
        F.col('city').isin(known_typos),
        F.array_union(quality_issues, F.array(F.lit('city_typo')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn(
        'is_valid',
        F.size(F.col('quality_issues')) == 0
    )
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags

def add_quality_flags_merchants(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))
    quality_issues = F.when(
        F.col('merchant_id').isNull(),
        F.array_union(quality_issues, F.array(F.lit('null_merchant_id')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('category').isNull()) | (F.col('category') == ''),
        F.array_union(quality_issues, F.array(F.lit('missing_category')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('mcc_code').isNotNull()) & ((F.col('mcc_code') < 1000) | (F.col('mcc_code') > 9999)),
        F.array_union(quality_issues, F.array(F.lit('invalid_mcc_code')))
    ).otherwise(quality_issues)

    known_typos = ['Peruss']
    quality_issues = F.when(
        F.col('country').isin(known_typos),
        F.array_union(quality_issues, F.array(F.lit('country_typo')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('merchant_name').isNotNull()) &
        (F.col('merchant_name').contains('SA')) &
        (~F.col('merchant_name').contains('S.A.')),
        F.array_union(quality_issues, F.array(F.lit('merchant_name_format_issue')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn(
        'is_valid',
        F.size(F.col('quality_issues')) == 0
    )
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags

def add_quality_flags_transactions(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))
    for col in ['transaction_id', 'customer_id', 'amount']:
        if col in df.columns:
            quality_issues = F.when(
                F.col(col).isNull(),
                F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
            ).otherwise(quality_issues)

    quality_issues = F.when(
        F.col('merchant_id').isNull(),
        F.array_union(quality_issues, F.array(F.lit('missing_merchant_id')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('amount').isNotNull()) & (F.col('amount') < 0),
        F.array_union(quality_issues, F.array(F.lit('negative_amount')))
    ).otherwise(quality_issues)

    valid_statuses = ['pending', 'completed', 'failed', 'cancelled']
    quality_issues = F.when(
        (F.col('status').isNotNull()) & (~F.col('status').isin(valid_statuses)),
        F.array_union(quality_issues, F.array(F.lit('invalid_status')))
    ).otherwise(quality_issues)

    valid_currencies = ['PEN', 'USD', 'EUR']
    quality_issues = F.when(
        (F.col('currency').isNotNull()) & (~F.col('currency').isin(valid_currencies)),
        F.array_union(quality_issues, F.array(F.lit('invalid_currency')))
    ).otherwise(quality_issues)

    valid_channels = ['online', 'mobile', 'atm', 'branch', 'pos', 'agent']
    quality_issues = F.when(
        (F.col('channel').isNotNull()) & (~F.col('channel').isin(valid_channels)),
        F.array_union(quality_issues, F.array(F.lit('invalid_channel')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn(
        'is_valid',
        F.size(F.col('quality_issues')) == 0
    )
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags

def generate_quality_summary(df: DataFrame) -> Dict:
    total_records = df.count()
    valid_records = df.filter(F.col('is_valid') == True).count()
    invalid_records = total_records - valid_records

    issues_exploded = df.filter(F.size(F.col('quality_issues')) > 0).select(
        F.explode('quality_issues').alias('issue_type')
    )

    issue_counts = {}
    if issues_exploded.count() > 0:
        issue_counts = {
            row['issue_type']: row['count']
            for row in issues_exploded.groupBy('issue_type').count().collect()
        }

    return {
        'total_records': total_records,
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'data_quality_score': (valid_records / total_records * 100) if total_records > 0 else 0.0,
        'issue_breakdown': issue_counts,
        'timestamp': datetime.now().isoformat()
    }

def print_quality_summary(summary: Dict):
    print('\n' + '='*60)
    print('BRONZE LAYER QUALITY SUMMARY')
    print('='*60)
    print(f"Total Records: {summary['total_records']}")
    print(f"Valid Records: {summary['valid_records']}")
    print(f"Invalid Records: {summary['invalid_records']}")
    print(f"Data Quality Score: {summary['data_quality_score']:.2f}%")

    if summary['issue_breakdown']:
        print('\nIssue Breakdown:')
        for issue, count in sorted(summary['issue_breakdown'].items(), key=lambda x: x[1], reverse=True):
            print(f"  - {issue}: {count}")

    print('='*60 + '\n')
