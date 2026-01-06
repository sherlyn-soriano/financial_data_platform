from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import List, Callable, Any
from dataclasses import dataclass

@dataclass
class ValidationRule:
    column: str
    check_name: str
    validation_expr: Callable[[Any], Any]


def create_metric_validation_rules(metric_columns: List[str]) -> List[ValidationRule]:
    rules = []
    for col_name in metric_columns:
        rules.append(ValidationRule(
            column=col_name,
            check_name=f'null_{col_name}',
            validation_expr=lambda c: c.isNull()
        ))
        rules.append(ValidationRule(
            column=col_name,
            check_name=f'negative_{col_name}',
            validation_expr=lambda c: c.isNotNull() & (c < 0)
        ))
    return rules


def add_quality_flags(df: DataFrame, validation_rules: List[ValidationRule]) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for rule in validation_rules:
        if rule.column in df.columns:
            quality_issues = F.when(
                rule.validation_expr(F.col(rule.column)),
                F.array_union(quality_issues, F.array(F.lit(rule.check_name)))
            ).otherwise(quality_issues)

    return (df
            .withColumn('quality_issues', quality_issues)
            .withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
            .withColumn('quality_check_timestamp', F.current_timestamp())
    )


def add_quality_flags_aggregations(df: DataFrame, metric_columns: List[str]) -> DataFrame:
    rules = create_metric_validation_rules(metric_columns)
    return add_quality_flags(df, rules)
