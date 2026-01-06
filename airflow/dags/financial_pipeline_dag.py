from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-eng@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

databricks_conn_id = 'databricks_default'
cluster_id = '{{ var.value.databricks_cluster_id }}'

bronze_notebook_config = {
    'existing_cluster_id': cluster_id,
    'timeout_seconds': 1800,
}

silver_notebook_config = {
    'existing_cluster_id': cluster_id,
    'timeout_seconds': 1800,
}

gold_notebook_config = {
    'existing_cluster_id': cluster_id,
    'timeout_seconds': 1800,
}

with DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='End-to-end financial data pipeline',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['production', 'databricks', 'financial'],
) as dag:

    bronze_customers = DatabricksSubmitRunOperator(
        task_id='bronze_ingest_customers',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/bronze/01_ingest_customers',
            **bronze_notebook_config
        },
    )

    bronze_merchants = DatabricksSubmitRunOperator(
        task_id='bronze_ingest_merchants',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/bronze/02_ingest_merchants',
            **bronze_notebook_config
        },
    )

    bronze_transactions = DatabricksSubmitRunOperator(
        task_id='bronze_ingest_transactions',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/bronze/03_ingest_transactions',
            **bronze_notebook_config
        },
    )

    silver_customers = DatabricksSubmitRunOperator(
        task_id='silver_transform_customers',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/silver/01_transform_customers',
            **silver_notebook_config
        },
    )

    silver_merchants = DatabricksSubmitRunOperator(
        task_id='silver_transform_merchants',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/silver/02_transform_merchants',
            **silver_notebook_config
        },
    )

    silver_transactions = DatabricksSubmitRunOperator(
        task_id='silver_transform_transactions',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/silver/03_transform_transactions',
            **silver_notebook_config
        },
    )

    gold_customer_metrics = DatabricksSubmitRunOperator(
        task_id='gold_customer_metrics',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/gold/01_customer_metrics',
            **gold_notebook_config
        },
    )

    gold_transaction_metrics = DatabricksSubmitRunOperator(
        task_id='gold_transaction_metrics',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/gold/02_transaction_metrics',
            **gold_notebook_config
        },
    )

    gold_fraud_analysis = DatabricksSubmitRunOperator(
        task_id='gold_fraud_analysis',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/gold/03_fraud_analysis',
            **gold_notebook_config
        },
    )

    gold_merchant_performance = DatabricksSubmitRunOperator(
        task_id='gold_merchant_performance',
        databricks_conn_id=databricks_conn_id,
        notebook_task={
            'notebook_path': '/Workspace/Repos/databricks/notebooks/gold/04_merchant_performance',
            **gold_notebook_config
        },
    )

    bronze_customers >> silver_customers >> gold_customer_metrics
    bronze_merchants >> silver_merchants >> gold_merchant_performance
    bronze_transactions >> silver_transactions >> [gold_transaction_metrics, gold_fraud_analysis]
    [silver_customers, silver_transactions] >> gold_fraud_analysis
    [silver_merchants, silver_transactions] >> gold_merchant_performance
