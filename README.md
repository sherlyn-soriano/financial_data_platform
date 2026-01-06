# Financial Data Pipeline

End-to-end data pipeline processing 1M+ financial transactions using **Medallion Architecture** (Bronze → Silver → Gold) with data quality validation.

## Skills Demonstrated

**Data Engineering**: PySpark • Delta Lake • ETL • Data Quality Validation • Medallion Architecture
**Orchestration**: Apache Airflow • Docker • DAG Design
**Cloud**: Azure Databricks • Azure Data Lake Gen2 • Azure Key Vault
**Infrastructure**: Pulumi (IaC)
**Development**: Python 3.12 • pytest • Git

## Architecture

```
Raw Data (JSON/CSV)
    ↓
BRONZE: Ingest + Validate (email, phone, DNI formats)
    ↓
SILVER: Transform + Clean (business rules, standardization)
    ↓
GOLD: Aggregate Metrics (customer/transaction/merchant analytics)
```

## What I Built

- Bronze Layer: Raw data ingestion with validation (3 notebooks)
- Silver Layer: Data transformation and cleansing (3 notebooks)
- Gold Layer: Analytics aggregations (3 notebooks)
- Data Quality Framework: Rule-based validation engine
- Airflow Orchestration: DAG with dependencies, retries, scheduling
- Infrastructure: Automated Azure provisioning with Pulumi
- Testing: Unit tests for validation logic

## Project Structure

```
airflow/dags/          Airflow DAG orchestrating pipeline
databricks/
├── libs/              Data quality validation
├── notebooks/
│   ├── bronze/        Ingestion (3)
│   ├── silver/        Transform (3)
│   └── gold/          Analytics (3)
infrastructure/        Pulumi Azure IaC
scripts/               Data generation
tests/                 Unit tests
docker-compose.yml     Airflow stack
```

## Quick Start

```bash
docker-compose up -d
```

Access Airflow: http://localhost:8080 (airflow/airflow)

Configure Databricks:
- Admin → Connections → Add Databricks
- Admin → Variables → databricks_cluster_id

Deploy infrastructure:
```bash
cd infrastructure && pulumi up
```

Generate data:
```bash
python scripts/generate_data.py
python scripts/upload_to_datalake.py
```

## Key Features

- Custom data quality validation framework
- Apache Airflow orchestration with Docker
- Distributed PySpark processing
- Delta Lake ACID transactions
- Infrastructure as Code with Pulumi
- Unit testing with pytest
