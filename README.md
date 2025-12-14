# Financial dasta engineer platform 

## 📊 Project Overview 

 A real-time financial data platform demonstrating enterprise-level Azure Data Engineering practices:

 - Processes 1M+ transactions across Bronze/Silver/Gold layers
 - unknow% fraud detection accuracy using ML models
 - unknow% faster
 - 100% Infraestructure as Code with Pulumi

 ## 🏗️ Architecture

 ### Tech Stack
 - **Azure Data Factory** - Orchestration & metadata-driven pipelines
 - **Azure Databricks** - PySpark transformation & ML
 - **Azure Data Lake Gen2** - Medallion architecture (Bronze/Silver/Gold)
 - **Delta Lake** - ACID transactions, time travel
 - **Pulumi** - Infraestructure as Code
 - **Azure DevOps** - CI/CD pipelines

 ### Medallion Architecture

- **🥉 Bronze**: Raw ingestion (Parquet)
- **🥈 Silver**: Cleansed, validated (Delta Lake + SCD Type 2)
- **🥇 Gold**: Star schema, aggregations, ML predictions



## 📂 Repository Structure
```
credicorp-transaction-intelligence-platform/
├── README.md                          
├── LICENSE                            
├── .gitignore                        
├── .env.example                  
├── requirements.txt                   # Python dependencies
│
├── architecture/            
│   ├── architecture-diagram.png
│   ├── data-flow.png
│   └── star-schema-erd.png
│
├── data/                            
│   ├── customers.csv
│   ├── merchants.csv
│   └── transactions/
│       └── transactions_2024-*.csv
│
├── scripts/                           # Automation scripts
│   ├── generate_data.py              # Synthetic data generator
│   ├── upload_to_datalake.py         # Upload to Azure
│   └── local_runner.py               # Local testing (optional)
│
├── infrastructure/                    # Pulumi IaC
│   ├── Pulumi.yaml                   # Project config
│   ├── Pulumi.dev.yaml               # Dev stack
│   ├── __main__.py                   # Main entrypoint
│   ├── requirements.txt              # Pulumi dependencies
│   └── platform/                     # Infrastructure modules
│       ├── __init__.py
│       ├── resource_group.py
│       ├── storage.py
│       ├── key_vault.py
│       ├── databricks_ws.py
│       └── data_factory.py
│
├── databricks/                   
│   ├── notebooks/
│   │   ├── 01_bronze_to_silver.py
│   │   ├── 02_silver_to_gold.py
│   │   └── 03_fraud_detection_ml.py
│   ├── libs/
│   │   └── data_quality.py           # Reusable functions
│   ├── tests/
│   │   └── test_transformations.py
│   └── deploy_notebooks.sh
│
├── azure-data-factory/               # ADF artifacts (JSON)
│   ├── pipelines/
│   ├── datasets/
│   └── linkedservices/
│
├── sql/                              # SQL scripts
│   ├── star_schema_ddl.sql
│   └── metadata_control_table.sql
│
├── devops/                           # CI/CD
│   ├── azure-pipelines.yml
│   └── README.md
│
├── docs/                             # Documentation
│   ├── ARCHITECTURE.md
│   ├── DATA_DICTIONARY.md
│   ├── RUNBOOK.md
│   └── PERFORMANCE_RESULTS.md
│
└── results/                          # Metrics & screenshots
    ├── performance-metrics.xlsx
    ├── cost-analysis.png
    └── dashboard-screenshots/
```