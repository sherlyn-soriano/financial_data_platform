FROM apache/airflow:2.7.3-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks==5.0.0 \
    apache-airflow-providers-microsoft-azure==8.0.0
