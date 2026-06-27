FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    dbt-core==1.8.0 \
    dbt-spark[PyHive]==1.8.0