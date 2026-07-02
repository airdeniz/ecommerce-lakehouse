FROM apache/airflow:2.9.1

USER root

# git: needed by dbt.
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow

# Python deps: dbt-* and the spark provider — the transformation/orchestration
# stack. dbt-spark talks to the Spark Thrift Server over PyHive (HiveServer2),
# so no local Spark engine is needed here.
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.9.0 \
    dbt-core==1.8.0 \
    dbt-spark[PyHive]==1.8.0
