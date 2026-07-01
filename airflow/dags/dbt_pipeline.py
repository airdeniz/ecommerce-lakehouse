from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    description="Run dbt transformations on Iceberg lakehouse",
    schedule_interval="0 2 * * *",  # her gece 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Come up active after a fresh `down -v` reset instead of the Airflow
    # default (paused), so the nightly schedule runs without a manual unpause.
    is_paused_upon_creation=False,
    tags=["dbt", "lakehouse"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
    )

    dbt_run >> dbt_test