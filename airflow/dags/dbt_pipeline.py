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

    # Emit target/catalog.json (and refresh manifest.json/run_results.json) so the
    # optional DataHub dbt ingestion has full column-level metadata to read. Harmless
    # when DataHub isn't running — it just writes artifacts into dbt/target.
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt",
    )

    dbt_run >> dbt_test >> dbt_docs_generate