"""
Airflow DAG: crypto batch pipeline.

Orchestrates the daily batch path end to end:

    batch_ingest  ->  load_to_warehouse  ->  dbt_run  ->  dbt_test

The streaming path (producer + consumer) runs continuously as separate
services, not on this schedule — this DAG handles the scheduled batch work
and the dbt transforms that model both batch and stream data.

Retries, alerting hooks, and a clear task graph are included because that's
what interviewers look for in a "real" pipeline.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,  # wire up a real alert (Slack/email) here in a job setting
}

# Paths inside the Airflow container (see docker-compose volume mounts).
PROJECT = "/opt/airflow/project"
DBT_DIR = f"{PROJECT}/dbt/crypto"

with DAG(
    dag_id="crypto_batch_pipeline",
    description="Ingest -> load -> transform -> test crypto market data",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "batch", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="batch_ingest",
        bash_command=f"cd {PROJECT} && python ingestion/batch_ingest.py",
    )

    load = BashOperator(
        task_id="load_to_warehouse",
        bash_command=f"cd {PROJECT} && python ingestion/load_to_warehouse.py",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    ingest >> load >> dbt_deps >> dbt_run >> dbt_test
