from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ETL/ is mounted into the container at this path (see docker-compose.yaml).
# The Airflow image has ETL/requirements.txt installed straight into its own
# Python (see Dockerfile) — no separate venv needed inside the container.
ETL_DIR = "/opt/airflow/etl"

SOURCES = ["relational_seed", "transactions", "inventory", "returns"]
# Typer registers relational_seed's command as "relational-seed"; the others
# match their source name exactly.
EXTRACT_COMMAND_NAME = {"relational_seed": "relational-seed"}

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl_phase1",
    description="randomapi.dev -> land raw -> load BigQuery raw_ecommerce -> build Phase 1 marts",
    schedule="@daily",
    start_date=datetime(2026, 8, 28),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "phase1", "etl"],
) as dag:
    load_tasks = {}

    for source in SOURCES:
        extract_command = EXTRACT_COMMAND_NAME.get(source, source)

        extract_task = BashOperator(
            task_id=f"extract_{source}",
            bash_command=f"cd {ETL_DIR} && python cli.py extract {extract_command}",
        )
        load_task = BashOperator(
            task_id=f"load_{source}",
            bash_command=f"cd {ETL_DIR} && python cli.py load bigquery-all {source} {{{{ ds }}}}",
        )
        extract_task >> load_task
        load_tasks[source] = load_task

    # marts.py only reads relational_seed-sourced tables (plan-project.md §4)
    build_marts = BashOperator(
        task_id="build_marts",
        bash_command=f"cd {ETL_DIR} && python cli.py load marts",
    )
    load_tasks["relational_seed"] >> build_marts

    # Real validation already happened inside each load task (load_bigquery_all
    # aborts on a failed check, see load/pipeline.py) — this is a convergence
    # point: one node that only succeeds once everything upstream did, so
    # alerting/notification has a single place to hang off.
    data_quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command="echo 'All loads and marts succeeded for this run.'",
    )
    [build_marts, load_tasks["transactions"], load_tasks["inventory"], load_tasks["returns"]] >> data_quality_gate
