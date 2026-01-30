from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ge_silver_data_quality",
    default_args=default_args,
    start_date=datetime(2026, 1, 24),
    schedule_interval=None,   # Triggered after dbt
    catchup=False,
    tags=["quality", "great_expectations", "silver"],
) as dag:

    run_ge_checkpoint = BashOperator(
        task_id="run_great_expectations_on_silver",
        bash_command="""
        docker exec great-expectations bash -c 'cd /great_expectations && python /usr/local/lib/python3.10/site-packages/great_expectations/cli.py checkpoint run checkpoints/silver_checkpoints.yml'
        """,
    )

    run_ge_checkpoint
