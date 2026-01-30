"""
Airflow DAG for dbt gold transformations (gold models only)
"""
import os
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_gold_transformation_dag():
    with DAG(
        dag_id='dbt_gold_transformation_pipeline',
        default_args=DEFAULT_ARGS,
        description='Run dbt gold transformation (gold models only)',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        tags=['production', 'dbt', 'athena', 'gold'],
    ) as dag:

        dbt_run_gold = BashOperator(
            task_id='dbt_run_gold_transformations',
            bash_command='export PATH=$PATH:/home/airflow/.local/bin && dbt run --profile gold --select path:models/gold',
            cwd='/dbt',
            env={
                'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
                'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
                'AWS_DEFAULT_REGION': os.environ.get('AWS_REGION', ''),
            },
        )

    return dag

globals()['dbt_gold_transformation_pipeline'] = create_gold_transformation_dag()
