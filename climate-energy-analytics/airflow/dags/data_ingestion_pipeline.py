"""
Airflow DAG for data ingestion (Kafka, etc.)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import sys
sys.path.append('/opt/airflow/dags')
from aggregate_counts import aggregate_and_display_counts
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

def create_ingestion_dag():
    with DAG(
        dag_id='data_ingestion_pipeline',
        default_args=DEFAULT_ARGS,
        description='Ingest data from Kafka and other sources',
        schedule_interval=None,  # Set to desired cron for prod
        start_date=days_ago(1),
        catchup=False,
        tags=['production', 'kafka', 'ingestion'],
    ) as dag:


        ingest_task = BashOperator(
            task_id='run_kafka_ingest',
            bash_command='run_all_kafka_data_posts_and_consume.sh',
            cwd='/opt/airflow/dags',
        )

        aggregate_counts_task = PythonOperator(
            task_id='aggregate_and_display_counts',
            python_callable=aggregate_and_display_counts,
            op_args=["/opt/airflow/logs/producer.log", "/opt/airflow/logs/consumer.log"],
        )

        ingest_task >> aggregate_counts_task

    return dag

globals()['data_ingestion_pipeline'] = create_ingestion_dag()
