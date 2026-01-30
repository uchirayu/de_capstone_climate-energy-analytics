from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_kafka_data_posts_and_consume_daily',
    default_args=default_args,
    description='Run Kafka data post and consume script daily',
    schedule_interval='0 2 * * *',  # Every day at 2:00 AM
    start_date=datetime(2026, 1, 15),
    catchup=False,
)

run_script = BashOperator(
    task_id='run_kafka_data_posts_and_consume_daily',
    bash_command="'/opt/airflow/dags/run_all_kafka_data_posts_and_consume.sh'",
    dag=dag,
)
