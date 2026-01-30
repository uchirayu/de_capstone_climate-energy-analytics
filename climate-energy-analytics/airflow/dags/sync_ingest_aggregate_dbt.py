"""
Airflow DAG to orchestrate data ingestion, aggregation, and dbt transformation daily at 2 AM.
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
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

def sync_condition(context, dag_run_obj):
    # Always trigger
    return dag_run_obj

def create_sync_dag():
    with DAG(
        dag_id='sync_ingest_aggregate_dbt',
        default_args=DEFAULT_ARGS,
        description='Synchronize ingestion, aggregation, and dbt transformation at 2 AM',
        schedule_interval='0 2 * * *',
        start_date=days_ago(1),
        catchup=False,
        tags=['sync', 'orchestration'],
    ) as dag:

        trigger_ingest = TriggerDagRunOperator(
            task_id='trigger_data_ingestion',
            trigger_dag_id='data_ingestion_pipeline',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )

        trigger_dbt = TriggerDagRunOperator(
            task_id='trigger_dbt_transformation',
            trigger_dag_id='dbt_transformation_pipeline',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )

        trigger_gold_dbt = TriggerDagRunOperator(
            task_id='trigger_dbt_gold_transformation',
            trigger_dag_id='dbt_gold_transformation_pipeline',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )

        trigger_ingest >> trigger_dbt >> trigger_gold_dbt

    return dag

globals()['sync_ingest_aggregate_dbt'] = create_sync_dag()
