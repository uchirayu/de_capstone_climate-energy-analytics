"""
Airflow DAG for dbt transformations (bronze to silver, Parquet output)
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


def create_transformation_dag():
    with DAG(
        dag_id='dbt_transformation_pipeline',
        default_args=DEFAULT_ARGS,
        description='Run dbt transformations (bronze to silver, Parquet output)',
        schedule_interval=None,  # Set to desired cron for prod
        start_date=days_ago(1),
        catchup=False,
        tags=['production', 'dbt', 'athena', 'transformation'],
    ) as dag:

        dbt_run = BashOperator(
            task_id='dbt_run_transformations',
            bash_command='export PATH=$PATH:/home/airflow/.local/bin && dbt run',
            cwd='/dbt',
            env={
                'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
                'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
                'AWS_DEFAULT_REGION': os.environ.get('AWS_REGION', ''),
                # Add any required env vars here
            },
        )

        # Add Athena repair table steps for each silver table
        repair_openmeteo = BashOperator(
            task_id="repair_openmeteo_silver_table",
            bash_command="aws athena start-query-execution --query-string 'MSCK REPAIR TABLE silver.openmeteo_historical;' --work-group primary --result-configuration OutputLocation=s3://climate-energy-raw-data/athena_results/",
        )
        repair_openweather = BashOperator(
            task_id="repair_openweather_silver_table",
            bash_command="aws athena start-query-execution --query-string 'MSCK REPAIR TABLE silver.openweather_monthly;' --work-group primary --result-configuration OutputLocation=s3://climate-energy-raw-data/athena_results/",
        )
        repair_sensor = BashOperator(
            task_id="repair_sensor_silver_table",
            bash_command="aws athena start-query-execution --query-string 'MSCK REPAIR TABLE silver.sensor_readings;' --work-group primary --result-configuration OutputLocation=s3://climate-energy-raw-data/athena_results/",
        )
        repair_eia = BashOperator(
            task_id="repair_eia_silver_table",
            bash_command="aws athena start-query-execution --query-string 'MSCK REPAIR TABLE silver.eia_energy;' --work-group primary --result-configuration OutputLocation=s3://climate-energy-raw-data/athena_results/",
        )
        repair_noaa = BashOperator(
            task_id="repair_noaa_silver_table",
            bash_command="aws athena start-query-execution --query-string 'MSCK REPAIR TABLE silver.noaa_historical;' --work-group primary --result-configuration OutputLocation=s3://climate-energy-raw-data/athena_results/",
        )

        # Set dependencies: run repairs after dbt transformations
        dbt_run >> [repair_openmeteo, repair_openweather, repair_sensor, repair_eia, repair_noaa]

    return dag

globals()['dbt_transformation_pipeline'] = create_transformation_dag()
