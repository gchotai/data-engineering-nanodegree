from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'gunja',
    'start_date': datetime(2020, 9, 24),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=3),
    'catchup':False,
    'email_on_retry': False,
    'max_active_runs':1
}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

city_temp_to_redshift = StageToRedshiftOperator(
    task_id='city_temp_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.city_temp_from_bucket",
    s3_bucket="temperature-processed-data",
    s3_key="city_temperature"
)


immigration_to_redshift = StageToRedshiftOperator(
    task_id='immigration_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.immigration_from_bucket",
    s3_bucket="temperature-processed-data",
    s3_key="immigration"
)

city_demographics_to_redshift = StageToRedshiftOperator(
    task_id='city_demographics_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.cities_demographics_from_bucket",
    s3_bucket="temperature-processed-data",
    s3_key="city_demographics"
)

airport_code_to_redshift = StageToRedshiftOperator(
    task_id='airport_code_data',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.airport_code_from_bucket",
    s3_bucket="temperature-processed-data",
    s3_key="airport_code"
)

load_city_temp_dimension_table = LoadDimensionOperator(
    task_id='Load_city_temp_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.city_temp",
    query=SqlQueries.city_temp_table_insert
)

load_immigration_dimension_table = LoadDimensionOperator(
    task_id='Load_immigration_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.immigration",
    query=SqlQueries.immigration_table_insert
)

load_city_demographics_dimension_table = LoadDimensionOperator(
    task_id='Load_city_demographics_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.cities_demographics",
    query=SqlQueries.cities_demographics_table_insert
)

load_airport_code_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_code_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.airport_code",
    query=SqlQueries.airport_code_table_insert
)

immigration_city_temp_fact_table = LoadFactOperator(
    task_id='Load_dim_to_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="immigration_city_temp",
    query=SqlQueries.immigration_city_temp_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["city_temp",
        "immigration",
        "cities_demographics",   
        "airport_code",
        "immigration_city_temp"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> city_temp_to_redshift
start_operator >> immigration_to_redshift
start_operator >> city_demographics_to_redshift
start_operator >> airport_code_to_redshift


city_temp_to_redshift >> load_city_temp_dimension_table
immigration_to_redshift >> load_immigration_dimension_table
airport_code_to_redshift >> load_airport_code_dimension_table
city_demographics_to_redshift >> load_city_demographics_dimension_table

load_city_temp_dimension_table >> immigration_city_temp_fact_table
load_immigration_dimension_table >> immigration_city_temp_fact_table
load_airport_code_dimension_table >> immigration_city_temp_fact_table
load_city_demographics_dimension_table >> immigration_city_temp_fact_table

immigration_city_temp_fact_table >> run_quality_checks

run_quality_checks >> end_operator
