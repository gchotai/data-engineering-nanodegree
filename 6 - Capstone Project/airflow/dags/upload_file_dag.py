import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator

default_args = {
    'owner': 'gunja',
    'start_date': datetime(2020, 9, 24),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False

}

dag = DAG('CSV_to_Json', 
          default_args=default_args,          
          schedule_interval=None
)



t1 = BashOperator(
           task_id='testairflow',
           dag=dag,
           bash_command='python /home/workspace/scripts/csv_to_json.py'
           )