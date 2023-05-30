import sys
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import etl_functions

default_args = {
    'owner': 'wahba',
    'start_date': days_ago(0),
    'email': 'bedoa625@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the dag
dag = DAG(
    dag_id='redit_dag',
    schedule_interval='@hourly',
    default_args=default_args,
    description='Apache Airflow Final Assignment'
)

# define tasks
# first task is authenticating with redit api
authenticating = PythonOperator(
    task_id='auth',
    python_callable=etl_functions.autanticating,
    dag=dag
)

pull_posts = PythonOperator(
    task_id='pull',
    python_callable=etl_functions.pull_post,
    dag=dag
)

transform_to_csv = PythonOperator(
    task_id='transform',
    python_callable=etl_functions.transforming_to_csv,
    dag=dag
)

load_to_s3 = PythonOperator(
    task_id='load',
    python_callable=etl_functions.loading_data,
    dag=dag
)

authenticating >> pull_posts >> transform_to_csv >> load_to_s3
