from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define a function that will be executed by the PythonOperator
def hello_world():
    print("Hello, Candy.AI!")


def check_libraries():
    from google.cloud import bigquery
    from google.cloud import storage
    import sqlalchemy
    print(f"BigQuery Client: {bigquery.Client}")
    print(f"Google Cloud Storage Client: {storage.Client}")
    print(f"SQLAlchemy Version: {sqlalchemy.__version__}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'daily_candy_test',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='0 1 * * *',  # Run once a day at 1am
    start_date=datetime(2024, 8, 27),
    catchup=False,          
)

# Create the PythonOperator task
hello_world_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag,
)

check_reqs_task = PythonOperator(
    task_id='check_reqs_task',
    python_callable=check_libraries,
    dag=dag,
)

# Set the task to run
hello_world_task >> check_reqs_task
