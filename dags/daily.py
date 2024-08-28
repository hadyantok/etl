from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

# Define a function that will be executed by the PythonOperator
def hello_candy():
    print("Hello, Candy.AI!")

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
    description='A simple DAG',
    schedule_interval='5 1 * * *',  # Run once a day at 1am
    start_date=datetime(2024, 8, 26),
    catchup=True,          
)

# Create the PythonOperator task
hello_candy = PythonOperator(
    task_id='hello_candy',
    python_callable=hello_candy,
    dag=dag,
)

conversations_dummy_update = DockerOperator(
        task_id='conversations_dummy_update',
        image='task_runner',
        api_version='auto',
        auto_remove=True,
        working_dir='/home/apps/app',
        mounts=[Mount(target="/home/apps",source="/home/hady/etl",type="bind"),],
        network_mode='bridge',
        command="python3 -m conversations.dummy_update "
    )

conversations_to_gcs = DockerOperator(
        task_id='conversations_to_gcs',
        image='task_runner',
        api_version='auto',
        auto_remove=True,
        working_dir='/home/apps/app',
        mounts=[Mount(target="/home/apps",source="/home/hady/etl",type="bind"),],
        network_mode='bridge',
        command="python3 -m conversations.to_gcs "
    )

conversations_to_bq = DockerOperator(
        task_id='conversations_to_bq',
        image='task_runner',
        api_version='auto',
        auto_remove=True,
        working_dir='/home/apps/app',
        mounts=[Mount(target="/home/apps",source="/home/hady/etl",type="bind"),],
        network_mode='bridge',
        command="python3 -m conversations.to_bq "
    )

# Set the task to run
hello_candy >> conversations_dummy_update >> conversations_to_gcs >> conversations_to_bq
