from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

# Define a function that will be executed by the PythonOperator
def hello_world():
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
hello_world_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag,
)

test_docker_operator = DockerOperator(
        task_id='test_docker_operator',
        image='hello-world',
        api_version='auto',
        auto_remove=True,  # Automatically remove the container once it's done
        network_mode='bridge',
    )

# Set the task to run
hello_world_task >> test_docker_operator
