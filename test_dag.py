from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_print_dag',
    default_args=default_args,
    description='A simple DAG that prints messages',
    catchup=False,
    tags=['sample', 'print', 'custom'],
)

# Python function to print a message
def print_hello():
    print("Hello from Airflow!")
    return "Hello task completed"

def print_world():
    print("World from Airflow!")
    return "World task completed"

def print_final():
    print("Final message from Airflow!")
    return "Final task completed"

# Define tasks
task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task2 = PythonOperator(
    task_id='print_world',
    python_callable=print_world,
    dag=dag,
)

task3 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task4 = PythonOperator(
    task_id='print_final',
    python_callable=print_final,
    dag=dag,
)

task5 = PythonOperator(
    task_id='print_world_second',
    python_callable=print_world,
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3 >> task4 >> task5
