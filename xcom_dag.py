from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="example_xcom",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "xcom"],
) as dag:

    def produce(**context):
        value = "hello-xcom"
        # Explicit push
        context["ti"].xcom_push(key="greeting", value=value)
        # Returning also pushes under key "return_value"
        return value

    def consume(**context):
        ti = context["ti"]
        greeting = ti.xcom_pull(task_ids="produce_task", key="greeting")
        return f"got: {greeting}"

    produce_task = PythonOperator(
        task_id="produce_task",
        python_callable=produce,
    )

    consume_task = PythonOperator(
        task_id="consume_task",
        python_callable=consume,
    )

    produce_task >> consume_task
