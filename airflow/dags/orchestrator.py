import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta


sys.path.append("/opt/airflow")
from api_request.insert_records import main


def safe_main_callable():
       return main()


default_args = {
    "description":"A DAG to orchestrate data",
    "start_date":datetime(2025, 9, 3),
    "catchup": False,
}


 
dag = DAG(
    dag_id="weather-api-orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=1)
)

with dag:
       task1 = PythonOperator(
            task_id = "ingest_data_task",
            python_callable = main,
    )