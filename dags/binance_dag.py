from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def run_websocket_to_kafka():
    os.system("python /usr/local/airflow/scripts/websocket_to_kafka.py")

def run_spark_processing():
    os.system("python /usr/local/airflow/scripts/spark_processing.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'binance_data_pipeline',
    default_args=default_args,
    description='A pipeline to collect, process, and store Binance data',
    schedule_interval=timedelta(days=1),
)

websocket_to_kafka_task = PythonOperator(
    task_id='websocket_to_kafka',
    python_callable=run_websocket_to_kafka,
    dag=dag,
)

spark_processing_task = PythonOperator(
    task_id='spark_processing',
    python_callable=run_spark_processing,
    dag=dag,
)

websocket_to_kafka_task >> spark_processing_task