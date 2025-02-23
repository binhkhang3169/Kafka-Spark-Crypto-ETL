from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from create_schema_task import create_schema_if_not_exists
from streaming_task import stream_binance_trades

# Default arguments cho DAG
default_args = {
    'owner': 'binhkhang3169',
    'start_date': datetime(2025, 2, 3, 10, 00)
}

# Định nghĩa DAG
with DAG('binance_trade_streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    create_schema_task = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema_if_not_exists
    )

    streaming_task = PythonOperator(
        task_id='stream_binance_trades',
        python_callable=stream_binance_trades
    )

    create_schema_task >> streaming_task
