import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import pandas as pd
import requests
import json
from kafka import KafkaProducer
import time
import logging

# Class PostgresOperators (đã được cung cấp trước đó)
class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def get_connection(self):
        return self.hook.get_conn()

    def get_data_to_pd(self, sql):
        return self.hook.get_pandas_df(sql)

    def save_data_to_postgres(self, df, table_name, schema='public', if_exists='replace'):
        conn = self.hook.get_uri()
        engine = create_engine(conn)
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)

    def execute_query(self, sql):
        self.hook.run(sql)

# Default arguments cho DAG
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def create_schema_if_not_exists():
    # Tạo đối tượng PostgresOperators với conn_id đã được định nghĩa trong Airflow Connections
    postgres_operator = PostgresOperators(conn_id="postgres")
    
    # Câu lệnh SQL để tạo schema nếu chưa tồn tại
    create_schema_query = """
    CREATE SCHEMA IF NOT EXISTS warehouse;
    """
    
    # Thực thi câu lệnh SQL
    postgres_operator.execute_query(create_schema_query)

# Hàm gọi API Binance
def get_binance_trades(symbol="BTCUSDT"):
    # Gọi API Binance để lấy danh sách giao dịch
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    res = requests.get(url)
    res.raise_for_status()  # Kiểm tra lỗi HTTP
    trades = res.json()
    return trades

# Hàm định dạng dữ liệu từ API Binance
def format_binance_trades(trades):
    formatted_trades = []

    for trade in trades:
        data = {
            "id": str(uuid.uuid4()),  # Tạo UUID ngẫu nhiên
            "trade_id": trade['id'],  # ID giao dịch từ Binance
            "price": float(trade['price']),  # Giá giao dịch
            "quantity": float(trade['qty']),  # Số lượng giao dịch
            "timestamp": datetime.fromtimestamp(trade['time'] / 1000).isoformat(),  # Thời gian giao dịch
            "is_buyer_maker": trade['isBuyerMaker'],  # Người mua là maker hay không
            "symbol": "BTCUSDT"  # Cặp giao dịch
        }
        formatted_trades.append(data)

    return formatted_trades

# Hàm lưu dữ liệu vào PostgreSQL sử dụng PostgresOperators
def save_to_postgres(trades):
    # Chuyển đổi danh sách giao dịch thành Pandas DataFrame
    df = pd.DataFrame(trades)

    # Tạo đối tượng PostgresOperators với conn_id (được định nghĩa trong Airflow Connections)
    postgres_operator = PostgresOperators(conn_id="postgres")

    # Lưu dữ liệu vào bảng PostgreSQL
    postgres_operator.save_data_to_postgres(
        df=df,
        table_name="binance_trades",  # Tên bảng
        schema="warehouse",  # Schema mặc định
        if_exists="append"  # Thêm dữ liệu vào bảng nếu đã tồn tại
    )

# Hàm stream dữ liệu đến Kafka
def stream_binance_trades():
    # Kết nối đến Kafka broker
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Chạy trong 1 phút
            break
        try:
            # Gọi API Binance và định dạng dữ liệu
            trades = get_binance_trades(symbol="BTCUSDT")  # Bạn có thể thay đổi cặp giao dịch tại đây
            formatted_trades = format_binance_trades(trades)

            # Lưu dữ liệu vào PostgreSQL
            save_to_postgres(formatted_trades)

            # Gửi từng giao dịch đến Kafka topic
            for trade in formatted_trades:
                producer.send('binance_trades', json.dumps(trade).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

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

    # Đảm bảo task tạo schema chạy trước task stream dữ liệu
    create_schema_task >> streaming_task