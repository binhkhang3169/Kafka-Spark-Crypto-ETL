import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
from kafka import KafkaProducer

# Class PostgresOperators (đã được cung cấp trước đó)
class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def save_data_to_postgres(self, df, table_name, schema='public', if_exists='replace'):
        conn = self.hook.get_uri()
        engine = create_engine(conn)
        df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)

# Hàm gọi API Binance
def get_binance_trades(symbol="BTCUSDT"):
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

# Hàm định dạng dữ liệu từ API Binance
def format_binance_trades(trades):
    formatted_trades = []
    for trade in trades:
        data = {
            "trade_id": trade['id'],
            "price": float(trade['price']),
            "quantity": float(trade['qty']),
            "timestamp": datetime.fromtimestamp(trade['time'] / 1000).isoformat(),
            "is_buyer_maker": trade['isBuyerMaker'],
            "symbol": "BTCUSDT"
        }
        formatted_trades.append(data)
    return formatted_trades

# Hàm lưu dữ liệu vào PostgreSQL
def save_to_postgres(trades):
    df = pd.DataFrame(trades)
    postgres_operator = PostgresOperators(conn_id="postgres")
    postgres_operator.save_data_to_postgres(df, "binance_trades", schema="warehouse", if_exists="append")

# Hàm stream dữ liệu đến Kafka
def stream_binance_trades():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while time.time() <= curr_time + 60:
        try:
            trades = get_binance_trades(symbol="BTCUSDT")
            formatted_trades = format_binance_trades(trades)
            save_to_postgres(formatted_trades)

            for trade in formatted_trades:
                producer.send('binance_trades5', json.dumps(trade).encode('utf-8'))
                time.sleep(1)
        except Exception as e:
            logging.error(f'An error occurred: {e}')
