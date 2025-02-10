import requests
import pandas as pd
import psycopg2
from datetime import datetime

# Kết nối PostgreSQL (Docker)
conn = psycopg2.connect(
    dbname="crypto_db",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# API Binance - Lấy 500 giao dịch gần nhất của BTC/USDT
url = "https://api.binance.com/api/v3/trades"
params = {"symbol": "BTCUSDT", "limit": 500}
response = requests.get(url, params=params)
data = response.json()

# Chuyển đổi dữ liệu thành DataFrame
df = pd.DataFrame(data)
df["time"] = pd.to_datetime(df["time"], unit="ms")
df["total_value"] = df["price"].astype(float) * df["qty"].astype(float)

# Lưu dữ liệu vào PostgreSQL
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_transactions (
        trade_id BIGINT PRIMARY KEY,
        symbol TEXT,
        price FLOAT,
        quantity FLOAT,
        time TIMESTAMP,
        is_buyer_maker BOOLEAN,
        total_value FLOAT
    )
""")

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO raw_transactions (trade_id, symbol, price, quantity, time, is_buyer_maker, total_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
    """, (row["id"], "BTCUSDT", row["price"], row["qty"], row["time"], row["isBuyerMaker"], row["total_value"]))

conn.commit()
cursor.close()
conn.close()

print("Dữ liệu được lưu thành công vào PostgreSQL!")
