CREATE TABLE IF NOT EXISTS fact_trades (
    trade_id TEXT PRIMARY KEY ,
    price FLOAT,
    quantity FLOAT,
    timestamp TIMESTAMP,
    is_buyer_maker BOOLEAN,
    symbol TEXT,
    total FLOAT
);



CREATE EXTERNAL TABLE IF NOT EXISTS fact_trades (
    trade_id STRING,
    price FLOAT,
    quantity FLOAT,
    `timestamp` TIMESTAMP, 
    is_buyer_maker BOOLEAN,
    symbol STRING,
    total FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/fact_trades';
