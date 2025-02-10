CREATE TABLE IF NOT EXISTS staging_transactions (
    trade_id BIGINT PRIMARY KEY,
    symbol TEXT,
    price FLOAT,
    quantity FLOAT,
    time TIMESTAMP,
    is_buyer_maker BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_trades (
    trade_id BIGINT PRIMARY KEY,
    symbol TEXT,
    price FLOAT,
    quantity FLOAT,
    total_value FLOAT,
    time TIMESTAMP,
    is_buyer_maker BOOLEAN
);
