from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, LongType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("BinanceDataCleaning") \
    .config("spark.jars", "/home/jovyan/work/postgresql-42.7.5.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "binance_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Schema dữ liệu
schema = StructType([
    StructField("trade_id", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", FloatType(), True),
    StructField("time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True)
])

# Chuyển đổi dữ liệu JSON
df_parsed = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Định dạng cột timestamp
df_transformed = df_parsed.withColumn("time", expr("cast(time/1000 as timestamp)"))

# Ghi vào bảng staging_transactions (Data Warehouse - PostgreSQL)
def write_to_postgres(df, epoch_id):
    if df.count() == 0:
        return  # 🔥 Tránh lỗi batch trống
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
        .option("dbtable", "staging_transactions") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

df_transformed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

# Đọc dữ liệu từ staging_transactions
staging_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
    .option("dbtable", "staging_transactions") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Tính toán và ghi vào fact_trades
fact_df = staging_df.withColumn("total_value", col("price") * col("quantity"))

fact_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
    .option("dbtable", "fact_trades") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Tạo và ghi vào các bảng dim
dim_symbols = staging_df.select("symbol").distinct()
dim_symbols.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
    .option("dbtable", "dim_symbols") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

dim_time = staging_df.select("time").distinct()
dim_time.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
    .option("dbtable", "dim_time") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()