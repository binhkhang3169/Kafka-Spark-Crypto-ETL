from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, TimestampType
from pyspark.sql.functions import expr, to_timestamp, from_json
import time

# Khởi tạo Spark Session
conf = SparkConf() \
    .setAppName("KafkaToPostgres") \
    .setMaster("local[2]") \
    .set("spark.executor.memory", "4g")

spark = SparkSession.builder \
    .config(conf=conf) \
    .config("spark.jars", "/opt/bitnami/spark/postgresql-42.7.5.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

kafka_topic_input = "binance_trades5"

# Định nghĩa schema cho dữ liệu từ Kafka
schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("symbol", StringType(), True)
])

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", kafka_topic_input) \
    .option("startingOffsets", "latest") \
    .load()

# Chuyển đổi dữ liệu từ binary sang JSON string
df_str = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse JSON thành DataFrame có schema
df_parsed = df_str.select(from_json("value", schema).alias("data")).select("data.*")

# Chuyển đổi timestamp từ String sang TimestampType
df_formatted = df_parsed.withColumn("timestamp", to_timestamp(df_parsed["timestamp"], "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))

# Thêm cột tổng giá trị giao dịch
df_final = df_formatted.withColumn("total", expr("quantity * price"))

# Hàm ghi vào PostgreSQL
def write_to_postgres(batch_df, batch_id):
    print(f"[INFO] Ghi batch {batch_id} vào PostgreSQL - {batch_df.count()} bản ghi.")
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "fact_trades") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

print("[INFO] Streaming bắt đầu...")

# Chạy Spark Streaming
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Chạy trong 1 phút
time.sleep(60)

# Dừng query và Spark
query.stop()
spark.stop()

print("[INFO] Dữ liệu từ Kafka đã được ghi vào PostgreSQL. Spark đã dừng.") 
