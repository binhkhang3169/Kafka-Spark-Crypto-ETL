from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("BinanceDataProcessing") \
    .config("spark.jars", "/home/jovyan/work/postgresql-42.7.5.jar") \
    .getOrCreate()

# Cấu hình kết nối PostgreSQL
DB_URL = "jdbc:postgresql://postgres:5432/crypto_db"
DB_USER = "postgres"
DB_PASSWORD = "password"

try:
    # Đọc dữ liệu từ staging_transactions
    staging_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "staging_transactions") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load()

    # Tạo bảng fact_trades với cột total_value
    fact_df = staging_df.withColumn("total_value", col("price") * col("quantity"))

    # Ghi dữ liệu vào bảng fact_trades
    fact_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "fact_trades") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .mode("append") \
        .save()

    print("✅ Dữ liệu đã được xử lý và ghi vào bảng fact_trades.")

except Exception as e:
    print(f"❌ Lỗi khi xử lý dữ liệu: {e}")

finally:
    spark.stop()  # Đóng SparkSession
