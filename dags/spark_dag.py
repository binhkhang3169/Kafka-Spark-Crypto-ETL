import uuid
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, BooleanType

# Schema for the Kafka message
schema = StructType([
    StructField("id", StringType(), False),
    StructField("trade_id", StringType(), False),
    StructField("price", FloatType(), False),
    StructField("quantity", FloatType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("is_buyer_maker", BooleanType(), False),
    StructField("symbol", StringType(), False)
])

# Create selection df
def create_selection_df_from_kafka(spark_df):
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

# Create connect kafka
def connect_to_kafka(spark_conn):
    spark_df = None
    
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'binance_trades') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df

# Create connect spark
def create_spark_connection():
    s_conn = None
    
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,org.postgresql:postgresql:42.7.5") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# Process batch data
def process_batch(df, epoch_id):
    # Check for null values and filter out rows with null values
    df = df.filter(
        col("id").isNotNull() &
        col("trade_id").isNotNull() &
        col("price").isNotNull() &
        col("quantity").isNotNull() &
        col("timestamp").isNotNull() &
        col("is_buyer_maker").isNotNull() &
        col("symbol").isNotNull()
    )

    # Create total column by multiplying quantity with price
    df = df.withColumn("total", col("quantity") * col("price"))

    # Save data to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("dbtable", "staging_transactions") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Main function to stream data from Kafka and process it
def stream_spark():
    spark_conn = create_spark_connection()
    
    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        
        # Start streaming and process each batch
        query = selection_df.writeStream \
            .foreachBatch(process_batch) \
            .start()
        
        query.awaitTermination()

# Run the main function
if __name__ == "__main__":
    stream_spark()