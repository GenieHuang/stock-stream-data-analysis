from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

from dotenv import load_dotenv
import os
import logging
from datetime import datetime

class SparkStream:

    stock_schema = StructType() \
    .add("timestamp", LongType()) \
    .add("opening", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("closing", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("trade_count", IntegerType()) \
    .add("vw_avg_price", DoubleType())

    symbol_schema = StructType() \
    .add("stock_symbol", StringType())

    def __init__(self, kafka_server, topic, access_key, secret_key):
        self.kafka_server = kafka_server
        self.topic = topic
        self.access_key = access_key
        self.secret_key = secret_key

        # Create Spark session
        self.create_spark_connection()
    

    def create_spark_connection(self):
        try:
            self.spark = SparkSession.builder \
            .appName("SparkKafkaStockStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.5.3,"
                                            "org.apache.hadoop:hadoop-aws:3.3.4,"
                                            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.jars", "pyspark_data_transformation/jar_files/spark-sql-kafka-0-10_2.12-3.4.4.jar,"
                                            "pyspark_data_transformation/jar_files/commons-pool2-2.11.1.jar,"
                                            "pyspark_data_transformation/jar_files/spark-token-provider-kafka-0-10_2.12-3.4.4.jar,"
                                            "pyspark_data_transformation/jar_files/kafka-clients-3.3.2.jar") \
            .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
            self.spark.sparkContext.setLogLevel("ERROR")
            logging.info("Spark session created successfully.")

        except Exception as e:
            logging.error(f"Error creating Spark session: {e}")
            raise e
        

    def connect_to_kafka(self):
        try:
            self.spark_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_server) \
                .option("subscribe", self.topic) \
                .option("failOnDataLoss", "false") \
                .option("startingOffsets", "earliest") \
                .load()
            
            logging.info("kafka dataframes created successfully.")

        except Exception as e:
            logging.error(f"Error connecting to Kafka: {e}")
            raise e
    

    def transform_data(self):
        self.transformed_df = self.spark_df.selectExpr("CAST(value AS STRING)", "CAST(key AS STRING)") \
                                    .select(from_json(col("key"), SparkStream.symbol_schema).alias("symbol"),
                                            from_json(col("value"), SparkStream.stock_schema).alias("stock_data")) \
                                    .select("stock_data.*", "symbol.*")


    def initiate_streaming_to_bucket(self, path):

        logger.info("Initiating streaming process...")
        stream_query = self.transformed_df \
                        .writeStream \
                        .format("parquet") \
                        .outputMode("append") \
                        .option("path", path) \
                        .option("checkpointLocation", '/tmp/checkpoint') \
                        .start()
        stream_query.awaitTermination()

    def start_streaming(self, path):
        if self.spark is not None:

            # Connect to Kafka with Spark
            self.connect_to_kafka()

            # Transform data
            self.transform_data()

            # Write to S3
            self.initiate_streaming_to_bucket(path)
            


if __name__ == "__main__":
    
    load_dotenv()

    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    topic_prefix = os.getenv("KAFKA_TOPIC")

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    path = os.getenv("AWS_S3_BUCKET_PATH")

    today = datetime.today().strftime("%Y-%m-%d")

    topic = f"""{topic_prefix}{today}"""
    # path = f"""{bucket_path}{today}"""

    # Initialize logging
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger("spark_structured_streaming")

    # Create Spark connection
    spark_stream = SparkStream("localhost:9092", topic, access_key, secret_key)

    # Start the streaming process
    spark_stream.start_streaming(path)
