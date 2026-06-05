from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = os.getenv('OPENSEARCH_PORT', '9200')

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("item_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType()),
    StructField("session_id", StringType())
])

def create_spark_session():
    return SparkSession.builder \
        .appName("ClickstreamOpenSearch") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.opensearch.client:opensearch-spark-30_2.12:1.1.0") \
        .config("opensearch.nodes", OPENSEARCH_HOST) \
        .config("opensearch.port", OPENSEARCH_PORT) \
        .config("opensearch.nodes.wan.only", "true") \
        .getOrCreate()

def write_to_opensearch(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("org.opensearch.spark.sql") \
            .option("opensearch.resource", "clickstream") \
            .option("opensearch.nodes", OPENSEARCH_HOST) \
            .option("opensearch.port", OPENSEARCH_PORT) \
            .option("opensearch.nodes.wan.only", "true") \
            .mode("append") \
            .save()

def process_stream(spark):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    events = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = events.writeStream \
        .foreachBatch(write_to_opensearch) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    process_stream(spark)
