from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("item_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType()),
    StructField("session_id", StringType())
])

def create_spark_session():
    return SparkSession.builder \
        .appName("ClickstreamSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.opensearch.client:opensearch-spark-30_2.12:1.1.0") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_DEFAULT_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("opensearch.nodes", os.getenv("OPENSEARCH_HOST", "localhost")) \
        .config("opensearch.port", os.getenv("OPENSEARCH_PORT", "9200")) \
        .config("opensearch.nodes.wan.only", "true") \
        .getOrCreate()

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

    s3_path = S3_BUCKET.replace("s3://", "s3a://")
    query = events.writeStream \
        .format("parquet") \
        .option("path", s3_path) \
        .option("checkpointLocation", s3_path + "_checkpoint") \
        .partitionBy("event_type") \
        .trigger(processingTime="30 seconds") \
        .start()

    def write_to_opensearch(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write.format("org.opensearch.spark.sql").option("opensearch.resource", "clickstream").option("opensearch.nodes", os.getenv("OPENSEARCH_HOST", "localhost")).option("opensearch.port", os.getenv("OPENSEARCH_PORT", "9200")).option("opensearch.nodes.wan.only", "true").mode("append").save()

    os_query = events.writeStream.foreachBatch(write_to_opensearch).option("checkpointLocation", s3_path + "_os_checkpoint").trigger(processingTime="5 seconds").start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    process_stream(spark)
