from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os
import sys

args = dict(a.split('=', 1) for a in sys.argv[1:] if '=' in a)

KAFKA_BROKER = args.get('KAFKA_BROKER') or os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = args.get('KAFKA_TOPIC') or os.getenv('KAFKA_TOPIC')
S3_BUCKET = args.get('S3_BUCKET') or os.getenv('S3_BUCKET')
AWS_DEFAULT_REGION = args.get('AWS_DEFAULT_REGION') or os.getenv('AWS_DEFAULT_REGION')
OPENSEARCH_HOST = args.get('OPENSEARCH_HOST') or os.getenv('OPENSEARCH_HOST')
OPENSEARCH_PORT = args.get('OPENSEARCH_PORT') or os.getenv('OPENSEARCH_PORT')
OPENSEARCH_USER = args.get('OPENSEARCH_USER') or os.getenv('OPENSEARCH_USER')
OPENSEARCH_PASSWORD = args.get('OPENSEARCH_PASSWORD') or os.getenv('OPENSEARCH_PASSWORD')

if not all([KAFKA_BROKER, KAFKA_TOPIC, S3_BUCKET, OPENSEARCH_HOST, OPENSEARCH_PORT]):
    raise EnvironmentError("Missing required env vars")

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
        .config("spark.hadoop.fs.s3a.bucket.clickstream-pipeline.endpoint.region", AWS_DEFAULT_REGION) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_DEFAULT_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("opensearch.nodes", OPENSEARCH_HOST) \
        .config("opensearch.port", OPENSEARCH_PORT) \
        .config("opensearch.nodes.wan.only", "true") \
        .config("opensearch.net.ssl", "true") \
        .config("opensearch.net.ssl.cert.allow.self.signed", "true") \
        .config("opensearch.net.http.auth.user", OPENSEARCH_USER) \
        .config("opensearch.net.http.auth.pass", OPENSEARCH_PASSWORD) \
        .getOrCreate()

def write_to_opensearch(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("org.opensearch.spark.sql") \
            .option("opensearch.resource", "clickstream") \
            .option("opensearch.nodes", OPENSEARCH_HOST) \
            .option("opensearch.port", OPENSEARCH_PORT) \
            .option("opensearch.nodes.wan.only", "true") \
            .option("opensearch.net.ssl", "true") \
            .option("opensearch.net.ssl.cert.allow.self.signed", "true") \
            .option("opensearch.security.ssl.certificate.verification", "false") \
            .option("opensearch.net.http.auth.user", OPENSEARCH_USER) \
            .option("opensearch.net.http.auth.pass", OPENSEARCH_PASSWORD) \
            .mode("append") \
            .save()

def process_stream(spark):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", "SSL") \
        .option("kafka.ssl.truststore.type", "JKS") \
        .option("kafka.ssl.endpoint.identification.algorithm", "") \
        .load()

    events = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    s3_path = S3_BUCKET.replace("s3://", "s3a://")

    s3_query = events.writeStream \
        .format("parquet") \
        .option("path", s3_path) \
        .option("checkpointLocation", s3_path + "_checkpoint") \
        .partitionBy("event_type") \
        .trigger(processingTime="30 seconds") \
        .start()

    os_query = events.writeStream \
        .foreachBatch(write_to_opensearch) \
        .option("checkpointLocation", s3_path + "_os_checkpoint") \
        .trigger(processingTime="5 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    process_stream(spark)
