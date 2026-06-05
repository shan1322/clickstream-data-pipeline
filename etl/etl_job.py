from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, unix_timestamp, when, sum as spark_sum,
    count, min as spark_min, max as spark_max,
    concat_ws, lit
)
from pyspark.sql.window import Window
import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET')
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DB = os.getenv('REDSHIFT_DB')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

REDSHIFT_URL = f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"

def create_spark_session():
    return SparkSession.builder \
        .appName("ClickstreamETL") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-client:3.3.4,"
                "com.amazon.redshift:redshift-jdbc42:2.1.0.9") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def sessionize(df):
    window = Window.partitionBy("user_id").orderBy("timestamp")
    
    df = df.withColumn("prev_timestamp", lag("timestamp", 1).over(window))
    df = df.withColumn("time_diff",
        (col("timestamp") - col("prev_timestamp")) / 1000
    )
    df = df.withColumn("new_session",
        when(col("time_diff") > 1800, 1)
        .when(col("prev_timestamp").isNull(), 1)
        .otherwise(0)
    )
    session_window = Window.partitionBy("user_id").orderBy("timestamp")
    df = df.withColumn("session_number",
        spark_sum("new_session").over(session_window)
    )
    df = df.withColumn("session_id",
        concat_ws("_", col("user_id"), col("session_number"))
    )
    return df

def calculate_engagement(df):
    session_window = Window.partitionBy("user_id", "session_id")
    
    df = df.withColumn("view_flag", when(col("event_type") == "view", 1).otherwise(0))
    df = df.withColumn("cart_flag", when(col("event_type") == "addtocart", 1).otherwise(0))
    df = df.withColumn("purchase_flag", when(col("event_type") == "transaction", 1).otherwise(0))
    
    sessions = df.groupBy("user_id", "session_id").agg(
        spark_min("timestamp").alias("session_start"),
        spark_max("timestamp").alias("session_end"),
        count("*").alias("total_events"),
        spark_sum("view_flag").alias("view_count"),
        spark_sum("cart_flag").alias("addtocart_count"),
        spark_sum("purchase_flag").alias("transaction_count")
    )
    
    sessions = sessions.withColumn("session_duration_seconds",
        (col("session_end") - col("session_start")) / 1000
    )
    sessions = sessions.withColumn("engagement_score",
        col("view_count") * 1.0 +
        col("addtocart_count") * 3.0 +
        col("transaction_count") * 5.0
    )
    return sessions

def write_to_redshift(df):
    df.write \
        .format("jdbc") \
        .option("url", REDSHIFT_URL) \
        .option("dbtable", "clickstream_sessions") \
        .option("user", REDSHIFT_USER) \
        .option("password", REDSHIFT_PASSWORD) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()

def run_etl():
    spark = create_spark_session()
    
    s3_path = S3_BUCKET.replace("s3://", "s3a://")
    df = spark.read.parquet(s3_path)
    
    print(f"Read {df.count()} raw events from S3")
    
    df = sessionize(df)
    sessions = calculate_engagement(df)
    
    print(f"Generated {sessions.count()} sessions")
    
    write_to_redshift(sessions)
    print("Written to Redshift successfully")
    
    spark.stop()

if __name__ == "__main__":
    run_etl()
