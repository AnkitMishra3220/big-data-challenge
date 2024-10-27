import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

# Load configuration
with open('./config/config.json', 'r') as config_file:
    config = json.load(config_file)

spark = SparkSession.builder \
    .appName(config['app_name']) \
    .getOrCreate()

# Define schemas
campaign_schema = StructType([
    StructField("network_id", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("campaign_name", StringType(), True),
])

view_log_schema = StructType([
    StructField("view_id", IntegerType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("banner_id", IntegerType(), True),
    StructField("campaign_id", IntegerType(), True),
])

# Load static campaign data
campaign_df = spark.read.csv(config['campaign_data_path'], schema=campaign_schema, header=True)

# Read from Kafka
view_log_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers']) \
    .option("subscribe", config['kafka_topic_input']) \
    .option("startingOffsets", config['kafka_starting_offsets']) \
    .load()

# Parse JSON data and create view duration
view_log_df = view_log_df.select(from_json(col("value").cast("string"), view_log_schema).alias("data")) \
    .select("data.*") \
    .withColumn("view_duration", (col("end_timestamp").cast("long") - col("start_timestamp")
                                  .cast("long")).cast(DoubleType()))

# Join with static campaign data
joined_df = view_log_df.join(campaign_df, "campaign_id")

# Aggregation logic
agg_df = joined_df.withWatermark("start_timestamp", config['watermark_delay']) \
    .groupBy(
    window("start_timestamp", config['window_duration']).alias("minute_window"),
    "campaign_id",
    "network_id"
) \
    .agg(
    avg("view_duration").alias("avg_duration"),
    count("view_id").alias("total_count")
) \
    .select(
    col("campaign_id"),
    col("network_id"),
    col("minute_window.start").alias("minute_timestamp"),
    col("avg_duration"),
    col("total_count")
)


# Write to Parquet partitioned by network_id and minute_timestamp
query_1 = agg_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", config['output_path']) \
    .option("checkpointLocation", config['checkpoint_location']) \
    .partitionBy("network_id", "minute_timestamp") \
    .trigger(processingTime=config['trigger_processing_time']) \
    .start()

# write to console
query_2 = agg_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime=config['trigger_processing_time']) \
    .start()

# Write to Kafka
query_3 = agg_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers']) \
    .option("topic", config['kafka_topic_output']) \
    .option("checkpointLocation", config['checkpoint_location']) \
    .start()


spark.streams.awaitAnyTermination()

query_1.awaitTermination()
query_2.awaitTermination()
query_3.awaitTermination()