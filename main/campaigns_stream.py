import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, expr, broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Load configuration
with open('./config/config.json', 'r') as config_file:
    config = json.load(config_file)

# Create SparkSession with optimized configuration
spark = SparkSession.builder \
    .appName(config['app_name']) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# Define schemas
campaign_schema = StructType([
    StructField("network_id", StringType(), True),
    StructField("campaign_id", IntegerType(), False),
    StructField("campaign_name", StringType(), True),
])

view_log_schema = StructType([
    StructField("view_id", IntegerType(), False),
    StructField("start_timestamp", TimestampType(), False),
    StructField("end_timestamp", TimestampType(), False),
    StructField("banner_id", IntegerType(), True),
    StructField("campaign_id", IntegerType(), False),
])

# Load static campaign data
campaign_df = spark.read.csv(config['campaign_data_path'], schema=campaign_schema, header=True)
campaign_df.cache()  # Cache the static data

# Read from Kafka
view_log_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers']) \
    .option("subscribe", config['kafka_topic_input']) \
    .option("startingOffsets", config['kafka_starting_offsets']) \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data and create view duration
parsed_df = view_log_df.select(from_json(col("value").cast("string"), view_log_schema).alias("data")) \
    .select("data.*") \
    .withColumn("view_duration", expr("cast((end_timestamp - start_timestamp) as double)"))

# Join with static campaign data
joined_df = parsed_df.join(broadcast(campaign_df), "campaign_id")

# Aggregation logic
agg_df = joined_df \
    .withWatermark("start_timestamp", config['watermark_delay']) \
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
parquet_query = agg_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", config['output_path']) \
    .option("checkpointLocation", f"{config['checkpoint_location']}/parquet") \
    .partitionBy("network_id", "minute_timestamp") \
    .trigger(processingTime=config['trigger_processing_time']) \
    .start()

# Write to Kafka
agg_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka_bootstrap_servers']) \
    .option("topic", config['kafka_topic_output']) \
    .option("checkpointLocation", f"{config['checkpoint_location']}/kafka") \
    .start()

spark.streams.awaitAnyTermination()
