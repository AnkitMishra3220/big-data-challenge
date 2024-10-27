import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, from_json, window, avg, count
from main.campaigns_stream import campaign_schema, view_log_schema


class TestCampaignsStream(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestCampaignsStream").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema_definitions(self):
        # Test campaign_schema
        expected_campaign_schema = StructType([
            StructField("campaign", StringType(), True),
            StructField("network_id", IntegerType(), True),
            StructField("campaign_id", IntegerType(), True),
            StructField("campaign_name", StringType(), True),
        ])
        self.assertEqual(campaign_schema.jsonValue(), expected_campaign_schema.jsonValue())

        # Test view_log_schema
        expected_view_log_schema = StructType([
            StructField("view_id", StringType(), True),
            StructField("campaign_id", IntegerType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("end_timestamp", TimestampType(), True),
        ])
        self.assertEqual(view_log_schema.jsonValue(), expected_view_log_schema.jsonValue())

    def test_data_processing(self):
        # Create sample data
        view_log_data = [
            (
                '{"view_id": "1", "campaign_id": 1, "start_timestamp": "2023-05-01T10:00:00", "end_timestamp": '
                '"2023-05-01T10:01:30"}',),
            (
                '{"view_id": "2", "campaign_id": 2, "start_timestamp": "2023-05-01T10:02:00", "end_timestamp": '
                '"2023-05-01T10:03:00"}',),
        ]
        view_log_df = self.spark.createDataFrame(view_log_data, ["value"])

        # Apply transformations
        processed_df = view_log_df.select(from_json(col("value").cast("string"), view_log_schema).alias("data")) \
            .select("data.*") \
            .withColumn("view_duration",
                        (col("end_timestamp").cast("long") - col("start_timestamp").cast("long")).cast(DoubleType()))

        # Check results
        self.assertEqual(processed_df.count(), 2)
        self.assertTrue("view_duration" in processed_df.columns)

    def test_join_and_aggregation(self):
        # Create sample data
        campaign_data = [
            ("campaign1", 1, 1, "Campaign 1"),
            ("campaign2", 2, 2, "Campaign 2"),
        ]
        campaign_df = self.spark.createDataFrame(campaign_data, campaign_schema)

        view_log_data = [
            ("1", 1, "2023-05-01T10:00:00", "2023-05-01T10:01:30"),
            ("2", 1, "2023-05-01T10:02:00", "2023-05-01T10:03:00"),
            ("3", 2, "2023-05-01T10:01:00", "2023-05-01T10:02:30"),
        ]
        view_log_df = self.spark.createDataFrame(view_log_data, view_log_schema) \
            .withColumn("view_duration",
                        (col("end_timestamp").cast("long") - col("start_timestamp").cast("long")).cast(DoubleType()))

        # Join and aggregate
        joined_df = view_log_df.join(campaign_df, "campaign_id")
        agg_df = joined_df.groupBy("campaign_id", "network_id") \
            .agg(avg("view_duration").alias("avg_duration"), count("view_id").alias("total_count"))

        # Check results
        self.assertEqual(agg_df.count(), 2)
        self.assertTrue(
            all(col in agg_df.columns for col in ["campaign_id", "network_id", "avg_duration", "total_count"]))

    def test_window_operations(self):
        # Create sample data with timestamps
        view_log_data = [
            ("1", 1, "2023-05-01T10:00:00", "2023-05-01T10:01:30"),
            ("2", 1, "2023-05-01T10:02:00", "2023-05-01T10:03:00"),
            ("3", 2, "2023-05-01T10:01:00", "2023-05-01T10:02:30"),
            ("4", 2, "2023-05-01T10:05:00", "2023-05-01T10:06:00"),
        ]
        view_log_df = self.spark.createDataFrame(view_log_data, view_log_schema) \
            .withColumn("start_timestamp", col("start_timestamp").cast(TimestampType())) \
            .withColumn("end_timestamp", col("end_timestamp").cast(TimestampType())) \
            .withColumn("view_duration",
                        (col("end_timestamp").cast("long") - col("start_timestamp").cast("long")).cast(DoubleType()))

        # Apply window operation
        window_df = view_log_df.groupBy(
            window("start_timestamp", "5 minutes"),
            "campaign_id"
        ).agg(
            avg("view_duration").alias("avg_duration"),
            count("view_id").alias("total_count")
        )

        # Check results
        self.assertEqual(window_df.count(), 3)
        self.assertTrue(
            all(col in window_df.columns for col in ["window", "campaign_id", "avg_duration", "total_count"]))


if __name__ == '__main__':
    unittest.main()
