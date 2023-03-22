import os
import unittest
from pyspark.sql import SparkSession
from extract_data.extract import logging
from transform_data.transform import read_data, transform_dataset, aggregated_dataset, sink_dataset

class TestWeatherDataPipeline(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.file_path = "test_data/weather_data.csv"

    def tearDown(self):
        self.spark.stop()

    def test_read_data(self):
        expected_count = 14
        df = read_data(self.file_path)
        actual_count = df.count()
        self.assertEqual(actual_count, expected_count)

    def test_transform_dataset(self):
        expected_count = 14
        df = read_data(self.file_path)
        transformed_df = transform_dataset(df)
        actual_count = transformed_df.count()
        self.assertEqual(actual_count, expected_count)

    def test_aggregated_dataset(self):
        expected_count = 4
        df = read_data(self.file_path)
        transformed_df = transform_dataset(df)
        aggregated_df = aggregated_dataset(transformed_df)
        actual_count = aggregated_df.count()
        self.assertEqual(actual_count, expected_count)

    def test_sink_dataset(self):
        expected_path = "test_data/output"
        expected_file_name = "output.csv"
        df = read_data(self.file_path)
        transformed_df = transform_dataset(df)
        aggregated_df = aggregated_dataset(transformed_df)
        sink_dataset(aggregated_df, expected_file_name, expected_path)
        self.assertTrue(os.path.exists(os.path.join(expected_path, expected_file_name)))


if __name__ == '__main__':
    unittest.main()
