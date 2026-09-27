import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()

class TestDataTransformerDropDuplicates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session that will be used across all tests."""
        cls.spark = BaseDataTransformer._get_spark()

    def setUp(self):
        """Creates a reusable DataFrame with duplicate rows."""
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        data = [
            ("   order_1   ", "customer_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0)),
            ("ORDER_2", "customer_2", "product_2", 15, 30.0, datetime(2024, 2, 24, 14, 45, 0)),
            ("    order_3", "customer_3", "product_3", 20, 40.0, datetime(2024, 2, 25, 16, 50, 0)),
            ("order_4", "customer_4", "product_4", 25, 50.0, datetime(2024, 2, 26, 18, 55, 0)),
            ("order_5", "customer_5", "product_5", 30, 60.0, datetime(2024, 2, 27, 20, 45, 0))
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_columns_to_select(self):
        """Tests for columns to select ."""
        transformer = BaseDataTransformer("tests/configs/transformers/filters_columns_to_select_config.yaml")
        transformation_id = "transformation_id_1"
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("Transformed DataFrame:")
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
        # Check if the DataFrame has duplicates removed based on specified columns
        expected_data = [
            ("order_1", "customer_1"),
            ("order_2", "customer_2"),
            ("order_3", "customer_3"),
            ("order_4", "customer_4"),
            ("order_5", "customer_5")
        ]
        expected_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        
        # Assert the row counts match
        assert transformed_df.count() == expected_df.count(), "Row counts do not match"

        # Assert that the DataFrame contains the expected columns
        assert "order_id" in transformed_df.columns, "Column 'order_id' is missing"
        assert "customer_id" in transformed_df.columns, "Column 'customer_id' is missing"

        # Assert the row count matches the expected number of rows
        assert transformed_df.count() == 5, f"Expected 5 rows, but got {transformed_df.count()}"


    def test_order_id_formatting_test(self):
        """Tests for columns to select ."""
        transformer = BaseDataTransformer("tests/configs/transformers/filters_columns_to_select_config.yaml")
        transformation_id = "transformation_id_2"
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("Transformed DataFrame:")
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
        # Check if the DataFrame has duplicates removed based on specified columns
        expected_data = [
            ("order_1", "customer_1"),
            ("order_2", "customer_2"),
            ("order_3", "customer_3"),
            ("order_4", "customer_4"),
            ("order_5", "customer_5")
        ]
        expected_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        
        # Assert the row counts match
        assert transformed_df.count() == expected_df.count(), "Row counts do not match"

        # Collect the transformed data to verify the customer_id has no spaces and all data is lowercase
        transformed_data = transformed_df.collect()
        for row in transformed_data:
            assert row["order_id"] == row["order_id"].strip().lower(), f"Order ID '{row['order_id']}' is not properly formatted"

if __name__ == "__main__":
    unittest.main()