import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from wilsonelser.transformation.engine import TransformationEngine
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()

class TestDataTransformerDropDuplicates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session that will be used across all tests."""
        cls.spark = TransformationEngine._get_spark()

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
            ("order_1", "customer_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0)),
            ("order_1", "customer_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0)),  # Duplicate row
            ("order_2", "customer_2", "product_2", 15, 30.0, datetime(2024, 2, 24, 14, 45, 0)),
            ("order_2", "customer_2", "product_2", 15, 30.0, datetime(2024, 2, 24, 14, 45, 0)),  # Duplicate row
            ("order_3", "customer_3", "product_3", 20, 40.0, datetime(2024, 2, 25, 16, 50, 0)),
            ("order_3", "customer_3", "product_3", 20, 40.0, datetime(2024, 2, 25, 16, 50, 0)),  # Duplicate row
            ("order_4", "customer_4", "product_4", 25, 50.0, datetime(2024, 2, 26, 18, 55, 0)),
            ("order_4", "customer_4", "product_4", 25, 50.0, datetime(2024, 2, 26, 18, 55, 0)),  # Duplicate row
            ("order_5", "customer_5", "product_5", 30, 60.0, datetime(2024, 2, 27, 20, 45, 0)),
            ("order_5", "customer_5", "product_5", 30, 60.0, datetime(2024, 2, 27, 20, 45, 0))   # Duplicate row
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_drop_duplicates_for_select_cols(self):
        """Tests drop duplicates logic on the DataFrame for select columns."""
        transformer = TransformationEngine("tests/configs/transformers/filters_drop_duplicates_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
        # Check if the DataFrame has duplicates removed based on specified columns
        expected_data = [
            ("order_1", "customer_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0)),
            ("order_2", "customer_2", "product_2", 15, 30.0, datetime(2024, 2, 24, 14, 45, 0)),
            ("order_3", "customer_3", "product_3", 20, 40.0, datetime(2024, 2, 25, 16, 50, 0)),
            ("order_4", "customer_4", "product_4", 25, 50.0, datetime(2024, 2, 26, 18, 55, 0)),
            ("order_5", "customer_5", "product_5", 30, 60.0, datetime(2024, 2, 27, 20, 45, 0))
        ]
        expected_df = self.spark.createDataFrame(expected_data, self.test_dataframe.schema)
        
        self.assertEqual(transformed_df.count(), expected_df.count(), "Assertion failed: The count after dropping duplicates does not match the expected count.")
        
        # Assert if customer_1 to customer_5 exist in the transformed DataFrame
        customers = [row['customer_id'] for row in transformed_df.collect()]
        for customer_id in ["customer_1", "customer_2", "customer_3", "customer_4", "customer_5"]:
            self.assertIn(customer_id, customers, f"Assertion failed: {customer_id} is not present in the transformed DataFrame.")
            
    def test_apply_drop_duplicates(self):
        """Tests drop duplicates logic on the DataFrame."""
        transformer = TransformationEngine("tests/configs/transformers/filters_drop_duplicates_config.yaml")
        transformation_id = "transformation_id_2"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
        # Check if the DataFrame has duplicates removed based on specified columns
        expected_data = [
            ("order_1", "customer_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0)),
            ("order_2", "customer_2", "product_2", 15, 30.0, datetime(2024, 2, 24, 14, 45, 0)),
            ("order_3", "customer_3", "product_3", 20, 40.0, datetime(2024, 2, 25, 16, 50, 0)),
            ("order_4", "customer_4", "product_4", 25, 50.0, datetime(2024, 2, 26, 18, 55, 0)),
            ("order_5", "customer_5", "product_5", 30, 60.0, datetime(2024, 2, 27, 20, 45, 0))
        ]
        expected_df = self.spark.createDataFrame(expected_data, self.test_dataframe.schema)
        
        self.assertEqual(transformed_df.count(), expected_df.count(), "Assertion failed: The count after dropping duplicates does not match the expected count.")
        
        # Assert if customer_1 to customer_5 exist in the transformed DataFrame
        customers = [row['customer_id'] for row in transformed_df.collect()]
        for customer_id in ["customer_1", "customer_2", "customer_3", "customer_4", "customer_5"]:
            self.assertIn(customer_id, customers, f"Assertion failed: {customer_id} is not present in the transformed DataFrame.")

if __name__ == "__main__":
    unittest.main()