import sys
from pathlib import Path
import random

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

from typing import Optional
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()

class TestDataTransformerMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session that will be used across all tests."""
        cls.spark = BaseDataTransformer._get_spark()
        
    def setUp(self):
        """Creates a reusable DataFrame with 25 rows, including 5 duplicate rows."""
        schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("region", StringType(), True),
            StructField("state", StringType(), True),
            StructField("Payment_ID", IntegerType(), True)
        ])

        regions = ["NY", "TX", "CA", "MD"]
        data = [
            (f"sale_{i}", f"store_1", f"product_{i}", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0), random.choice(regions), "NY", random.randint(1, 1000))
            for i in range(1, 6)  # Generates first 5 rows with random region and Payment_ID
        ] + [
            (f"sale_{i}", f"store_2", f"product_{i}", 15, float(i * 2.5), datetime(2024, 2, 24, 14, 45, 0), random.choice(regions), "TX", random.randint(1, 1000))
            for i in range(6, 21)  # Generates the next 15 rows with random region and Payment_ID
        ] + [
            ("sale_1", "store_1", "product_1", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0), random.choice(regions), "NY", random.randint(1, 1000))
            for _ in range(5)  # Generates 5 duplicate rows
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_transformations(self):
        """Tests the main transformations logic on the DataFrame."""
        transformer = BaseDataTransformer("tests/configs/transformers/main_transformation_config.yaml")
        transformation_id = "transformation_id_1"
        logger.info("test_dataframe:\n%s", df_to_string(self.test_dataframe))
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
        # Check if the new columns are added
        self.assertIn("processed_date", transformed_df.columns)
        self.assertIn("Customer_Number", transformed_df.columns)
        self.assertIn("Payment_ID", transformed_df.columns)
        self.assertIn("Invoice_Due_Date", transformed_df.columns)
        self.assertIn("total_price", transformed_df.columns)
        self.assertIn("discounted_price", transformed_df.columns)
        self.assertIn("price_category", transformed_df.columns)
        self.assertIn("region_state", transformed_df.columns)
        self.assertIn("region_state_with_trim", transformed_df.columns)
        
        # Check if the expressions are correctly applied
        first_row: Optional[Row] = transformed_df.select("total_price", "discounted_price", "price_category").first()
        total_price_value = first_row['total_price']  # type: ignore
        discounted_price_value = first_row['discounted_price']  # type: ignore
        price_category_value = first_row['price_category']  # type: ignore
        
        expected_total_price = 10 * 20.0  # Fixed quantity and price
        expected_discounted_price = expected_total_price * 0.9
        expected_price_category = "Low"  # Price is 20.0 which is less than 30
        
        self.assertAlmostEqual(total_price_value, expected_total_price, places=2, msg=f"Assertion failed: total_price {total_price_value} is not equal to expected {expected_total_price}")
        self.assertAlmostEqual(discounted_price_value, expected_discounted_price, places=2, msg=f"Assertion failed: discounted_price {discounted_price_value} is not equal to expected {expected_discounted_price}")
        self.assertEqual(price_category_value, expected_price_category, f"Assertion failed: price_category {price_category_value} is not equal to expected {expected_price_category}")
        
        # Check if the concatenations are correctly applied
        first_row: Optional[Row] = transformed_df.select("region_state", "region_state_with_trim").first()
        region_state_value = first_row['region_state']  # type: ignore
        region_state_with_trim_value = first_row['region_state_with_trim']  # type: ignore
        
        # Check if the distinct columns are correctly applied
        distinct_columns = ["sales_id", "store_id", "product_id", "quantity", "price", "timestamp", "region", "state", "processed_date", "Customer_Number", "Payment_ID", "Invoice_Due_Date"]
        distinct_df = transformed_df.dropDuplicates(distinct_columns)
        distinct_count = distinct_df.count()
        original_count = self.test_dataframe.count()
        logger.info("After dropping duplicates")
        logger.info("distinct_df:\n%s", df_to_string(distinct_df))
          
if __name__ == "__main__":
    unittest.main()