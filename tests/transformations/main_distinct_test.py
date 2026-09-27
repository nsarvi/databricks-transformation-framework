import sys
from pathlib import Path
import random

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
        transformer = BaseDataTransformer("tests/configs/transformers/main_transformation_distinct_config.yaml")
        transformation_id = "transformation_id_1"
        logger.info("test_dataframe:\n%s", df_to_string(self.test_dataframe))
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        
    # Check if the distinct columns are correctly applied
        self.assertIn("sales_id", transformed_df.columns)
        self.assertIn("store_id", transformed_df.columns)
        self.assertIn("product_id", transformed_df.columns)
  
                
        # Check if the distinct columns are correctly applied
        distinct_count = transformed_df.distinct().count()
        original_count = self.test_dataframe.count()
        self.assertEqual(distinct_count, original_count - 5, f"Assertion failed: distinct count {distinct_count} is not equal to expected {original_count - 5}")

if __name__ == "__main__":
    unittest.main()