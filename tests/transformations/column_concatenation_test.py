import sys
from pathlib import Path

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

class TestDataTransformerColumnExpressions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session that will be used across all tests."""
        cls.spark = BaseDataTransformer._get_spark()

    def setUp(self):
        """Creates a reusable DataFrame with 20 rows."""
        schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("region", StringType(), True),
            StructField("state", StringType(), True)
        ])

        data = [
            (f"sale_{i}", f"store_1", f"product_{i}", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0), "North", "NY")
            for i in range(1, 6)  # Generates first 5 rows with region as North and state as NY
        ] + [
            (f"sale_{i}", f"store_2", f"product_{i}", 15, float(i * 2.5), datetime(2024, 2, 24, 14, 45, 0), "South", "TX")
            for i in range(6, 21)  # Generates the rest with region as South and state as TX
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_column_expressions(self):
        """Tests column expressions logic on the DataFrame."""
        transformer = BaseDataTransformer("tests/configs/transformers/column_concatenations_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        # Check if the new columns are added
        self.assertIn("total_price", transformed_df.columns)
        self.assertIn("discounted_price", transformed_df.columns)
        
        # Check if the expressions are correctly applied
        first_row: Optional[Row] = transformed_df.select("total_price", "discounted_price").first()
        total_price_value = first_row['total_price']  # type: ignore
        discounted_price_value = first_row['discounted_price']  # type: ignore
        
        expected_total_price = 10 * 20.0  # Fixed quantity and price
        expected_discounted_price = expected_total_price * 0.9
        
        self.assertAlmostEqual(total_price_value, expected_total_price, places=2, msg=f"Assertion failed: total_price {total_price_value} is not equal to expected {expected_total_price}")
        self.assertAlmostEqual(discounted_price_value, expected_discounted_price, places=2, msg=f"Assertion failed: discounted_price {discounted_price_value} is not equal to expected {expected_discounted_price}")

    def test_apply_column_concatenations(self):
        """Tests column concatenations logic on the DataFrame."""
        transformer = BaseDataTransformer("tests/configs/transformers/column_concatenations_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
        # Check if the new concatenated columns are added
        self.assertIn("region_state", transformed_df.columns)
        self.assertIn("region_state_with_trim", transformed_df.columns)
        
        # Check if the concatenations are correctly applied
        first_row: Optional[Row] = transformed_df.select("region_state", "region_state_with_trim").first()
        region_state_value = first_row['region_state']  # type: ignore
        region_state_with_trim_value = first_row['region_state_with_trim']  # type: ignore
        
        expected_region_state = "North | NY"  # Concatenation of region and state
        expected_region_state_with_trim = "North - NY"  # Concatenation of trimmed region and state
        
        self.assertEqual(region_state_value, expected_region_state, f"Assertion failed: region_state {region_state_value} is not equal to expected {expected_region_state}")
        self.assertEqual(region_state_with_trim_value, expected_region_state_with_trim, f"Assertion failed: region_state_with_trim {region_state_with_trim_value} is not equal to expected {expected_region_state_with_trim}")

if __name__ == "__main__":
    unittest.main()