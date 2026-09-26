import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

tests_src_path = (Path(__file__).resolve().parent.parent.parent / "tests/src").as_posix()
sys.path.append(tests_src_path)


print("Final sys.path:")
print("\n".join(sys.path))

from typing import Optional
import unittest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
import random

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session  that will be used across all tests."""
        cls.spark = BaseDataTransformer._get_spark()

    def setUp(self):
        """Creates a reusable DataFrame  with 10 rows."""
        schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("state", StringType(), True)  # Added state column
        ])

        state_codes = ["CA", "NY", "TX", "FL", "WA"]  # List of random state codes
        data = [
            (
            f"sale_{i}",
            f"store_1",
            f"product_{i}",
            i * 2,
            i * 10.5,
            datetime(2024, 2, 23, 12, 30, 0),
            random.choice(state_codes)  # Randomly assign a state code
            )
            for i in range(1, 11)  # Generates 10 rows
        ]

        self.sample_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_custom_functions(self):
        """Tests custom functions with column mappings on the DataFrame."""
        # Transformation ID for the test
        transformation_id = "transformation_id_1"

        # Create an instance of the transformer
        transformer = BaseDataTransformer("tests/configs/transformers/custom_function_config.yaml")

        # Apply transformations using the transformer
        transformed_df = transformer.apply_transformations(transformation_id, self.sample_dataframe)

        # Assertions to validate the transformations
        # Ensures row count is unchanged
        self.assertEqual(transformed_df.count(), self.sample_dataframe.count())

        # Check if the new columns are added
        self.assertIn("total_price_by_func", transformed_df.columns)
        self.assertIn("price", transformed_df.columns)
    

        # Validate the values in the new columns
        first_row: Optional[Row] = transformed_df.select("total_price_by_func", "price", "state").first()
        total_price_by_func_value = first_row["total_price_by_func"]  # type: ignore
        price_value = first_row["price"]  # type: ignore
    

        # Expected values for the first row
        first_row = self.sample_dataframe.first()
        if first_row is None:
            self.fail("Sample DataFrame is empty, cannot compute expected values.")
        expected_total_price_by_func = first_row["price"] * first_row["quantity"]
        first_row = self.sample_dataframe.first()
        if first_row is None:
            self.fail("Sample DataFrame is empty, cannot compute expected values.")
        expected_price = first_row["price"]
  
        # Validate the transformed values
        self.assertEqual(total_price_by_func_value, expected_total_price_by_func, f"Assertion failed: total_price_by_func {total_price_by_func_value} is not equal to expected {expected_total_price_by_func}")
        self.assertEqual(price_value, expected_price, f"Assertion failed: price {price_value} is not equal to expected {expected_price}")
        
        print(transformed_df.show())
if __name__ == "__main__":
    unittest.main()
