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
            StructField("timestamp", TimestampType(), True)
        ])

        data = [
            (f"sale_{i}", f"store_1", f"product_{i}", i * 2, i * 10.5, datetime(2024, 2, 23, 12, 30, 0))
            for i in range(1, 11)  # Generates 10 rows
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_transformations(self):
        """Tests transformation logic on the DataFrame."""
        transformer = BaseDataTransformer("tests/configs/transformers/filters_column_conditions_config.yaml")
        transformation_id = "transformation_id_1"
        # Apply transformation
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)

        # Display for debugging
        print(transformed_df.show())

        # Filtering Assertions
        expected_products = {"product_8", "product_9", "product_10"}
        
        # Verify row count matches expected filtered products
        assert transformed_df.count() == 3, f"Expected 3 rows, got {transformed_df.count()}"

        # Ensure only expected products remain
        filtered_products = set(row["product_id"] for row in transformed_df.select("product_id").collect())
        assert filtered_products == expected_products, f"Unexpected products found: {filtered_products}"

        # Ensure 'store_id' condition is applied
        assert transformed_df.filter(F.col("store_id").like("%no_store_like_this%")).count() == 0, "Unexpected store_id values found"

if __name__ == "__main__":
    unittest.main()
