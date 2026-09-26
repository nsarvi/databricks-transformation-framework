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

class TestDataTransformerDistinctColumns(unittest.TestCase):

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
            StructField("timestamp", TimestampType(), True)
        ])

        data = [
            (f"sale_{i}", f"store_1", f"product_{i}", 10, 20.0, datetime(2024, 2, 23, 12, 30, 0))
            for i in range(1, 11)  # Generates 10 rows with fixed quantity and price
        ] + [
            (f"sale_{i}", f"store_2", f"product_{i}", 15, float(i * 2.5), datetime(2024, 2, 24, 14, 45, 0))
            for i in range(11, 21)  # Generates additional 10 rows with price between 10 and 50
        ]

        self.test_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_distinct_columns(self):
        """Tests distinct columns logic on the DataFrame."""
        transformer = BaseDataTransformer("tests/configs/transformers/filters_distinct_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        print(transformed_df.show())
        
        # Check if the DataFrame is distinct based on specified columns
        distinct_df = self.test_dataframe.select("sales_id", "product_id").distinct()
        self.assertEqual(transformed_df.count(), distinct_df.count(), "Assertion failed: The distinct count does not match the expected count.")
        
        # Check if the distinct columns are correctly applied
        distinct_columns = ["sales_id", "product_id"]
        for column in distinct_columns:
            self.assertIn(column, transformed_df.columns, f"Assertion failed: Column {column} is not in the transformed DataFrame")

if __name__ == "__main__":
    unittest.main()