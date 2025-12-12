import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer

class TestAdmDataTransformerColumnExpressions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session that will be used across all tests."""
        cls.spark = AdmBaseDataTransformer._get_spark()

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
        transformer = AdmBaseDataTransformer("tests/configs/transformers/adm_column_expression_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id,self.test_dataframe,)
        print(transformed_df.show())
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

    def test_discounted_price_data_type(self):
        """Tests the data type of the discounted_price column."""
        transformer = AdmBaseDataTransformer("tests/configs/transformers/adm_column_expression_config.yaml")
        transformation_id = "transformation_id_1"
        
        transformed_df = transformer.apply_transformations(transformation_id, self.test_dataframe)
        print(transformed_df.show())
        
        # Check if the data type of discounted_price is double
        discounted_price_type = dict(transformed_df.dtypes)["discounted_price"]
        self.assertEqual(discounted_price_type, "double", f"Assertion failed: discounted_price type {discounted_price_type} is not equal to expected double")


if __name__ == "__main__":
    unittest.main()