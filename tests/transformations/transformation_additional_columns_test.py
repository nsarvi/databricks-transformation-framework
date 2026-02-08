import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

from typing import Optional
import unittest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer

class TestAdmDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session  that will be used across all tests."""
        cls.spark = AdmBaseDataTransformer._get_spark()

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

        self.sample_dataframe = self.spark.createDataFrame(data, schema)

    def test_apply_transformations(self):
        """Tests transformation logic on the DataFrame."""
        transformer = AdmBaseDataTransformer("tests/configs/transformers/adm_additional_column_config.yaml")
        transformation_id = "transformation_id_1"
        expected_customer_number="CUST_NBR"
        # Get the current date 
        expected_date = datetime.now().date()
        expected_region_empty=''
     
        transformed_df = transformer.apply_transformations(transformation_id, self.sample_dataframe,)
        # Get the first row of the DataFrame
        first_row:Optional[Row] = transformed_df.select("processed_date","region").first()
        processed_date_value = first_row['processed_date'].date() # type: ignore
        region_value=first_row['region'] # type: ignore
        
        print(transformed_df.show())
        # assertions on the additional column values
        # Ensures row count is unchanged
        self.assertEqual(transformed_df.count(), self.sample_dataframe.count()) 
        # Check if additional column is added
        self.assertIn("region", transformed_df.columns)  
        # Check if additional column is added
        self.assertIn("processed_date", transformed_df.columns) 
        # Check if column value is equal to configured constant
        assert transformed_df.filter(F.col("Customer_Number") != expected_customer_number).count() == 0 , f"Assertion failed: Customer_Number has no constant added {expected_customer_number}" 
        assert processed_date_value == expected_date, f"Assertion failed: processed_date {processed_date_value} is not equal to current date {expected_date}"
        assert region_value == expected_region_empty, f"Assertion failed: region  {region_value} is not equal to current date {expected_region_empty}"

if __name__ == "__main__":
    unittest.main()
