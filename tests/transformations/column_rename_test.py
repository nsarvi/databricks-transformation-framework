import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from wilsonelser.transformation.engine import TransformationEngine
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session  that will be used across all tests."""
        cls.spark = TransformationEngine._get_spark()

    def setUp(self):
        """Creates a reusable DataFrame  with 10 rows."""
        schema = StructType([
            StructField("sales_id_test", StringType(), True),
            StructField("store_id_test", StringType(), True),
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
        transformer = TransformationEngine("tests/configs/transformers/column_rename_config.yaml")
        transformation_id = "transformation_id_1"
 
        transformed_df = transformer.apply_transformations(transformation_id, self.sample_dataframe)
        
        logger.info("transformed_df:\n%s", df_to_string(transformed_df))
 
        # Ensures row count is unchanged
        self.assertEqual(transformed_df.count(), self.sample_dataframe.count()) 
        # Check if additional column is renamed
        self.assertIn("sales_id", transformed_df.columns)  
        # Check if additional column is renamed
        self.assertIn("store_id", transformed_df.columns) 
        

       
if __name__ == "__main__":
    unittest.main()
