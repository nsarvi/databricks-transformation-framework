import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import sys
from pathlib import Path
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from typing import Optional
from adm.integration.adm_data_transformer import AdmDataTransformer
from adm.integration.adm_table_writer import AdmTableWriter
from adm.integration.adm_table_reader import AdmTableReader
import logging

class TestAdmDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = AdmTableReader._get_spark()
        self.table_name_customer_mapics="sandbox.integration_framework.customer_mapics"

             
    def setUp(self):
         # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('adm.integration')
        logger.setLevel(logging.DEBUG)
       
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_customer_mapics}")
       # Create test data for adm_customer_mapics table
        customer_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("customer_phone_mapics", StringType(), True),
            StructField("customer_email_mapics", StringType(), True),
            StructField("customer_status_mapics", StringType(), True),
            StructField("customer_since_mapics", TimestampType(), True),
            StructField("customer_type_mapics", StringType(), True),
            StructField("customer_region", StringType(), True),
            StructField("customer_loyalty_points", IntegerType(), True),
            StructField("customer_preferred_store", StringType(), True)
        ])
        customer_data = [
            ("cust_1", "John Doe", "123 Elm St", "555-1234", "john.doe@example.com", "active", datetime(2020, 1, 1), "regular", "region_1", 100, "store_1"),
            ("cust_2", "Jane Doe", "456 Oak St", "555-5678", "jane.doe@example.com", "inactive", datetime(2019, 5, 15), "premium", "region_2", 200, "store_2"),
            ("cust_3", "Alice Smith", "789 Pine St", "555-8765", "alice.smith@example.com", "active", datetime(2021, 3, 10), "regular", "region_3", 150, "store_3"),
            ("cust_4", "Bob Johnson", "321 Maple St", "555-4321", "bob.johnson@example.com", "active", datetime(2018, 7, 22), "regular", "region_1", 120, "store_1"),
            ("cust_5", "Carol White", "654 Birch St", "555-6543", "carol.white@example.com", "inactive", datetime(2020, 11, 5), "premium", "region_2", 180, "store_2"),
            ("cust_6", "David Brown", "987 Cedar St", "555-9876", "david.brown@example.com", "active", datetime(2017, 2, 14), "regular", "region_3", 130, "store_3"),
            ("cust_7", "Eva Green", "159 Spruce St", "555-1597", "eva.green@example.com", "active", datetime(2021, 8, 30), "premium", "region_1", 170, "store_1"),
            ("cust_8", "Frank Black", "753 Willow St", "555-7539", "frank.black@example.com", "inactive", datetime(2019, 12, 25), "regular", "region_2", 110, "store_2"),
            ("cust_9", "Grace Blue", "951 Fir St", "555-9512", "grace.blue@example.com", "active", datetime(2020, 6, 18), "premium", "region_3", 160, "store_3"),
            ("cust_10", "Henry Yellow", "852 Redwood St", "555-8524", "henry.yellow@example.com", "active", datetime(2018, 9, 9), "regular", "region_1", 140, "store_1")
        ]
        self.customer_df = self.spark.createDataFrame(customer_data, customer_schema)

        # Write the DataFrame to the table using AdmTableWriter
        writer = AdmTableWriter("tests/configs/sources/target_tables.yaml")
        writer.write_table("source_customer_mapics",self.customer_df )

       
    @classmethod
    def tearDownClass(cls):
        """Stops the Spark session after all tests."""
      

    def test_subquery_execution(self):
        """Tests reading from a subquery and joining with another source."""
        # Initialize the table reader
        adm_table_reader = AdmTableReader("tests/configs/sources/read_from_subquery_config.yaml")

        # Read the subquery source
        subquery_df = adm_table_reader.read_source_table("sapitmxref_subquery")
        
        # Show the result for debugging
        print(subquery_df.show())

        # 1. Validate that the DataFrame is not empty
        self.assertGreater(subquery_df.count(), 0, "The subquery DataFrame is empty.")

        # 2. Validate that the required columns exist
        self.assertIn("customer_id", subquery_df.columns, "Column 'customer_id' is missing in the subquery DataFrame.")
        self.assertIn("total_loyalty_points", subquery_df.columns, "Column 'total_loyalty_points' is missing in the subquery DataFrame.")

        # 3. Validate the sum of loyalty points for all customers
        actual_total_loyalty_points = subquery_df.agg(F.sum("total_loyalty_points").alias("sum_loyalty_points")).collect()[0]["sum_loyalty_points"]
        expected_total_loyalty_points = self.customer_df.agg(F.sum("customer_loyalty_points").alias("sum_loyalty_points")).collect()[0]["sum_loyalty_points"]

        self.assertEqual(
            actual_total_loyalty_points,
            expected_total_loyalty_points,
            f"Total loyalty points mismatch. Expected: {expected_total_loyalty_points}, Got: {actual_total_loyalty_points}"
        )

        # 4. Validate that all customer IDs in the subquery result exist in the original customer table
        subquery_customer_ids = set(row["customer_id"] for row in subquery_df.select("customer_id").collect())
        original_customer_ids = set(row["customer_id"] for row in self.customer_df.select("customer_id").collect())

        self.assertTrue(
            subquery_customer_ids.issubset(original_customer_ids),
            "Some customer IDs in the subquery result do not exist in the original customer table."
        )
        

if __name__ == "__main__":
    unittest.main()