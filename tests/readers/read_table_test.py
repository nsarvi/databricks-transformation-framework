from logging import config
import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)
config_file_path = (Path(__file__).resolve().parent.parent.parent / "tests/configs/sources").as_posix()
sys.path.append(config_file_path)
env_file_path = (Path(__file__).resolve().parent.parent.parent / "tests/configs/env").as_posix()
sys.path.append(env_file_path)

import sys
from pathlib import Path
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, Row
from typing import Optional
from wilsonelser.transformation.engine import TransformationEngine
import pytest
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()
pytestmark = pytest.mark.integration

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = TransformationEngine._get_spark()

    def setUp(self):
       
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

        # Write the DataFrame to the table with the engine
        writer = TransformationEngine("tests/configs/sources/target_tables.yaml")
        writer.write_table("source_customer_mapics",self.customer_df )

        # Create test data for adm_sales_mapics table
        sales_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("transaction_date", TimestampType(), True)
        ])
        sales_data = [
            ("trans_1", "cust_1", "prod_1", "store_1", 10, 45.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_2", "cust_2", "prod_2", "store_2", 20, 55.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_3", "cust_3", "prod_3", "store_3", 30, 35.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_4", "cust_4", "prod_4", "store_1", 15, 50.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_5", "cust_5", "prod_5", "store_2", 25, 60.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_6", "cust_6", "prod_6", "store_3", 35, 40.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_7", "cust_7", "prod_7", "store_1", 20, 55.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_8", "cust_8", "prod_8", "store_2", 30, 65.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_9", "cust_9", "prod_9", "store_3", 40, 35.0, datetime(2025, 3, 15, 12, 0, 0)),
            ("trans_10", "cust_10", "prod_10", "store_1", 50, 45.0, datetime(2025, 3, 15, 12, 0, 0))
        ]
        self.sales_df = self.spark.createDataFrame(sales_data, sales_schema)

        # Write the DataFrame to the table with the engine
        writer.write_table("source_sales_mapics",self.sales_df, )

    @classmethod
    def tearDownClass(cls):
        """Stops the Spark session after all tests."""
      
    
    def test_table_reads(self):
        """Tests reading from the source tables and applying transformations."""
        table_reader = TransformationEngine("tests/configs/sources/read_tables_config.yaml")
        
        result_df = table_reader.read_source_table("source_1")


        logger.info("result_df:\n%s", df_to_string(result_df))
        # assertions on the additional column values
        # Ensures row count is unchanged
        self.assertEqual(result_df.count(), self.customer_df.count()) 
        # Check if additional column is added
        self.assertIn("source_system", result_df.columns)  
                # Get the first row of the DataFrame
        first_row:Optional[Row] = result_df.select("processed_date","source_system").first()

        source_sys_value = first_row['source_system']  # type: ignore
        self.assertEqual(source_sys_value, "MAPICS")
    
    def test_table_reads_using_environments(self):
        """Tests reading from the source tables and applying transformations using environments."""
        table_reader = TransformationEngine("tests/configs/sources/read_env_specific_tables_config.yaml", "tests/configs/env/test-env-config.yaml")
        
        result_df = table_reader.read_source_table("source_1")


        logger.info("result_df:\n%s", df_to_string(result_df))
        # assertions on the additional column values
        # Ensures row count is unchanged
        self.assertEqual(result_df.count(), self.customer_df.count()) 
        # Check if additional column is added
        self.assertIn("source_system", result_df.columns)  
                # Get the first row of the DataFrame
        first_row:Optional[Row] = result_df.select("processed_date","source_system").first()

        source_sys_value = first_row['source_system']  # type: ignore
        self.assertEqual(source_sys_value, "MAPICS")

    def test_table_reads_using_environment_file_from_root(self):
        """Tests reading from the source tables and applying transformations using environments."""
        
        table_reader = TransformationEngine("read_env_specific_tables_config.yaml", "test-env-config.yaml")
        
        result_df = table_reader.read_source_table("source_1")


        logger.info("result_df:\n%s", df_to_string(result_df))
        # assertions on the additional column values
        # Ensures row count is unchanged
        self.assertEqual(result_df.count(), self.customer_df.count()) 
        # Check if additional column is added
        self.assertIn("source_system", result_df.columns)  
                # Get the first row of the DataFrame
        first_row:Optional[Row] = result_df.select("processed_date","source_system").first()

        source_sys_value = first_row['source_system']  # type: ignore
        self.assertEqual(source_sys_value, "MAPICS")

if __name__ == "__main__":
    unittest.main()