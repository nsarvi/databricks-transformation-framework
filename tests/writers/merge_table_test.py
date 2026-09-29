import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from wilsonelser.transformation.engine import TransformationEngine
from wilsonelser.transformation.utils.config_utils import ConfigUtils
from wilsonelser.transformation import yaml_constants as YC
from datetime import datetime
import pytest
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string
from integration_env import table_name

logger = LoggingHandler(__name__).get_logger()
pytestmark = pytest.mark.integration

class TestTableWriter(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = TransformationEngine._get_spark()
        self.table_name_merge=table_name("customer_sales")
        self.table_name_customer_mapics=table_name("customer_mapics")
        self.table_name_sales_mapics=table_name("sales_mapics")


    def setUp(self):
        """Creates a reusable DataFrame with 10 rows."""
        logger.info("dropping tables if they exist  %s", self.table_name_merge)
        self.table_name_merge=table_name("customer_sales")

        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_sales_mapics}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_customer_mapics}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_merge}")
        
        # Create test data for adm_customer_mapics table - Initial data for merge
        self.customer_schema = StructType([
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
            ("cust_5", "Carol White", "654 Birch St", "555-6543", "carol.white@example.com", "inactive", datetime(2020, 11, 5), "premium", "region_2", 180, "store_2")

        ]
         # Create test data for adm_sales_mapics table - - Initial data for merge
        self.sales_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id_mapics", StringType(), True),
            StructField("product_id_mapics", StringType(), True),
            StructField("store_id_mapics", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date_mapics", TimestampType(), True),
            StructField("payment_method_mapics", StringType(), True)
        ])
        sales_data = [
            ("trans_1", "cust_1", "prod_1", "store_1", 10, 45.0, 5.0, 40.0, datetime(2025, 3, 15, 12, 0, 0), "Credit Card"),
            ("trans_2", "cust_2", "prod_2", "store_2", 20, 55.0, 10.0, 45.0, datetime(2025, 3, 15, 12, 0, 0), "Debit Card"),
            ("trans_3", "cust_3", "prod_3", "store_3", 30, 35.0, 5.0, 30.0, datetime(2025, 3, 15, 12, 0, 0), "Cash"),
            ("trans_4", "cust_4", "prod_4", "store_1", 15, 50.0, 7.5, 42.5, datetime(2025, 3, 15, 12, 0, 0), "Credit Card"),
            ("trans_5", "cust_5", "prod_5", "store_2", 25, 60.0, 12.0, 48.0, datetime(2025, 3, 15, 12, 0, 0), "Debit Card")
        ]

        customer_df = self.spark.createDataFrame(customer_data, self.customer_schema)

        # Write the DataFrame to the table with the engine
        writer = TransformationEngine("tests/configs/merge/source_config.yaml")
        writer.write_table("source_customer_mapics",customer_df)

        self.sales_df = self.spark.createDataFrame(sales_data, self.sales_schema)

        # Write the DataFrame to the table with the engine
        writer.write_table("source_sales_mapics",self.sales_df)

        # Create initial target table for merge test
        target_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("customer_phone_mapics", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("product_id_mapics", StringType(), True),
            StructField("store_id_mapics", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])
        initial_target_data = [
            ("initial_cust_1", "John Doe", "123 Elm St", "555-1234", "john.doe@example.com", "trans_1", "prod_1", "store_1", 10, 45.0),
            ("initial_cust_2", "Jane Doe", "456 Oak St", "555-5678", "jane.doe@example.com", "trans_2", "prod_2", "store_2", 20, 55.0),
            ("initial_cust_3", "Alice Smith", "789 Pine St", "555-8765", "alice.smith@example.com", "trans_3", "prod_3", "store_3", 30, 35.0),
            ("initial_cust_4", "Bob Johnson", "321 Maple St", "555-4321", "bob.johnson@example.com", "trans_4", "prod_4", "store_1", 15, 50.0),
            ("initial_cust_5", "Carol White", "654 Birch St", "555-6543", "carol.white@example.com", "trans_5", "prod_5", "store_2", 25, 60.0)
        ]
        initial_target_df = self.spark.createDataFrame(initial_target_data, target_schema)
        initial_target_df.write.mode("overwrite").saveAsTable(self.table_name_merge)
        logger.info("Initial target table %s created with data.", self.table_name_merge)

    @classmethod
    def tearDownClass(self):
        """Drops the table after each test."""
        logger.info("Dropping tables ")
        # self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_merge}")


    def test_write_table_merge(self):
        """Tests the write_table method with merge operation."""
        writer = TransformationEngine("tests/configs/targets/write_table_merge_config.yaml")
        target_id = "target_customer_sales"

        # Perform merge operation
        writer.write_table(table_id=target_id)

        # Verify the schema of the resulting DataFrame
        expected_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("customer_phone_mapics", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("product_id_mapics", StringType(), True),
            StructField("store_id_mapics", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        # Verify the schema of the resulting DataFrame
        result_df = self.spark.table(self.table_name_merge)
        self.assertEqual(result_df.schema, expected_schema)
        logger.info("Resulting DataFrame schema: ")
        logger.info("result_df:\n%s", df_to_string(result_df))

        # Verify if the initial data loaded exists
        result_customer_ids = [row['customer_id'] for row in result_df.select("customer_id").collect()]
        expected_customer_ids = ["initial_cust_1", "initial_cust_2", "initial_cust_3", "initial_cust_4", "initial_cust_5"]
        
        for customer_id in expected_customer_ids:
            self.assertIn(customer_id, result_customer_ids)
        logger.info("Verified initial customer IDs exist in the resulting DataFrame: %s", expected_customer_ids)
        # Verify if the merged data exists - Filters are applied post merge for the price < 50
        expected_merged_customer_ids = ["cust_1", "cust_3"]
        
        for customer_id in expected_merged_customer_ids:
            self.assertIn(customer_id, result_customer_ids)
        logger.info("Verified merged customer IDs exist in the resulting DataFrame: %s", expected_merged_customer_ids)

    
    def test_verify_inserted_customer_sales(self):
        logger.info("Testing insert and merge operation for initial_cust_1")
        """Tests the insert and merge operation for initial_cust_1."""
        writer = TransformationEngine("tests/configs/targets/write_table_merge_config.yaml")
        target_id = "target_customer_sales"

        # Insert new customer and sales data for initial_cust_1
        new_customer_data = [
            ("initial_cust_1", "inserted_John Doe", "inserted_123 Elm St", "inserted_555-1234", "inserted_john.doe@example.com", "inserted_active", datetime(2020, 1, 1), "inserted_regular", "inserted_region_1", 100, "inserted_store_1")
        ]
        new_sales_data = [
            ("trans_6", "initial_cust_1", "prod_6", "store_1", 5, 25.0, 2.5, 22.5, datetime(2025, 3, 16, 12, 0, 0), "Credit Card")
        ]

        new_customer_df = self.spark.createDataFrame(new_customer_data, self.customer_schema)
        new_sales_df = self.spark.createDataFrame(new_sales_data, self.sales_schema)

        source_writer = TransformationEngine("tests/configs/merge/source_config.yaml")
        source_writer.write_table("source_customer_mapics",new_customer_df)
        source_writer.write_table("source_sales_mapics", new_sales_df)

        # Perform merge operation
        writer.write_table(table_id=target_id)
        
        result_df = self.spark.table(self.table_name_merge)
        logger.info("Resulting DataFrame schema: ")
        logger.info("result_df:\n%s", df_to_string(result_df))

        # Verify if the initial data loaded exists
        result_customer_ids = [row['customer_id'] for row in result_df.select("customer_id").collect()]
        expected_customer_ids = ["initial_cust_1", "initial_cust_2", "initial_cust_3", "initial_cust_4", "initial_cust_5"]

        for customer_id in expected_customer_ids:
            self.assertIn(customer_id, result_customer_ids)
        logger.info("Verified initial customer IDs exist in the resulting DataFrame: %s", expected_customer_ids)
        
        # Verify the inserted and merged data for initial_cust_1
        result_customer_data = result_df.filter(result_df.customer_id == "initial_cust_1").collect()
        self.assertEqual(len(result_customer_data), 1)
        self.assertEqual(result_customer_data[0]['customer_name'], "inserted_John Doe")
        self.assertEqual(result_customer_data[0]['product_id_mapics'], "prod_6")
        logger.info("Verified inserted and merged data for initial_cust_1: %s", result_customer_data[0])

    @unittest.skip("Skipping ")
    def test_merge_duplicate_customer_sales(self):
        logger.info("Testing merge operation for duplicate customer data")
        """Tests the merge operation for duplicate customer data."""
        writer = TransformationEngine("tests/configs/targets/write_table_merge_config.yaml")
        target_id = "target_customer_sales"

        # Insert duplicate customer and sales data
        duplicate_customer_data = [
            ("cust_1", "duplicate_1", "123 Elm St", "555-1234", "john.doe@example.com", "active", datetime(2020, 1, 1), "regular", "region_1", 100, "store_1"),
            ("cust_1", "duplicate_2", "123 Elm St", "555-1234", "john.doe@example.com", "active", datetime(2020, 1, 1), "regular", "region_1", 100, "store_1"),
            ("cust_2", "duplicate_3", "456 Oak St", "555-5678", "jane.doe@example.com", "inactive", datetime(2019, 5, 15), "premium", "region_2", 200, "store_2"),
            ("cust_2", "duplicate_4", "456 Oak St", "555-5678", "jane.doe@example.com", "inactive", datetime(2019, 5, 15), "premium", "region_2", 200, "store_2"),
            ("cust_3", "duplicate_5", "789 Pine St", "555-8765", "alice.smith@example.com", "active", datetime(2021, 3, 10), "regular", "region_3", 150, "store_3"),
            ("cust_3", "duplicate_6", "789 Pine St", "555-8765", "alice.smith@example.com", "active", datetime(2021, 3, 10), "regular", "region_3", 150, "store_3"),
            ("cust_4", "duplicate_7", "321 Maple St", "555-4321", "bob.johnson@example.com", "active", datetime(2018, 7, 22), "regular", "region_1", 120, "store_1"),
            ("cust_4", "duplicate_8", "321 Maple St", "555-4321", "bob.johnson@example.com", "active", datetime(2018, 7, 22), "regular", "region_1", 120, "store_1"),
            ("cust_5", "duplicate_9", "654 Birch St", "555-6543", "carol.white@example.com", "inactive", datetime(2020, 11, 5), "premium", "region_2", 180, "store_2"),
            ("cust_5", "duplicate_10", "654 Birch St", "555-6543", "carol.white@example.com", "inactive", datetime(2020, 11, 5), "premium", "region_2", 180, "store_2")
        ]
        duplicate_sales_data = [
            ("trans_6", "cust_1", "prod_6", "store_1", 5, 25.0, 2.5, 22.5, datetime(2025, 3, 16, 12, 0, 0), "Credit Card"),
            ("trans_7", "cust_1", "prod_7", "store_1", 10, 50.0, 5.0, 45.0, datetime(2025, 3, 17, 12, 0, 0), "Credit Card")
        ]

        duplicate_customer_df = self.spark.createDataFrame(duplicate_customer_data, self.customer_schema)
        duplicate_sales_df = self.spark.createDataFrame(duplicate_sales_data, self.sales_schema)

        source_writer = TransformationEngine("tests/configs/merge/source_config.yaml")
        source_writer.write_table("source_customer_mapics", duplicate_customer_df)
        source_writer.write_table("source_sales_mapics", duplicate_sales_df)

        # Perform merge operation
        writer.write_table(table_id=target_id)
        
        result_df = self.spark.table(self.table_name_merge)
        logger.info("Resulting DataFrame schema: ")
        logger.info("result_df:\n%s", df_to_string(result_df))

        # Verify if the initial data loaded exists
        result_customer_ids = [row['customer_id'] for row in result_df.select("customer_id").collect()]
        expected_customer_ids = ["initial_cust_1", "initial_cust_2", "initial_cust_3", "initial_cust_4", "initial_cust_5"]

        for customer_id in expected_customer_ids:
            self.assertIn(customer_id, result_customer_ids)
        logger.info("Verified initial customer IDs exist in the resulting DataFrame: %s", expected_customer_ids)
        
        # Verify the merged data for cust_1
        result_customer_data = result_df.filter(result_df.customer_id == "cust_1").collect()
        self.assertEqual(len(result_customer_data), 2)
        self.assertEqual(result_customer_data[0]['product_id_mapics'], "prod_6")
        logger.info("Verified merged data for cust_1: %s", result_customer_data)

if __name__ == "__main__":
    unittest.main()