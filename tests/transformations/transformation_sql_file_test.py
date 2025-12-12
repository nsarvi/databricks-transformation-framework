import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from adm.integration.adm_data_transformer import AdmDataTransformer
from adm.integration.adm_table_writer import AdmTableWriter
import logging


class TestSQLTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Creates a Spark session and sets up test data."""
        cls.spark = SparkSession.builder \
            .appName("TestSQLTransformations") \
            .master("local[*]") \
            .getOrCreate()

        # Create test data for customer table
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
        ]
        cls.customer_df = cls.spark.createDataFrame(customer_data, customer_schema)

        # Write the DataFrame to the table using AdmTableWriter
        writer = AdmTableWriter("tests/configs/sources/sql_file_source_config.yaml")
        writer.write_table("source_customer_mapics", cls.customer_df)

        # Create test data for sales table
        sales_schema = StructType([
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
        ]
        cls.sales_df = cls.spark.createDataFrame(sales_data, sales_schema)

        # Write the DataFrame to the table using AdmTableWriter
        writer.write_table("source_sales_mapics", cls.sales_df)

    @classmethod
    def tearDownClass(cls):
        """Stops the Spark session after all tests."""
        cls.spark.stop()

    def test_sql_transformation(self):
        """Tests the SQL transformation using a configuration file."""
        transformer = AdmDataTransformer("tests/configs/transformers/adm_transformation_sql_file_config.yaml")
        result_df = transformer.apply_transformations("transformation_id_1")

        # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('adm.integration.adm_table_writer')
        logger.setLevel(logging.DEBUG)

        # Log the input and output DataFrames
        logger.info("Customer DataFrame:")
        self.customer_df.show()
        logger.info("Sales DataFrame:")
        self.sales_df.show()
        logger.info("Result DataFrame after SQL file transformation:")
        result_df.show()

        # Validate the result
        # Check if the result contains the expected columns and rows
        expected_columns = ["customer_id", "customer_name", "transaction_id", "total_amount"]
        self.assertTrue(all(col in result_df.columns for col in expected_columns), "Result DataFrame is missing expected columns.")

        # Check if specific rows exist in the result
        result_data = [row.customer_id for row in result_df.select("customer_id").collect()]
        self.assertIn("cust_1", result_data, "cust_1 is not in the result set")
    


if __name__ == "__main__":
    unittest.main()