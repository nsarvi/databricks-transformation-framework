import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from wilsonelser.transformation.table_writer import TableWriter
from wilsonelser.transformation.utils.config_utils import ConfigUtils
from wilsonelser.transformation import yaml_constants as YC
from datetime import datetime
import logging

class TestTableWriter(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = TableWriter._get_spark()
        # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('wilsonelser.transformation.table_writer')
        self.logger.setLevel(logging.DEBUG)
        self.table_name_append="sandbox.integration_framework.no_schema_customer_cluster_by"
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_append}")


    def setUp(self):
        """Creates a reusable DataFrame with 10 rows."""

        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_address", StringType(), True),
            StructField("customer_phone", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("customer_status", StringType(), True),
            StructField("customer_since", TimestampType(), True),
            StructField("customer_type", StringType(), True),
            StructField("customer_region", StringType(), True),
            StructField("customer_sales", DoubleType(), True),
            StructField("customer_loyalty_points", IntegerType(), True),
            StructField("customer_preferred_store", StringType(), True),
            StructField("customer_credit_limit", DoubleType(), True),
            StructField("customer_last_purchase_date", TimestampType(), True)
        ])

        data = [
            ("C001", "John Doe", "123 Elm St", "555-1234", "john.doe@example.com", "active", datetime(2020, 1, 1, 12, 0, 0), "premium", "North", 1000.0, 100, "store_1", 5000.0, datetime(2021, 1, 1, 12, 0, 0)),
            ("C002", "Jane Doe", "456 Oak St", "555-5678", "jane.doe@example.com", "inactive", datetime(2019, 5, 15, 12, 0, 0), "regular", "South", 1500.0, 200, "store_2", 3000.0, datetime(2021, 6, 15, 12, 0, 0)),
            ("C003", "Alice Smith", "789 Pine St", "555-8765", "alice.smith@example.com", "active", datetime(2018, 3, 10, 12, 0, 0), "premium", "East", 2000.0, 300, "store_3", 7000.0, datetime(2021, 3, 10, 12, 0, 0)),
            ("C004", "Bob Brown", "321 Maple St", "555-4321", "bob.brown@example.com", "inactive", datetime(2017, 7, 20, 12, 0, 0), "regular", "West", 2500.0, 400, "store_4", 6000.0, datetime(2021, 7, 20, 12, 0, 0)),
            ("C005", "Charlie Black", "654 Cedar St", "555-6789", "charlie.black@example.com", "active", datetime(2016, 11, 30, 12, 0, 0), "premium", "North", 3000.0, 500, "store_5", 8000.0, datetime(2021, 11, 30, 12, 0, 0)),
            ("C006", "Diana White", "987 Birch St", "555-9876", "diana.white@example.com", "inactive", datetime(2015, 2, 25, 12, 0, 0), "regular", "South", 3500.0, 600, "store_1", 4000.0, datetime(2021, 2, 25, 12, 0, 0)),
            ("C007", "Eve Green", "159 Spruce St", "555-1357", "eve.green@example.com", "active", datetime(2014, 6, 5, 12, 0, 0), "premium", "East", 4000.0, 700, "store_2", 9000.0, datetime(2021, 6, 5, 12, 0, 0)),
            ("C008", "Frank Blue", "753 Willow St", "555-2468", "frank.blue@example.com", "inactive", datetime(2013, 9, 15, 12, 0, 0), "regular", "West", 4500.0, 800, "store_3", 5000.0, datetime(2021, 9, 15, 12, 0, 0)),
            ("C009", "Grace Yellow", "852 Aspen St", "555-3698", "grace.yellow@example.com", "active", datetime(2012, 12, 25, 12, 0, 0), "premium", "North", 5000.0, 900, "store_4", 10000.0, datetime(2021, 12, 25, 12, 0, 0)),
            ("C010", "Hank Red", "951 Poplar St", "555-1478", "hank.red@example.com", "inactive", datetime(2011, 4, 10, 12, 0, 0), "regular", "South", 5500.0, 1000, "store_5", 6000.0, datetime(2021, 4, 10, 12, 0, 0))
        ]


        self.test_data_frame = self.spark.createDataFrame(data, schema)
        
    @classmethod
    def tearDownClass(self):
        """Drops the table after each test."""
        self.logger.info("Dropping tables ")

        
    def test_write_table_using_config_file_with_no_schema_file(self):
        """Tests writing a DataFrame to a table with no schema."""
        writer = TableWriter("tests/configs/targets/write_table_no_schema_config.yaml")
        target_id = "target_customer_no_schema"

        # Write the DataFrame to the table
        writer.write_table(target_id,self.test_data_frame)

        # Verify the table exists
        self.assertTrue(self.spark.catalog.tableExists(self.table_name_append))

        # Read the table back into a DataFrame
        result_df = self.spark.table(self.table_name_append)

        # Verify the schema of the written table
        self.assertEqual(result_df.schema, self.test_data_frame.schema)
        self.logger.info("Table data before writing")
        self.logger.info(self.test_data_frame.show())
        self.logger.info("Table data after after writing")
        self.logger.info(result_df.show())
        # Verify the data of the written table
        self.assertEqual(result_df.count(), self.test_data_frame.count())
        
        # Verify the table creation with partition key
        history_df = self.spark.sql(f"""
            SELECT * 
            FROM (DESCRIBE HISTORY {self.table_name_append})
            ORDER BY version ASC
            LIMIT 1
        """)
        operation_parameters = history_df.select("operationParameters").collect()[0].asDict()
        self.assertIn("clusterBy", operation_parameters["operationParameters"])
        cluster_by = eval(operation_parameters["operationParameters"]["clusterBy"])
        self.assertEqual(cluster_by, ["customer_region"])
        self.logger.info("Test test_write_table_using_config_file_cluster_by passed")

    
if __name__ == "__main__":
    unittest.main()