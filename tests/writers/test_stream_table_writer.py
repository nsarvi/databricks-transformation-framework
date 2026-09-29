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
        self.stream_target_table_name=table_name("adm_target_customer_stream_1")
        self.stream_source_table_name=table_name("customer_mapics_stream_1")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.stream_target_table_name}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.stream_source_table_name}")
        
        self.schema = StructType([
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
        

    def setUp(self):
        """Creates a reusable DataFrame with 10 rows."""

        

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


        self.test_data_frame = self.spark.createDataFrame(data, self.schema)
        writer = TransformationEngine("tests/configs/targets/write_stream_source_tables_config.yaml")
        target_id = "source_customer_stream_1"
        writer.write_table(target_id,self.test_data_frame )
        
        
    @classmethod
    def tearDownClass(self):
        """Drops the table after each test."""
        logger.info("Dropping tables ")
      #  self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_append}")
      #  self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_overwrite}")
      #  self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name_cluster_by}")
        
        
    def test_write_stream_table_using_config_file_append(self):
        
        table_reader = TransformationEngine("tests/configs/targets/write_stream_tables_config.yaml")
        
        # Read the streaming source table
        result_df = table_reader.read_source_table("source_customer_stream_1")
        
        writer = TransformationEngine("tests/configs/targets/write_stream_tables_config.yaml")
        target_id = "target_customer_stream_1"

        # Write the DataFrame to the table
        writer.write_table(target_id,result_df)

        # Verify the table exists
        self.assertTrue(self.spark.catalog.tableExists(self.stream_target_table_name))

        # Read the table back into a DataFrame
        result_df = self.spark.table(self.stream_target_table_name)

        # Verify the schema of the written table
        self.assertEqual(result_df.schema, self.test_data_frame.schema)
        logger.info("Table data before writing")
        logger.info("test_data_frame:\n%s", df_to_string(self.test_data_frame))
        logger.info("Table data after after writing")
        logger.info("result_df:\n%s", df_to_string(result_df))
        # Verify the data of the written table
        self.assertEqual(result_df.count(), self.test_data_frame.count())

        # add additional data to source table 
        data_2 = [
        ("C011", "Michael Scott", "1725 Slough Ave", "555-1111", "michael.scott@dundermifflin.com", "active", datetime(2021, 1, 1, 10, 0, 0), "premium", "East", 1200.0, 150, "store_1", 6000.0, datetime(2022, 1, 1, 10, 0, 0)),
        ("C012", "Pam Beesly", "123 Art St", "555-2222", "pam.beesly@dundermifflin.com", "inactive", datetime(2020, 5, 15, 11, 0, 0), "regular", "West", 1800.0, 250, "store_2", 4000.0, datetime(2022, 6, 15, 11, 0, 0)),
        ("C013", "Jim Halpert", "456 Prank Rd", "555-3333", "jim.halpert@dundermifflin.com", "active", datetime(2019, 3, 10, 9, 0, 0), "premium", "North", 2200.0, 350, "store_3", 7500.0, datetime(2022, 3, 10, 9, 0, 0)),
        ("C014", "Dwight Schrute", "789 Beet Ln", "555-4444", "dwight.schrute@dundermifflin.com", "inactive", datetime(2018, 7, 20, 8, 0, 0), "regular", "South", 2700.0, 450, "store_4", 6500.0, datetime(2022, 7, 20, 8, 0, 0)),
        ("C015", "Angela Martin", "321 Cat St", "555-5555", "angela.martin@dundermifflin.com", "active", datetime(2017, 11, 30, 7, 0, 0), "premium", "East", 3200.0, 550, "store_5", 8500.0, datetime(2022, 11, 30, 7, 0, 0)),
        ("C016", "Kevin Malone", "654 Chili Ave", "555-6666", "kevin.malone@dundermifflin.com", "inactive", datetime(2016, 2, 25, 6, 0, 0), "regular", "West", 3700.0, 650, "store_1", 4500.0, datetime(2022, 2, 25, 6, 0, 0)),
        ("C017", "Stanley Hudson", "987 Crossword Blvd", "555-7777", "stanley.hudson@dundermifflin.com", "active", datetime(2015, 6, 5, 5, 0, 0), "premium", "North", 4200.0, 750, "store_2", 9500.0, datetime(2022, 6, 5, 5, 0, 0)),
        ("C018", "Kelly Kapoor", "159 Gossip Ln", "555-8888", "kelly.kapoor@dundermifflin.com", "inactive", datetime(2014, 9, 15, 4, 0, 0), "regular", "South", 4700.0, 850, "store_3", 5500.0, datetime(2022, 9, 15, 4, 0, 0)),
        ("C019", "Ryan Howard", "852 Temp St", "555-9999", "ryan.howard@dundermifflin.com", "active", datetime(2013, 12, 25, 3, 0, 0), "premium", "East", 5200.0, 950, "store_4", 10500.0, datetime(2022, 12, 25, 3, 0, 0)),
        ("C020", "Toby Flenderson", "951 HR Blvd", "555-0000", "toby.flenderson@dundermifflin.com", "inactive", datetime(2012, 4, 10, 2, 0, 0), "regular", "West", 5700.0, 1050, "store_5", 6500.0, datetime(2022, 4, 10, 2, 0, 0))
        ]
        
        self.test_data_frame = self.spark.createDataFrame(data_2, self.schema)
        writer = TransformationEngine("tests/configs/targets/write_stream_source_tables_config.yaml")
        target_id = "source_customer_stream_1"
        writer.write_table(target_id,self.test_data_frame )
        
        writer = TransformationEngine("tests/configs/targets/write_stream_tables_config.yaml")
        target_id = "target_customer_stream_1"
        # Read the updated streaming source table
        updated_result_df = table_reader.read_source_table("source_customer_stream_1")

        # Write the updated DataFrame to the target table
        target_id = "target_customer_stream_1"
        writer.write_table(target_id, updated_result_df)

        # Verify the table still exists
        self.assertTrue(self.spark.catalog.tableExists(self.stream_target_table_name))

        # Read the updated table back into a DataFrame
        updated_target_df = self.spark.table(self.stream_target_table_name)

        # Verify the schema of the updated table
        self.assertEqual(updated_target_df.schema, self.test_data_frame.schema)


        # Verify if specific records (C011 to C020) exist in the target written table
        expected_ids = {"C011", "C012", "C013", "C014", "C015", "C016", "C017", "C018", "C019", "C020"}
        actual_ids = set(row.customer_id for row in updated_target_df.select("customer_id").distinct().collect())
        self.assertTrue(expected_ids.issubset(actual_ids), "Not all expected customer IDs are present in the target table")
           
if __name__ == "__main__":
    unittest.main()