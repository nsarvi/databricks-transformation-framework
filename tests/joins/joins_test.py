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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from wilsonelser.transformation.data_transformer import DataTransformer
from wilsonelser.transformation.table_writer import TableWriter
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
import logging

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = BaseDataTransformer._get_spark()

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

        # Write the DataFrame to the table using TableWriter
        writer = TableWriter("tests/configs/joins/source_config.yaml")
        writer.write_table("source_customer_mapics",self.customer_df)

        # Create test data for adm_sales_mapics table
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
            ("trans_4", "cust_4", "prod_4", "store_1", 15, 50.0, 7.5, 42.5, datetime(2025, 3, 15, 12, 0, 0), "Credit Card"),
            ("trans_5", "cust_5", "prod_5", "store_2", 25, 60.0, 12.0, 48.0, datetime(2025, 3, 15, 12, 0, 0), "Debit Card"),
            ("trans_6", "cust_6", "prod_6", "store_3", 35, 40.0, 8.0, 32.0, datetime(2025, 3, 15, 12, 0, 0), "Cash"),
            ("trans_7", "cust_7", "prod_7", "store_1", 20, 55.0, 11.0, 44.0, datetime(2025, 3, 15, 12, 0, 0), "Credit Card"),
            ("trans_8", "cust_8", "prod_8", "store_2", 30, 65.0, 13.0, 52.0, datetime(2025, 3, 15, 12, 0, 0), "Debit Card"),
            ("trans_9", "cust_9", "prod_9", "store_3", 40, 35.0, 7.0, 28.0, datetime(2025, 3, 15, 12, 0, 0), "Cash"),
            ("trans_10", "cust_10", "prod_10", "store_1", 50, 45.0, 9.0, 36.0, datetime(2025, 3, 15, 12, 0, 0), "Credit Card")
        ]
        self.sales_df = self.spark.createDataFrame(sales_data, sales_schema)

        # Write the DataFrame to the table using TableWriter
        writer.write_table("source_sales_mapics", self.sales_df)

    @classmethod
    def tearDownClass(cls):
        """Stops the Spark session after all tests."""
      
   
    def test_apply_joins(self):
        """Tests the apply_joins method."""
        transformer = DataTransformer("tests/configs/joins/joins_config.yaml")
        result_df = transformer.apply_joins("combine_id_1")
        
         # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('wilsonelser.transformation.table_writer')
        logger.setLevel(logging.DEBUG)
        logger.info(self.customer_df.show())
        logger.info(self.sales_df.show())
        logger.info(result_df.show())
        
        # Check if cust_1 and cust_3 are in the result set
        result_customer_ids = [row.transaction_id for row in result_df.select("transaction_id").distinct().collect()]
        logger.info(result_customer_ids)
        self.assertIn("trans_1", result_customer_ids, "trans_1 is not in the result set")
        self.assertIn("trans_3", result_customer_ids, "trans_3 is not in the result set")

        print(result_df.show())
    
   #@unittest.skip("Its working, skip for now")
    def test_apply_joins_with_and_conditions(self):
        """Tests the apply_joins method with and conditions."""
        transformer = DataTransformer("tests/configs/joins/joins_config.yaml")
        result_df = transformer.apply_joins("combine_with_and_condition")
        
         # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('wilsonelser.transformation.table_writer')
        logger.setLevel(logging.DEBUG)
        logger.info(self.customer_df.show())
        logger.info(self.sales_df.show())
        logger.info(result_df.show())
        
        # Check if cust_1 and cust_3 are in the result set
        result_customer_ids = [row.transaction_id for row in result_df.select("transaction_id").distinct().collect()]
   
        self.assertIn("trans_1", result_customer_ids, "trans_1 is not in the result set")
        self.assertIn("trans_3", result_customer_ids, "trans_3 is not in the result set")

    
        
if __name__ == "__main__":
    unittest.main()