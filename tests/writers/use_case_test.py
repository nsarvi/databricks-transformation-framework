import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

tests_src_path = (Path(__file__).resolve().parent.parent.parent / "tests/src").as_posix()
sys.path.append(tests_src_path)

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from adm.integration.adm_table_writer import AdmTableWriter
from adm.integration.adm_table_reader import AdmTableReader
from adm.integration.utils.config_utils import ConfigUtils
from adm.integration import yaml_constants as YC
from adm.integration.adm_data_transformer import AdmDataTransformer
from datetime import datetime
import logging

class TestAdmTableWriter(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = AdmTableWriter._get_spark()
        # Enable the logger to debug level
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('adm.integration.adm_table_writer')
        self.logger.setLevel(logging.DEBUG)


    def setUp(self):
        """Creates a reusable DataFrame with 10 rows."""

        
        
    @classmethod
    def tearDownClass(self):
        """Drops the table after each test."""


    @unittest.skip("Skipping test_r2r_use_case_hist")
    def test_r2r_use_case_open_items(self):
        """Tests writing a DataFrame to a table with no schema."""
        
        config_file="tests/configs/use_case/silver_r2r_ibm_arit_daily_open_items_fact.yml"
        transformer = AdmDataTransformer(config_file)
        writer = AdmTableWriter(config_file)
       

        adm_table_reader = AdmTableReader(config_file)
        df = adm_table_reader.read_source_table("aritopen_open_item")
        target_table_id = "ibm_arit_daily_open_items_fact"
        writer.write_table(target_table_id,df)
        self.logger.info("Test R2R Use Case: DataFrame written to table successfully.")
        
        
   # @unittest.skip("Skipping test_r2r_use_case_hist")
    def test_r2r_use_case_hist(self):
        """Tests writing a DataFrame to a table with no schema."""
        
        config_file="tests/configs/use_case/silver_r2r_ibm_arit_monthly_open_items_history_fact.yml"
        transformation_id="aritopen_open_item_transformation"
        transformer = AdmDataTransformer(config_file)
        writer = AdmTableWriter(config_file)
       

        adm_table_reader = AdmTableReader(config_file)
        df = transformer.read_source_table("aritohst_open_item_month_end_hist")
        target_table_id = "ibm_arit_monthly_open_items_hist_fact"
        writer.write_table(target_table_id,df)
        self.logger.info("Test R2R Use Case: DataFrame written to table successfully.")

    
if __name__ == "__main__":
    unittest.main()