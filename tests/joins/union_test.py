import sys
from pathlib import Path

src_path = (Path(__file__).resolve().parent.parent.parent / "src").as_posix()
sys.path.append(src_path)

import unittest
from unittest import mock
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from wilsonelser.transformation.data_transformer import DataTransformer
from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer

class TestDataTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """Creates a Spark session that will be used across all tests."""
        self.spark = BaseDataTransformer._get_spark()

    def setUp(self):
        """Creates reusable DataFrames for testing."""
        schema = StructType([
            StructField("sales_id", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        data1 = [
            (f"sale_{i}", f"store_1", f"product_{i}", i * 2, i * 10.5, datetime(2024, 2, 23, 12, 30, 0))
            for i in range(1, 6)  # Generates 5 rows
        ]

        data2 = [
            (f"sale_{i+5}", f"store_2", f"product_{i+5}", (i+5) * 2, (i+5) * 10.5, datetime(2024, 2, 24, 12, 30, 0))
            for i in range(1, 6)  # Generates another 5 rows
        ]

        data3 = [
            (f"sale_{i+10}", f"store_3", f"product_{i+10}", (i+10) * 2, (i+10) * 10.5, datetime(2024, 2, 25, 12, 30, 0))
            for i in range(1, 6)  # Generates another 5 rows
        ]

        data4 = [
            (f"sale_{i+15}", f"store_4", f"product_{i+15}", (i+15) * 2, (i+15) * 10.5, datetime(2024, 2, 26, 12, 30, 0))
            for i in range(1, 6)  # Generates another 5 rows
        ]

        self.df1 = self.spark.createDataFrame(data1, schema)
        self.df2 = self.spark.createDataFrame(data2, schema)
        self.df3 = self.spark.createDataFrame(data3, schema)
        self.df4 = self.spark.createDataFrame(data4, schema)

    @mock.patch("wilsonelser.transformation.data_transformer.DataTransformer.read_source_table")
    def test_apply_unions(self, mock_read_source_table):
        """Tests union logic on the DataFrames."""
        mock_read_source_table.side_effect = lambda table_id: {
            "source_1": self.df1,
            "source_2": self.df2,
            "source_3": self.df3,
            "source_4": self.df4
        }[table_id]

        transformer = DataTransformer("tests/configs/transformers/union_config.yaml")
        combine_id = "combine_id_1"

        # Mock the combine lookup
        transformer.combine_lookup = {
            combine_id: {
                YC.UNIONS_KEY: {
                    YC.SOURCE_IDS_KEY: ["source_1", "source_2", "source_3", "source_4"],
                    YC.DISTINCT_KEY: True
                }
            }
        }

        # Apply the union transformations
        union_df = transformer.apply_unions(combine_id)
        print(union_df.show())
        # Check if the union operation is correctly applied
        self.assertEqual(union_df.count(), self.df1.count() + self.df2.count() + self.df3.count() + self.df4.count())
        self.assertIn("sales_id", union_df.columns)
        self.assertIn("store_id", union_df.columns)
        self.assertIn("product_id", union_df.columns)
        self.assertIn("quantity", union_df.columns)
        self.assertIn("price", union_df.columns)
        self.assertIn("timestamp", union_df.columns)

    @mock.patch("wilsonelser.transformation.data_transformer.DataTransformer.read_source_table")
    def test_apply_unions_without_distinct(self, mock_read_source_table):
        """Tests union logic on the DataFrames without distinct."""
        mock_read_source_table.side_effect = lambda source_id: {
            "source_1": self.df1,
            "source_2": self.df2,
            "source_3": self.df3,
            "source_4": self.df4
        }[source_id]

        transformer = DataTransformer("tests/configs/transformers/union_config.yaml")
        combine_id = "combine_id_2"

        # Mock the combine lookup
        transformer.combine_lookup = {
            combine_id: {
                YC.UNIONS_KEY: {
                    YC.SOURCE_IDS_KEY: ["source_1", "source_2", "source_3", "source_4"],
                    YC.DISTINCT_KEY: False
                }
            }
        }

        # Apply the union transformations
        union_df = transformer.apply_unions(combine_id)
        print(union_df.show())
        # Check if the union operation is correctly applied
        self.assertEqual(union_df.count(), self.df1.count() + self.df2.count() + self.df3.count() + self.df4.count())
        self.assertIn("sales_id", union_df.columns)
        self.assertIn("store_id", union_df.columns)
        self.assertIn("product_id", union_df.columns)
        self.assertIn("quantity", union_df.columns)
        self.assertIn("price", union_df.columns)
        self.assertIn("timestamp", union_df.columns)

if __name__ == "__main__":
    unittest.main()