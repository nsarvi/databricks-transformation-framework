import unittest
from decimal import Decimal

from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from use_cases.custom_transformations.common_tx import standardize_strings
from wilsonelser.transformation.base_integration import BaseIntegration
from wilsonelser.transformation.utils.logging_utils import LoggingHandler
from log_helpers import df_to_string

logger = LoggingHandler(__name__).get_logger()


class TestStandardizeStrings(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = BaseIntegration._get_spark()

    def test_standardize_strings(self):
        schema = StructType([
            StructField("client_uno", IntegerType(), True),
            StructField("client_code", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("credit_limit", DecimalType(18, 2), True),
        ])
        data = [
            (1, "A100      ", "  Acme\x00 Corp\x07 ", Decimal("10.50")),   # CHAR padding, control characters
            (2, "   ", "", Decimal("0.00")),                               # whitespace-only and empty
            (3, None, "Line1\nLine2", None),                               # NULL and a kept line feed
        ]
        result = standardize_strings(self.spark.createDataFrame(data, schema))
        logger.info("result:\n%s", df_to_string(result))

        rows = {row.client_uno: row for row in result.collect()}
        self.assertEqual(result.columns, ["client_uno", "client_code", "client_name", "credit_limit"])
        self.assertEqual(rows[1].client_code, "A100")
        self.assertEqual(rows[1].client_name, "Acme Corp")
        self.assertIsNone(rows[2].client_code)
        self.assertIsNone(rows[2].client_name)
        self.assertIsNone(rows[3].client_code)
        self.assertEqual(rows[3].client_name, "Line1\nLine2")
        self.assertEqual(rows[1].credit_limit, Decimal("10.50"))
        self.assertEqual(dict(result.dtypes)["credit_limit"], "decimal(18,2)")


if __name__ == "__main__":
    unittest.main()
