"""Legacy test for the P2C sales order invoice pipeline from the old ADM workspace.

Kept for reference only: it reads client tables (sandbox.spoadm_*) that don't exist in the
Wilson-Elser workspace. pytest ignores the tests/legacy folder (see tests/conftest.py).
"""
import unittest

import pytest
from log_helpers import df_to_string
from wilsonelser.transformation.data_transformer import DataTransformer
from wilsonelser.transformation.utils.logging_utils import LoggingHandler

logger = LoggingHandler(__name__).get_logger()
pytestmark = pytest.mark.integration


class TestP2CSalesOrderInvoice(unittest.TestCase):

    def test_apply_p2c_sales_order_invoice(self):
        """Tests the apply_joins method on the P2C sales order invoice config."""
        transformer = DataTransformer("tests/legacy/configs/joins/sales_order_invoice.yml")
        result_df = transformer.apply_joins("combine_1")

        logger.info("result_df:\n%s", df_to_string(result_df))


if __name__ == "__main__":
    unittest.main()
