"""Unit test for reading a source that isn't in the config; no Spark needed."""
from unittest import mock

import pytest

from wilsonelser.transformation.table_reader import TableReader


def test_unknown_source_id_raises_naming_the_defined_sources():
    reader = TableReader.__new__(TableReader)   # skip __init__: no Spark session or config file
    reader.logger = mock.MagicMock()
    reader.spark = mock.MagicMock()
    reader.source_lookup = {"customers": {"source_id": "customers"}, "sales": {"source_id": "sales"}}

    with pytest.raises(ValueError, match="Source 'custmers' is not defined in the config's sources. Defined sources: customers, sales"):
        reader.read_source_table("custmers")
    reader.spark.table.assert_not_called()
    reader.spark.sql.assert_not_called()
