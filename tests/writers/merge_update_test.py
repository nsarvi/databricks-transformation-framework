"""Unit tests for parsing merge `update` entries; no Spark needed."""
from unittest import mock

import pytest

from wilsonelser.transformation.table_writer import TableWriter


@pytest.mark.parametrize("entry, expected", [
    ("name = source.name", ("name", "source.name")),
    ("name=source.name", ("name", "source.name")),
    ("  name   =   source.name  ", ("name", "source.name")),
    ("flag = CASE WHEN source.x = 1 THEN 'Y' ELSE 'N' END", ("flag", "CASE WHEN source.x = 1 THEN 'Y' ELSE 'N' END")),
    ("is_same = source.a == source.b", ("is_same", "source.a == source.b")),
    ("`order id` = source.`order id`", ("`order id`", "source.`order id`")),
])
def test_update_entries_split_on_the_first_equals(entry, expected):
    assert TableWriter._parse_update_assignment(entry) == expected


@pytest.mark.parametrize("entry", ["name source.name", "= source.name", "name =", "   "])
def test_malformed_update_entries_raise(entry):
    with pytest.raises(ValueError, match="must look like '<column> = <expression>'"):
        TableWriter._parse_update_assignment(entry)


def test_merge_passes_parsed_updates_to_delta():
    writer = TableWriter.__new__(TableWriter)   # skip __init__: no Spark session or config file
    writer.logger = mock.MagicMock()
    writer.spark = mock.MagicMock()
    writer.target_lookup = {"t1": {
        "table": "c.s.target",
        "merge_condition": "target.id = source.id",
        "merge_actions": {"when_matched": {"update": ["name=source.name", "flag = CASE WHEN source.x = 1 THEN 'Y' END"]}},
    }}

    with mock.patch("wilsonelser.transformation.table_writer.DeltaTable") as delta_table, \
            mock.patch("wilsonelser.transformation.table_writer.F") as functions:
        functions.expr.side_effect = lambda expression: f"expr({expression})"
        writer.merge_into_target("t1", mock.MagicMock())

    merge_builder = delta_table.forName.return_value.alias.return_value.merge.return_value
    merge_builder.whenMatchedUpdate.assert_called_once_with(set={
        "name": "expr(source.name)",
        "flag": "expr(CASE WHEN source.x = 1 THEN 'Y' END)",
    })
