"""Unit tests for applying a target's table_properties; no Spark needed."""
from unittest import mock

from wilsonelser.transformation.table_writer import TableWriter


def make_writer(table_properties, current_properties) -> TableWriter:
    writer = TableWriter.__new__(TableWriter)   # skip __init__: no Spark session or config file
    writer.logger = mock.MagicMock()
    writer.spark = mock.MagicMock()
    writer.target_lookup = {"t1": {"table": "c.s.target", "table_properties": table_properties}}

    def sql(statement):
        result = mock.MagicMock()
        if statement.startswith("SHOW TBLPROPERTIES"):
            result.collect.return_value = [{"key": k, "value": v} for k, v in current_properties.items()]
        return result

    writer.spark.sql.side_effect = sql
    return writer


def alter_statements(writer):
    return [call.args[0] for call in writer.spark.sql.call_args_list if call.args[0].startswith("ALTER TABLE")]


def test_sets_missing_and_changed_properties_only():
    writer = make_writer(
        {"delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "true", "owner_team": "data"},
        {"delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "false"},
    )

    writer._apply_table_properties("t1")

    assert alter_statements(writer) == [
        "ALTER TABLE c.s.target SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true', 'owner_team' = 'data')"
    ]


def test_no_statement_when_properties_already_match():
    writer = make_writer({"delta.autoOptimize.optimizeWrite": "true"}, {"delta.autoOptimize.optimizeWrite": "true"})

    writer._apply_table_properties("t1")

    assert alter_statements(writer) == []


def test_no_queries_without_table_properties():
    writer = make_writer(None, {})

    writer._apply_table_properties("t1")

    writer.spark.sql.assert_not_called()


def test_yaml_booleans_become_delta_true_false():
    writer = make_writer({"delta.appendOnly": True, "delta.enableChangeDataFeed": False}, {})

    writer._apply_table_properties("t1")

    assert alter_statements(writer) == [
        "ALTER TABLE c.s.target SET TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableChangeDataFeed' = 'false')"
    ]


def test_quotes_in_values_are_escaped():
    writer = make_writer({"comment_owner": "O'Brien"}, {})

    writer._apply_table_properties("t1")

    assert alter_statements(writer) == ["ALTER TABLE c.s.target SET TBLPROPERTIES ('comment_owner' = 'O\\'Brien')"]


def test_write_table_applies_properties_after_creating_the_table():
    writer = make_writer({"delta.appendOnly": "false"}, {})
    writer.spark.catalog.tableExists.return_value = False
    writer.target_lookup["t1"].update({"write_type": "table", "write_mode": "append"})
    calls = []
    writer._create_table = mock.MagicMock(side_effect=lambda *a: calls.append("create"))
    writer._apply_table_properties = mock.MagicMock(side_effect=lambda *a: calls.append("properties"))
    writer._log_latest_operation_metrics = mock.MagicMock()

    writer.write_table("t1", mock.MagicMock())

    assert calls == ["create", "properties"]
