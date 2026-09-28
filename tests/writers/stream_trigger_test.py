"""Unit tests for streaming trigger handling in TableWriter; no Spark needed."""
from unittest import mock

import pytest

from wilsonelser.transformation.table_writer import TableWriter


def make_writer(target_options=None) -> TableWriter:
    writer = TableWriter.__new__(TableWriter)   # skip __init__: no Spark session or config file
    writer.logger = mock.MagicMock()
    writer.spark = mock.MagicMock()
    writer.spark.catalog.tableExists.return_value = True
    writer.global_config = {"base_checkpoint_location": "/Volumes/c/s/checkpoints", "query_name_prefix": "q"}
    writer.target_lookup = {"t1": {
        "table": "c.s.target", "write_type": "stream", "write_mode": "append",
        "options": target_options if target_options is not None else {},
    }}
    return writer


@pytest.mark.parametrize("trigger_option, expected", [
    (None, {"availableNow": True}),
    ("availableNow", {"availableNow": True}),
    ("once", {"availableNow": True}),
    ("processingTime=10 seconds", {"processingTime": "10 seconds"}),
    ("continuous=1 second", {"continuous": "1 second"}),
])
def test_trigger_options(trigger_option, expected):
    stream_writer = mock.MagicMock()

    make_writer()._apply_trigger(stream_writer, trigger_option)

    stream_writer.trigger.assert_called_once_with(**expected)


def test_once_logs_a_deprecation_warning():
    writer = make_writer()

    writer._apply_trigger(mock.MagicMock(), "once")

    assert "deprecated" in writer.logger.warning.call_args.args[0]


def test_unknown_trigger_raises():
    with pytest.raises(ValueError, match="Unsupported trigger 'every 5 minutes'"):
        make_writer()._apply_trigger(mock.MagicMock(), "every 5 minutes")


def test_stream_write_keeps_config_and_does_not_pass_trigger_as_option():
    target_options = {"trigger": "availableNow", "mergeSchema": "true"}
    writer = make_writer(target_options)
    df = mock.MagicMock()
    stream_writer = df.writeStream.outputMode.return_value
    query = stream_writer.options.return_value.queryName.return_value.trigger.return_value.toTable.return_value
    query.isActive = False

    writer.write_table("t1", df)

    stream_writer.options.assert_called_once_with(mergeSchema="true", checkpointLocation="/Volumes/c/s/checkpoints/t1")
    assert target_options == {"trigger": "availableNow", "mergeSchema": "true"}     # config unchanged
