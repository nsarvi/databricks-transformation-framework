"""Unit tests for waiting on streaming writes (wait / timeout_seconds); no Spark needed."""
from unittest import mock

import pytest

from wilsonelser.transformation.table_writer import TableWriter


def make_writer(**target_settings) -> TableWriter:
    writer = TableWriter.__new__(TableWriter)   # skip __init__: no Spark session or config file
    writer.logger = mock.MagicMock()
    writer.spark = mock.MagicMock()
    writer.spark.catalog.tableExists.return_value = True
    writer.global_config = {"base_checkpoint_location": "/Volumes/c/s/checkpoints"}
    writer.target_lookup = {"t1": {
        "table": "c.s.target", "write_type": "stream", "write_mode": "append",
        "options": {"trigger": "processingTime=10 seconds"}, **target_settings,
    }}
    return writer


def started_query(df):
    """The StreamingQuery that write_table gets back from toTable for a mocked DataFrame."""
    return df.writeStream.outputMode.return_value.options.return_value.queryName.return_value \
        .trigger.return_value.toTable.return_value


class FakeClock:
    """time.monotonic() that advances whenever the query is waited on."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def await_termination(self, seconds):
        self.now += seconds
        return False                      # still running


def test_waits_until_the_query_finishes():
    writer, df = make_writer(), mock.MagicMock()
    query = started_query(df)
    query.awaitTermination.side_effect = [False, False, True]
    query.isActive = False

    assert writer.write_table("t1", df) is None
    assert query.awaitTermination.call_count == 3
    query.stop.assert_not_called()


def test_timeout_stops_the_query_and_returns_normally():
    writer, df, clock = make_writer(timeout_seconds=70), mock.MagicMock(), FakeClock()
    query = started_query(df)
    query.awaitTermination.side_effect = clock.await_termination
    query.isActive = True

    with mock.patch("wilsonelser.transformation.table_writer.time.monotonic", clock.monotonic):
        assert writer.write_table("t1", df) is None

    waits = [call.args[0] for call in query.awaitTermination.call_args_list]
    assert waits == [30, 30, 10]          # progress every 30s, never past the 70s timeout
    query.stop.assert_called()


def test_wait_false_returns_the_running_query_without_stopping_it():
    writer, df = make_writer(wait=False), mock.MagicMock()
    query = started_query(df)
    query.isActive = True

    assert writer.write_table("t1", df) is query
    query.awaitTermination.assert_not_called()
    query.stop.assert_not_called()


def test_a_failed_query_raises_and_is_stopped():
    writer, df = make_writer(), mock.MagicMock()
    query = started_query(df)
    query.awaitTermination.side_effect = RuntimeError("stream failed")
    query.isActive = True

    with pytest.raises(RuntimeError, match="stream failed"):
        writer.write_table("t1", df)
    query.stop.assert_called_once()


@pytest.mark.parametrize("settings, message", [
    ({"timeout_seconds": 0}, "must be a positive number"),
    ({"timeout_seconds": "1h"}, "must be a positive number"),
    ({"wait": "no"}, "must be true or false"),
    ({"wait": False, "timeout_seconds": 60}, "needs 'wait: true'"),
])
def test_invalid_settings_raise_before_starting(settings, message):
    writer, df = make_writer(**settings), mock.MagicMock()

    with pytest.raises(ValueError, match=message):
        writer.write_table("t1", df)
    df.writeStream.outputMode.assert_not_called()
