"""Unit tests for the framework's logging setup; each test uses its own namespace so they don't interfere."""
import io
import logging
import uuid

from unittest import mock

import pytest

from wilsonelser.transformation.utils import logging_utils
from wilsonelser.transformation.utils.logging_utils import LoggingHandler, configure_logging


@pytest.fixture
def namespace():
    name = f"dtf_test_{uuid.uuid4().hex[:8]}"
    yield name
    logging.getLogger(name).handlers.clear()
    logging_utils._configured_namespaces.discard(name)


def no_root_handlers():
    """Simulates a host that hasn't configured logging, like a Databricks notebook.

    Used inside the test body: pytest attaches its own root handlers when each test starts.
    """
    return mock.patch.object(logging.getLogger(), "handlers", [])


def test_logger_names_are_nested_under_the_namespace(namespace):
    assert LoggingHandler("column_rename_test", namespace=namespace).get_logger().name == f"{namespace}.column_rename_test"
    assert LoggingHandler(f"{namespace}.transformation.x", namespace=namespace).get_logger().name == f"{namespace}.transformation.x"
    assert LoggingHandler(f"{namespace}_other", namespace=namespace).get_logger().name == f"{namespace}.{namespace}_other"


def test_framework_module_names_are_kept_as_is():
    assert LoggingHandler("wilsonelser.transformation.table_writer").get_logger().name == "wilsonelser.transformation.table_writer"


def test_without_host_logging_the_console_handler_prints(namespace, monkeypatch):
    monkeypatch.delenv("DTF_LOG_LEVEL", raising=False)
    stream = io.StringIO()
    configure_logging(namespace=namespace, stream=stream)

    with no_root_handlers():
        logging.getLogger(f"{namespace}.module").info("loaded 10 rows")

    assert "loaded 10 rows" in stream.getvalue()


def test_with_host_logging_records_go_to_the_host_only(namespace):
    stream = io.StringIO()
    configure_logging(namespace=namespace, stream=stream)
    host = mock.MagicMock(spec=logging.Handler, level=logging.NOTSET)

    # Simulates a host that configured logging, like pytest or an app calling logging.basicConfig
    with mock.patch.object(logging.getLogger(), "handlers", [host]):
        logging.getLogger(f"{namespace}.module").info("loaded 10 rows")

    assert host.handle.call_args.args[0].getMessage() == "loaded 10 rows"
    assert stream.getvalue() == ""                  # not printed twice
    assert logging.getLogger(namespace).propagate is True


def test_host_logging_configured_after_the_framework_still_receives_records(namespace):
    """The framework is often imported before the host sets up logging (e.g. pytest's conftest)."""
    with no_root_handlers():
        configure_logging(namespace=namespace)
    host = mock.MagicMock(spec=logging.Handler, level=logging.NOTSET)

    with mock.patch.object(logging.getLogger(), "handlers", [host]):
        logging.getLogger(f"{namespace}.module").info("after host setup")

    assert host.handle.call_args.args[0].getMessage() == "after host setup"


def test_first_logging_handler_configures_logging(namespace):
    with no_root_handlers():
        LoggingHandler("module", namespace=namespace)

    assert len(logging.getLogger(namespace).handlers) == 1


def test_configuring_twice_adds_one_console_handler(namespace):
    with no_root_handlers():
        configure_logging(namespace=namespace)
        configure_logging(namespace=namespace)

    assert len(logging.getLogger(namespace).handlers) == 1


@pytest.mark.parametrize("env_value, expected", [("DEBUG", logging.DEBUG), ("warning", logging.WARNING), ("nonsense", logging.INFO)])
def test_level_comes_from_dtf_log_level(namespace, monkeypatch, env_value, expected):
    monkeypatch.setenv("DTF_LOG_LEVEL", env_value)
    configure_logging(namespace=namespace)

    assert logging.getLogger(namespace).level == expected


def test_explicit_level_overrides_the_environment(namespace, monkeypatch):
    monkeypatch.setenv("DTF_LOG_LEVEL", "DEBUG")
    configure_logging(namespace=namespace, level="ERROR")

    assert logging.getLogger(namespace).level == logging.ERROR
