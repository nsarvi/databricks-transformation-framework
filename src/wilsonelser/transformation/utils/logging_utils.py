"""Logging for the framework.

Every framework logger lives under the `wilsonelser` namespace (the package name), so one setting controls
them all. Logging is configured automatically the first time a LoggingHandler is created. Records always
propagate to the host's logging (pytest, an app that called logging.basicConfig), and a console handler
prints them only while the host has no handlers of its own (e.g. a Databricks notebook or job). This is
decided per record, so it works whichever is configured first, and nothing is printed twice.

The level comes from the DTF_LOG_LEVEL environment variable, INFO by default. Call configure_logging()
to change level, handlers or propagation explicitly.
"""
import logging
import os
from typing import Optional


FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
)

DEFAULT_NAMESPACE = "wilsonelser"
LOG_LEVEL_ENV_VAR = "DTF_LOG_LEVEL"

_configured_namespaces: set = set()


class _ConsoleWhenHostUnconfigured(logging.StreamHandler):
    """Console handler that prints only while the root logger has no handlers, i.e. no host logging."""

    def emit(self, record: logging.LogRecord) -> None:
        if not logging.getLogger().handlers:
            super().emit(record)


def _parse_level(level: Optional[str | int]) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        val = logging.getLevelName(level.strip().upper())
        if isinstance(val, int):
            return val
    return logging.INFO


def configure_logging(
    *,
    level: Optional[str | int] = None,
    stream: Optional[object] = None,
    add_console_handler: Optional[bool] = None,
    add_file_handler: bool = False,
    file_path: Optional[str] = None,
    file_mode: str = "a",
    propagate: bool = True,
    env_var: str = LOG_LEVEL_ENV_VAR,
    namespace: str = DEFAULT_NAMESPACE,
    formatter: logging.Formatter = FORMATTER,
) -> None:
    """
    Configures the framework's namespace logger. Runs automatically the first time a LoggingHandler is
    created; call it yourself to override the defaults. Safe to call more than once.

    Parameters
    ----------
    level : str|int|None
        Log level (e.g. "DEBUG", logging.INFO). If None, read from `env_var`, else INFO.
    stream : file-like|None
        Stream for the console handler; sys.stderr if None.
    add_console_handler : bool|None
        None: attach a console handler that prints only while the host has no logging handlers.
        True: attach one that always prints. False: no console handler. At most one is attached.
    add_file_handler : bool
        Attach a file handler for `file_path` (at most one per path).
    file_path : str|None
        Path of the log file; required when add_file_handler=True.
    file_mode : str
        File mode for the file handler, default "a".
    propagate : bool
        Pass records on to the host's (root logger's) handlers; default True.
    env_var : str
        Environment variable to read the level from when `level` is None.
    namespace : str
        The logger namespace to configure (default "wilsonelser").
    formatter : logging.Formatter
        Formatter for the handlers added here.
    """
    numeric_level = _parse_level(level if level is not None else os.getenv(env_var))

    namespace_logger = logging.getLogger(namespace)
    namespace_logger.setLevel(numeric_level)
    namespace_logger.propagate = propagate

    if add_console_handler is not False and not any(getattr(h, "_dtf_console", False) for h in namespace_logger.handlers):
        handler_class = logging.StreamHandler if add_console_handler else _ConsoleWhenHostUnconfigured
        console = handler_class(stream=stream)
        console.setFormatter(formatter)
        console._dtf_console = True   # marks it so repeated calls don't add another
        namespace_logger.addHandler(console)

    if add_file_handler:
        if not file_path:
            raise ValueError("file_path is required when add_file_handler=True")
        if not any(getattr(h, "_dtf_path", None) == file_path for h in namespace_logger.handlers):
            file_handler = logging.FileHandler(file_path, mode=file_mode, encoding="utf-8")
            file_handler.setFormatter(formatter)
            file_handler._dtf_path = file_path
            namespace_logger.addHandler(file_handler)

    _configured_namespaces.add(namespace)


def set_level(level: str | int, namespace: str = DEFAULT_NAMESPACE) -> None:
    """Changes the framework log level at run time."""
    logging.getLogger(namespace).setLevel(_parse_level(level))


class LoggingHandler:
    """
    Gives framework code a logger under the framework namespace, configuring logging on first use.
    """

    def __init__(self, name: str | None = None, *, namespace: str = DEFAULT_NAMESPACE):
        """
        Args:
            name: The logger name, usually __name__. Names outside the namespace are nested under it,
                  e.g. 'column_rename_test' -> 'wilsonelser.column_rename_test'.
            namespace: Root namespace for the framework (default 'wilsonelser').
        """
        if namespace not in _configured_namespaces:
            configure_logging(namespace=namespace)

        if not name:
            fq_name = namespace
        elif name == namespace or name.startswith(f"{namespace}."):
            fq_name = name
        else:
            fq_name = f"{namespace}.{name}"

        self._logger: logging.Logger = logging.getLogger(fq_name)

    def get_logger(self) -> logging.Logger:
        """Return the underlying logger instance."""
        return self._logger
