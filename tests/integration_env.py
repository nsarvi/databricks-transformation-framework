import os


def table_name(name: str) -> str:
    """Returns the fully qualified name of a test table in the schema created by tests/conftest.py."""
    return f"{os.environ['catalog']}.{os.environ['schema']}.{name}"
