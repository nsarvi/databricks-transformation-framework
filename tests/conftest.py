"""Shared pytest setup for integration tests.

Integration tests need a Unity Catalog catalog to write to. Set it with environment variables:

    TEST_CATALOG   catalog to create the test schema in (required; integration tests are skipped without it)
    TEST_SCHEMA    schema to use (optional); when set, it is kept after the run so the tables can be inspected.
                   When not set, a schema named dtf_test_<random> is created and dropped at the end of the run.

The catalog and schema are exposed as the `catalog` and `schema` environment variables, which the
${catalog}/${schema} placeholders in tests/configs resolve to.
"""
import os
import uuid

import pytest

from wilsonelser.transformation.base_integration import BaseIntegration
from wilsonelser.transformation.utils.logging_utils import LoggingHandler

logger = LoggingHandler(__name__).get_logger()

# Old client-data tests kept for reference; they need tables from workspace
collect_ignore = ["legacy"]


def pytest_collection_modifyitems(config, items):
    if os.environ.get("TEST_CATALOG"):
        return
    skip = pytest.mark.skip(reason="Set TEST_CATALOG to a catalog you can create schemas in to run integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def integration_schema(request):
    """Creates the test schema and checkpoint volume once per run, when integration tests are selected."""
    catalog = os.environ.get("TEST_CATALOG")
    if not catalog or not any("integration" in item.keywords for item in request.session.items):
        yield None
        return

    keep_schema = bool(os.environ.get("TEST_SCHEMA"))
    schema = os.environ.get("TEST_SCHEMA") or f"dtf_test_{uuid.uuid4().hex[:8]}"
    spark = BaseIntegration._get_spark()

    logger.info("Creating test schema %s.%s", catalog, schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.checkpoints")
    os.environ["catalog"] = catalog
    os.environ["schema"] = schema

    yield catalog, schema

    if keep_schema:
        logger.info("Keeping test schema %s.%s (TEST_SCHEMA is set)", catalog, schema)
    else:
        logger.info("Dropping test schema %s.%s", catalog, schema)
        spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
