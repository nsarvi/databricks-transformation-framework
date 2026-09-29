"""Backward compatibility: the classes before TransformationEngine still accept a config path or a PipelineConfig.

The rest of the test suite uses TransformationEngine, the standard entry point; this file keeps the older
classes covered. No Spark needed.
"""
from unittest import mock

import pytest

from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from wilsonelser.transformation.data_transformer import DataTransformer
from wilsonelser.transformation.engine import TransformationEngine
from wilsonelser.transformation.pipeline_config import PipelineConfig
from wilsonelser.transformation.table_reader import TableReader
from wilsonelser.transformation.table_writer import TableWriter

CONFIG_PATH = "tests/configs/joins/joins_config.yaml"
CLASSES = [BaseDataTransformer, TableReader, DataTransformer, TableWriter, TransformationEngine]


@pytest.fixture(autouse=True)
def test_schema(monkeypatch):
    monkeypatch.setenv("catalog", "test_catalog")
    monkeypatch.setenv("schema", "test_schema")


@pytest.fixture(autouse=True)
def no_spark():
    with mock.patch("wilsonelser.transformation.base_integration.BaseIntegration._get_spark", return_value=mock.MagicMock()):
        yield


@pytest.mark.parametrize("cls", CLASSES, ids=lambda c: c.__name__)
def test_accepts_a_config_path(cls):
    instance = cls(CONFIG_PATH)

    assert "combine_id_1" in instance.combine_lookup
    assert instance.source_lookup["source_1"]["table"] == "test_catalog.test_schema.customer_mapics"


@pytest.mark.parametrize("cls", CLASSES, ids=lambda c: c.__name__)
def test_accepts_a_loaded_config_without_reloading(cls):
    config = PipelineConfig.load(CONFIG_PATH)

    with mock.patch.object(PipelineConfig, "load") as load:
        instance = cls(config)

    load.assert_not_called()
    assert instance.pipeline_config is config
