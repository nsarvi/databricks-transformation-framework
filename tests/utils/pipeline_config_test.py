"""Unit tests for PipelineConfig and TransformationEngine construction; no Spark needed."""
from unittest import mock

import pytest
import yaml

from wilsonelser.transformation.engine import TransformationEngine
from wilsonelser.transformation.pipeline_config import PipelineConfig
from wilsonelser.transformation.utils.config_utils import ConfigUtils

SOURCES = {"sources": [
    {"source_id": "customers", "table": "${catalog}.bronze.customers", "transformation_id": "clean"},
    {"source_id": "sales", "table": "${catalog}.bronze.sales"},
]}
TRANSFORMATIONS = {"transformations": [{"id": "clean", "filters": [{"condition": "active = 1"}]}]}
COMBINES = {"combine": [{"combine_id": "customer_sales", "joins": {"source_ids": [
    {"source_id": "customers", "alias": "c", "join_order": 1},
    {"source_id": "sales", "alias": "s", "join_order": 2},
]}}]}
TARGETS = {
    "global_config": {"base_checkpoint_location": "/Volumes/${catalog}/silver/checkpoints"},
    "targets": [{"table_id": "silver_sales", "table": "${catalog}.silver.sales", "source_type": "combine", "source_id": "customer_sales"}],
}


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content, sort_keys=False))
    return path


@pytest.fixture(autouse=True)
def catalog(monkeypatch):
    monkeypatch.setenv("catalog", "dev_sales")


def test_load_from_a_single_file(tmp_path):
    path = write(tmp_path / "pipeline.yml", {**SOURCES, **TRANSFORMATIONS, **COMBINES, **TARGETS})

    config = PipelineConfig.load(str(path))

    assert config.source("customers")["table"] == "dev_sales.bronze.customers"
    assert config.target("silver_sales")["source_id"] == "customer_sales"
    assert config.global_config["base_checkpoint_location"] == "/Volumes/dev_sales/silver/checkpoints"


def test_load_from_a_folder_merges_its_files(tmp_path):
    write(tmp_path / "pipeline" / "01_sources.yml", SOURCES)
    write(tmp_path / "pipeline" / "02_transformations.yaml", TRANSFORMATIONS)
    write(tmp_path / "pipeline" / "joins" / "customer_sales.yml", COMBINES)
    write(tmp_path / "pipeline" / "targets.yml", TARGETS)
    (tmp_path / "pipeline" / "notes.txt").write_text("not a config")

    config = PipelineConfig.load(str(tmp_path / "pipeline"))

    assert sorted(config.sources) == ["customers", "sales"]
    assert list(config.combines) == ["customer_sales"]
    assert config.target("silver_sales")["table"] == "dev_sales.silver.sales"


def test_same_id_in_two_files_raises(tmp_path):
    write(tmp_path / "pipeline" / "a.yml", SOURCES)
    write(tmp_path / "pipeline" / "b.yml", {"sources": [{"source_id": "sales", "table": "x.y.z"}]})

    with pytest.raises(ValueError, match="more than one entry with source_id 'sales'"):
        PipelineConfig.load(str(tmp_path / "pipeline"))


def test_setting_the_same_global_key_in_two_files_raises(tmp_path):
    write(tmp_path / "pipeline" / "a.yml", {"global_config": {"query_name_prefix": "a"}})
    write(tmp_path / "pipeline" / "b.yml", {"global_config": {"query_name_prefix": "b"}})

    with pytest.raises(ValueError, match="global_config' sets query_name_prefix in more than one file"):
        PipelineConfig.load(str(tmp_path / "pipeline"))


def test_empty_folder_raises(tmp_path):
    (tmp_path / "pipeline").mkdir()

    with pytest.raises(ValueError, match="No YAML files found"):
        PipelineConfig.load(str(tmp_path / "pipeline"))


@pytest.mark.parametrize("data, message", [
    ({"sources": [{"table": "x.y.z"}]}, r"sources\[0\] is missing 'source_id'"),
    ({"sources": [{"source_id": "a", "transformation_id": "missing"}]}, "unknown transformation_id 'missing'"),
    ({**SOURCES, **TRANSFORMATIONS, "combine": [{"combine_id": "c", "unions": {"source_ids": ["customers", "orders"]}}]},
     "combine 'c' uses unknown input 'orders'"),
    ({**SOURCES, **TRANSFORMATIONS, "targets": [{"table_id": "t", "source_type": "combine", "source_id": "nope"}]},
     "target 't' uses unknown combine id 'nope'"),
    ({**SOURCES, **TRANSFORMATIONS, "targets": [{"table_id": "t", "source_type": "table", "source_id": "sales"}]},
     "target 't' has source_type 'table'"),
    ({**SOURCES, **TRANSFORMATIONS, "combine": [{"combine_id": "sales", "unions": {"source_ids": ["customers"]}}]},
     "ids used as both a source_id and a combine_id: sales"),
])
def test_invalid_configs_fail_at_load(data, message):
    with pytest.raises(ValueError, match=message):
        PipelineConfig.from_dict(data)


def test_all_problems_are_reported_together():
    data = {"sources": [{"source_id": "a", "transformation_id": "x"}, {"source_id": "b", "transformation_id": "y"}]}

    with pytest.raises(ValueError) as error:
        PipelineConfig.from_dict(data)
    assert "'x'" in str(error.value) and "'y'" in str(error.value)


def test_unknown_id_lookup_names_the_defined_ids():
    config = PipelineConfig.from_dict({**SOURCES, **TRANSFORMATIONS})

    with pytest.raises(ValueError, match="'orders' is not defined in sources.*Defined: customers, sales"):
        config.source("orders")


def test_auto_env_uses_the_current_workspace(tmp_path, monkeypatch):
    env_dir = tmp_path / "env"
    write(env_dir / "environments.yaml", {"dev": "adb-111.1.azuredatabricks.net"})
    write(env_dir / "dev.yaml", {"catalog": "from_dev_env_file"})
    monkeypatch.setattr(ConfigUtils, "ENVIRONMENTS_FILE", str(env_dir / "environments.yaml"))
    monkeypatch.setattr(ConfigUtils, "_current_workspace_url", staticmethod(lambda: "adb-111.1.azuredatabricks.net"))

    config = PipelineConfig.from_dict({**SOURCES, **TRANSFORMATIONS}, env_config_file="auto")

    assert config.source("customers")["table"] == "from_dev_env_file.bronze.customers"


def test_engine_accepts_a_loaded_config_without_reloading():
    config = PipelineConfig.from_dict({**SOURCES, **TRANSFORMATIONS, **COMBINES, **TARGETS})

    with mock.patch.object(TransformationEngine, "_get_spark", return_value=mock.MagicMock()), \
            mock.patch.object(PipelineConfig, "load") as load:
        engine = TransformationEngine(config)

    load.assert_not_called()
    assert engine.pipeline_config is config
    assert engine.target_lookup["silver_sales"]["table"] == "dev_sales.silver.sales"
