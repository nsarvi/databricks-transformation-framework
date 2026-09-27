import pytest
import yaml

from wilsonelser.transformation.utils.config_utils import ConfigUtils


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({
        "sources": [{"source_id": "s1", "table": "${catalog}.${schema}.customers"}],
        "global_config": {"base_checkpoint_location": "/Volumes/${catalog}/${schema}/checkpoints"},
    }))
    return str(path)


def test_placeholders_resolved_from_environment_variables(config_file, monkeypatch):
    monkeypatch.setenv("catalog", "dev_catalog")
    monkeypatch.setenv("schema", "bronze")

    config = ConfigUtils.load_config(config_file)

    assert config["sources"][0]["table"] == "dev_catalog.bronze.customers"
    assert config["global_config"]["base_checkpoint_location"] == "/Volumes/dev_catalog/bronze/checkpoints"


def test_env_config_file_takes_precedence_over_environment_variables(config_file, tmp_path, monkeypatch):
    monkeypatch.setenv("catalog", "from_env_var")
    monkeypatch.setenv("schema", "bronze")
    env_file = tmp_path / "env.yaml"
    env_file.write_text("catalog: from_env_file\n")

    config = ConfigUtils.load_config(config_file, str(env_file))

    assert config["sources"][0]["table"] == "from_env_file.bronze.customers"


def test_unresolved_placeholder_raises(config_file, monkeypatch):
    monkeypatch.delenv("catalog", raising=False)
    monkeypatch.delenv("schema", raising=False)

    with pytest.raises(ValueError, match=r"\$\{catalog\}"):
        ConfigUtils.load_config(config_file)


def test_config_without_placeholders_needs_no_values(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("sources:\n  - source_id: s1\n    table: main.default.customers\n")

    assert ConfigUtils.load_config(str(path))["sources"][0]["table"] == "main.default.customers"


def test_resolve_text_for_sql(monkeypatch):
    monkeypatch.setenv("catalog", "dev_catalog")

    sql = ConfigUtils.resolve_text("SELECT * FROM ${catalog}.${schema}.sales", {"schema": "silver"})

    assert sql == "SELECT * FROM dev_catalog.silver.sales"
