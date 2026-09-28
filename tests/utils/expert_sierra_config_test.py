"""Checks the Expert Sierra Bronze config resolves and its table list is valid, without reading data."""
import yaml

from wilsonelser.transformation.utils.config_utils import ConfigUtils

LOAD_CONFIG = "use_cases/expert_sierra/config/bronze_load.yml"


def _settings(env: str = "dev") -> dict:
    return ConfigUtils.load_config(LOAD_CONFIG, f"config/env/{env}.yaml")["bronze_load"]


def test_bronze_load_settings_resolve_for_every_environment():
    for env in ("dev", "uat", "prod"):
        settings = _settings(env)

        assert settings["catalog"] == f"expertsierra_{env}"
        assert settings["checkpoint_location"] == f"/Volumes/expertsierra_{env}/bronze/checkpoints"


def test_bronze_tables_are_valid():
    tables = ConfigUtils.load_config(_settings()["tables_config"])["tables"]

    names = [table["name"] for table in tables]
    assert names, "no tables configured"
    assert len(names) == len(set(names)), "duplicate table names"
    for table in tables:
        assert table.get("load_mode") in ("full", "incremental"), f"{table['name']}: invalid load_mode"
        assert isinstance(table.get("enabled", True), bool), f"{table['name']}: enabled must be true or false"


def test_bronze_transformation_exists():
    settings = _settings()
    # Read as plain YAML: the transformation config's ${job_run_id} is only set by the notebook at run time
    with open(ConfigUtils._resolve_path(settings["transformation_config"])) as f:
        transformation_ids = {t["id"] for t in yaml.safe_load(f)["transformations"]}

    assert settings["transformation_id"] in transformation_ids
