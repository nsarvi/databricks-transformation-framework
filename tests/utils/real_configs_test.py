"""Parse-only checks for the real pipeline and environment configs.

These tests never read data: they load every config in use_cases/ and config/env/ with dummy values
for ${...} placeholders, so YAML errors and missing ids are caught before a job runs.
Backup folders (bkp/) are not real pipelines and are skipped.
"""
import re
from pathlib import Path

import pytest
import yaml

from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_CONFIGS = sorted(p for p in (REPO_ROOT / "use_cases").rglob("*.yml") if "bkp" not in p.parts)
ENV_CONFIGS = sorted((REPO_ROOT / "config" / "env").glob("*.yaml"))

# Each config section and the id key the framework's lookups require on every entry
REQUIRED_IDS = {
    YC.SOURCES_KEY: YC.SOURCE_ID_KEY,
    YC.TARGETS_KEY: YC.TABLE_ID_KEY,
    YC.TRANSFORMATIONS_KEY: YC.ID_KEY,
    YC.COMBINE_KEY: YC.COMBINE_ID_KEY,
}


def _relative(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


@pytest.mark.parametrize("config_path", PIPELINE_CONFIGS, ids=_relative)
def test_pipeline_config_loads(config_path, monkeypatch):
    for name in set(re.findall(r"\$\{([^}]+)\}", config_path.read_text())):
        monkeypatch.setenv(name, f"dummy_{name}")

    config = ConfigUtils.load_config(str(config_path))

    assert isinstance(config, dict), "config must be a YAML mapping"
    for section, id_key in REQUIRED_IDS.items():
        for index, entry in enumerate(config.get(section) or []):
            assert isinstance(entry, dict) and id_key in entry, f"{section}[{index}] is missing '{id_key}'"


@pytest.mark.parametrize("env_path", ENV_CONFIGS, ids=_relative)
def test_env_config_is_flat_mapping(env_path):
    values = yaml.safe_load(env_path.read_text())

    assert isinstance(values, dict), "env config must be a YAML mapping"
    for key, value in values.items():
        assert not isinstance(value, (dict, list)), f"'{key}' must be a single value to fill ${{{key}}}"
