"""Parse-only checks for the real pipeline and environment configs.

These tests never read data: they load every config in use_cases/ and config/env/ with dummy values
for ${...} placeholders, so YAML errors and missing ids are caught before a job runs.
Backup folders (bkp/), bundle job definitions (resources/) and DQ table and column files (config/tables/) are
not pipeline configs; DQ table files get their own structural test.
"""
import re
from pathlib import Path

import pytest
import yaml

from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils

REPO_ROOT = Path(__file__).resolve().parents[2]
# DQ table files (config/tables/<table>.yml) list their column files (config/tables/<table>/<column>.yml),
# each with the column's rules and every rule with a DQX check
DQ_TABLE_DIRS = sorted({p.parent for p in (REPO_ROOT / "use_cases").rglob("config/tables/*.yml")})
PIPELINE_CONFIGS = sorted(
    p for p in (REPO_ROOT / "use_cases").rglob("*.yml")
    if not {"bkp", "resources"} & set(p.parts) and not any(d == p.parent or d in p.parents for d in DQ_TABLE_DIRS)
)
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


def _load_dq_table(path: Path) -> dict:
    """A table file with its listed column files read in, as the QBE notebook does."""
    table = yaml.safe_load(path.read_text())
    column_files = table.get("columns") or []
    assert isinstance(column_files, list), f"{path.name}: columns must list column files"
    columns = []
    for column_file in column_files:
        column_path = path.parent / column_file
        assert column_path.is_file(), f"{path.name}: column file {column_file} not found"
        column = yaml.safe_load(column_path.read_text())
        assert column.get("column"), f"{column_file}: needs column"
        columns.append(column)
    names = [column["column"] for column in columns]
    assert len(names) == len(set(names)), f"{path.name}: a column is in more than one column file"
    return {**table, "columns": columns}


def _table_rules(table: dict) -> list:
    rules = [rule for column in table["columns"] for rule in column.get("rules") or []]
    return rules + list(table.get("table_rules") or [])


@pytest.mark.parametrize("tables_dir", DQ_TABLE_DIRS, ids=_relative)
def test_dq_table_files_are_well_formed(tables_dir):
    tables = {path.name: _load_dq_table(path) for path in sorted(tables_dir.glob("*.yml"))}

    rule_ids = [rule.get("rule_id") for table in tables.values() for rule in _table_rules(table)]
    assert all(rule_ids), "every rule needs a rule_id"
    assert len(rule_ids) == len(set(rule_ids)), "rule_ids must be unique across all table files"
    for name, table in tables.items():
        assert table.get("table"), f"{name}: needs table"
        assert "source_table" not in table, f"{name}: source_table was removed; set only table"
        assert table.get("columns") or table.get("table_rules"), f"{name}: needs columns or table_rules"
        for rule in _table_rules(table):
            rule_id = rule["rule_id"]
            assert (rule.get("check") or {}).get("function"), f"{rule_id}: needs check.function"
            assert rule.get("criticality", "error") in ("error", "warn"), f"{rule_id}: criticality must be error or warn"
            for key in ("minimum_threshold", "target_threshold"):
                value = rule.get(key)
                assert value is None or (isinstance(value, (int, float)) and 0 <= value <= 100), f"{rule_id}: {key} must be 0-100"
            # DQX reads a bare string in a value list as a column name; text values must be quoted, e.g. "'OPEN'"
            if rule["check"]["function"] in ("is_in_list", "is_not_in_list", "is_not_null_and_is_in_list"):
                for value in rule["check"].get("arguments", {}).get("allowed", []):
                    if isinstance(value, str):
                        assert value.startswith("'") and value.endswith("'"), f"{rule_id}: quote text values in allowed, e.g. \"'{value}'\""
