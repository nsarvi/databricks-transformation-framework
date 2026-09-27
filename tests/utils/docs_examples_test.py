"""Checks that every YAML example in the docs is valid, so the docs can't drift from the config format.

Each ```yaml block in README.md and docs/*.md is loaded like a real config (placeholders filled with
dummy values) and must be a mapping whose sections have the ids the framework looks up.
"""
import re
from pathlib import Path

import pytest
import yaml

from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_FILES = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").glob("*.md"))]
YAML_BLOCK = re.compile(r"```yaml\n(.*?)```", re.DOTALL)

REQUIRED_IDS = {
    YC.SOURCES_KEY: YC.SOURCE_ID_KEY,
    YC.TARGETS_KEY: YC.TABLE_ID_KEY,
    YC.TRANSFORMATIONS_KEY: YC.ID_KEY,
    YC.COMBINE_KEY: YC.COMBINE_ID_KEY,
}


def _examples():
    for doc in DOC_FILES:
        text = doc.read_text()
        for match in YAML_BLOCK.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            yield pytest.param(match.group(1), id=f"{doc.relative_to(REPO_ROOT)}:{line}")


@pytest.mark.parametrize("example", list(_examples()))
def test_doc_yaml_example_is_valid(example, monkeypatch):
    for name in set(re.findall(r"\$\{([^}]+)\}", example)):
        monkeypatch.setenv(name, f"dummy_{name}")

    config = yaml.safe_load(example)
    assert isinstance(config, dict), "example must be a YAML mapping"
    config = ConfigUtils._resolve_placeholders(config, {})

    for section, id_key in REQUIRED_IDS.items():
        for index, entry in enumerate(config.get(section) or []):
            assert isinstance(entry, dict) and id_key in entry, f"{section}[{index}] is missing '{id_key}'"
    for index, entry in enumerate(config.get(YC.COMBINE_KEY) or []):
        for kind in (YC.JOINS_KEY, YC.UNIONS_KEY):
            if kind in entry:
                assert isinstance(entry[kind], dict), f"combine[{index}].{kind} must be a mapping, not a list"
