import pytest

from wilsonelser.transformation.utils.config_utils import ConfigUtils


@pytest.fixture
def environments_file(tmp_path):
    path = tmp_path / "env" / "environments.yaml"
    path.parent.mkdir()
    path.write_text(
        "dev: adb-111.1.azuredatabricks.net\n"
        "uat: https://adb-222.2.azuredatabricks.net/\n"
        "# prod: <workspace url>\n"
    )
    return str(path)


def test_current_env_matches_workspace(environments_file):
    assert ConfigUtils.current_env("adb-111.1.azuredatabricks.net", environments_file) == "dev"


def test_current_env_ignores_scheme_trailing_slash_and_case(environments_file):
    assert ConfigUtils.current_env("ADB-222.2.azuredatabricks.net", environments_file) == "uat"
    assert ConfigUtils.current_env("https://adb-111.1.azuredatabricks.net/", environments_file) == "dev"


def test_unknown_workspace_raises(environments_file):
    with pytest.raises(ValueError, match="listed exactly once"):
        ConfigUtils.current_env("adb-999.9.azuredatabricks.net", environments_file)


def test_workspace_listed_twice_raises(tmp_path):
    path = tmp_path / "environments.yaml"
    path.write_text("dev: adb-111.1.azuredatabricks.net\nuat: adb-111.1.azuredatabricks.net\n")

    with pytest.raises(ValueError, match=r"\['dev', 'uat'\]"):
        ConfigUtils.current_env("adb-111.1.azuredatabricks.net", str(path))


def test_env_config_file_is_next_to_environments_file(environments_file):
    expected = environments_file.replace("environments.yaml", "dev.yaml")

    assert ConfigUtils.env_config_file("adb-111.1.azuredatabricks.net", environments_file) == expected


def test_repo_environments_file_resolves_env_config():
    env_file = ConfigUtils.env_config_file("adb-7405613624754030.10.azuredatabricks.net")

    assert env_file == "config/env/dev.yaml"
    assert ConfigUtils.load_env_variables(env_file)["expertsierra_catalog"] == "expertsierra_dev"
