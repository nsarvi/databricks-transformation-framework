import os
import yaml
import re
import json
import sys
from pathlib import Path
from typing import Dict, Optional
from wilsonelser.transformation import yaml_constants as YC
from pyspark.sql.types import StructType
from wilsonelser.transformation.utils.logging_utils import LoggingHandler

logger = LoggingHandler(__name__).get_logger()

class ConfigUtils:
    """Utility class for handling configuration and schema loading."""


    PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
    # Maps each environment to its Databricks workspace; env config files sit next to it as <env>.yaml
    ENVIRONMENTS_FILE = "config/env/environments.yaml"

    @staticmethod
    def _normalize_workspace_url(url: str) -> str:
        return re.sub(r"^https?://", "", str(url).strip()).rstrip("/").lower()

    @staticmethod
    def _current_workspace_url() -> str:
        # Imported here: base_integration imports this module
        from wilsonelser.transformation.base_integration import BaseIntegration
        return BaseIntegration._get_spark().conf.get("spark.databricks.workspaceUrl")

    @staticmethod
    def current_env(workspace_url: Optional[str] = None, environments_file: Optional[str] = None) -> str:
        """Returns the environment (e.g. "dev") whose workspace is the current one.

        Looks up the workspace URL, from the Spark session unless given, in the environments file
        (one `<env>: <workspace url>` line per environment).
        """
        environments_file = environments_file or ConfigUtils.ENVIRONMENTS_FILE
        workspace_url = workspace_url or ConfigUtils._current_workspace_url()
        with open(ConfigUtils._resolve_path(environments_file)) as f:
            environments = yaml.safe_load(f) or {}
        workspace = ConfigUtils._normalize_workspace_url(workspace_url)
        matches = [env for env, url in environments.items() if url and ConfigUtils._normalize_workspace_url(url) == workspace]
        if len(matches) != 1:
            raise ValueError(
                f"Workspace {workspace_url} must be listed exactly once in {environments_file}, found: {matches}"
            )
        return matches[0]

    @staticmethod
    def env_config_file(workspace_url: Optional[str] = None, environments_file: Optional[str] = None) -> str:
        """Returns the env config file for the current workspace, e.g. "config/env/dev.yaml"."""
        environments_file = environments_file or ConfigUtils.ENVIRONMENTS_FILE
        env = ConfigUtils.current_env(workspace_url, environments_file)
        return str(Path(environments_file).parent / f"{env}.yaml")

    @staticmethod
    def _lookup_placeholder(name: str, env_vars: dict) -> str:
        """Returns the value for a ${name} placeholder: the env config file first, then environment variables."""
        if name in env_vars and env_vars[name] is not None:
            return str(env_vars[name])
        if name in os.environ:
            return os.environ[name]
        raise ValueError(
            f"No value for placeholder '${{{name}}}'. Set it in the env config file or as an environment variable."
        )

    @staticmethod
    def resolve_text(text: str, env_vars: Optional[dict] = None) -> str:
        """Replaces ${name} placeholders in a string, e.g. the contents of a SQL file."""
        env_vars = env_vars or {}
        return ConfigUtils.PLACEHOLDER_PATTERN.sub(
            lambda m: ConfigUtils._lookup_placeholder(m.group(1), env_vars), text
        )

    @staticmethod
    def _resolve_placeholders(config: dict, env_vars: dict) -> dict:
        """Recursively replace ${name} placeholders with values from the env config file or environment variables."""

        def substitute(value):
            if isinstance(value, str):
                return ConfigUtils.resolve_text(value, env_vars)
            elif isinstance(value, dict):
                return {k: substitute(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute(v) for v in value]
            return value

        if not isinstance(config, dict):
            raise TypeError("Input config must be a dictionary.")
        resolved_config = substitute(config)
        if not isinstance(resolved_config, dict):
            raise TypeError("Resolved config must be a dictionary.")
        return resolved_config


    @staticmethod
    def load_env_variables(env_config_file: Optional[str] = None) -> Dict:
        """Loads placeholder values from the env config file; returns an empty dict when none is given."""
        if not env_config_file:
            return {}

        env_file_path = ConfigUtils._resolve_path(env_config_file)

        with open(env_file_path, "r") as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _has_placeholders(obj) -> bool:
        """Recursively check for ${var} style placeholders in config values."""
        if isinstance(obj, str):
            return bool(re.search(r"\$\{[^}]+\}", obj))  # only matches ${...}
        elif isinstance(obj, dict):
            return any(ConfigUtils._has_placeholders(v) for v in obj.values())
        elif isinstance(obj, list):
            return any(ConfigUtils._has_placeholders(v) for v in obj)
        return False

    @staticmethod
    def _resolve_path(path_str: str) -> Path:
        """
        Resolve a file path by checking:
        1. If absolute and exists, return it.
        2. check in each directory in sys.path.
        3. check in current working directory.
        4. If not found, raise FileNotFoundError with attempted paths.
        """
        attempted_paths = []

        # 1. Absolute path check
        p = Path(path_str)
        if p.is_absolute() and p.exists():
            logger.debug(f"Found config file at absolute path: {p}")
            return p
        attempted_paths.append(str(p))

        # 2. Check in sys.path directories
        for dir_path in sys.path:
            candidate = Path(dir_path) / path_str
            attempted_paths.append(str(candidate))
            if candidate.exists():
                logger.debug(f"Found config file in sys.path: {candidate}")
                return candidate

        # 3. Check in current working directory
        cwd_candidate = Path.cwd() / path_str
        attempted_paths.append(str(cwd_candidate))
        if cwd_candidate.exists():
            logger.debug(f"Found config file in current working directory: {cwd_candidate}")
            return cwd_candidate

        # If none found, log all attempts and raise error
        error_msg = (
            f"Config file '{path_str}' not found. Attempted paths:\n" +
            "\n".join(f"  - {p}" for p in attempted_paths)
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)


    @staticmethod
    def load_config(config_file: str, env_config_file: Optional[str] = None) -> Dict:
        config_path = ConfigUtils._resolve_path(config_file)

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        if ConfigUtils._has_placeholders(config):
            env_vars = ConfigUtils.load_env_variables(env_config_file)
            config = ConfigUtils._resolve_placeholders(config, env_vars)

        return config
    
   
    @staticmethod
    def get_schema(schema_file: str) -> StructType:
        """Loads schema from JSON file, found the same way as config files (absolute path, sys.path, then cwd)."""
        schema_path = ConfigUtils._resolve_path(schema_file)

        with open(schema_path) as file:
            return StructType.fromJson(json.load(file))
