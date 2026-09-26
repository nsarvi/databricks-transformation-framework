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


    @staticmethod
    def _resolve_placeholders(config: dict, env_vars: dict) -> dict:
        """Recursively replace placeholders like {catalog} or ${catalog} with env values."""
        pattern_dollar = re.compile(r"\$\{([^}]+)\}")

        def substitute(value):
            if isinstance(value, str):
                value = pattern_dollar.sub(lambda m: env_vars.get(m.group(1), m.group(0)), value)
                return value
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
    def _load_env_variables(env_config_file: Optional[str] = None) -> Dict:
        if not env_config_file:
            raise ValueError("env_config_file must be provided.")
        
        env_file_path = ConfigUtils._resolve_path(env_config_file)

        with open(env_file_path, "r") as f:
            return yaml.safe_load(f)

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
            env_vars = ConfigUtils._load_env_variables(env_config_file)
            config = ConfigUtils._resolve_placeholders(config, env_vars)

        return config
    
   
    @staticmethod
    def get_schema(schema_file: str) -> StructType:
        """Loads schema from JSON file."""
        # Get the absolute path of the current module
        module_path = Path(__file__).resolve()
        relative_schema_file = module_path.parent.parent.parent.parent.parent / schema_file

        if not os.path.exists(relative_schema_file):
            logger.error(f"Schema file not found: {relative_schema_file}")
            raise FileNotFoundError(f"Schema file not found: {relative_schema_file}")

        with open(relative_schema_file) as file:
            return StructType.fromJson(json.load(file))
