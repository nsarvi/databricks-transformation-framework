import os
import yaml
import json
from pathlib import Path
from typing import Dict
from pyspark.sql.types import StructType
from adm.integration.utils.logging_utils import LoggingHandler

logger = LoggingHandler(__name__).get_logger()

class ConfigUtils:
    """Utility class for handling configuration and schema loading."""

    @staticmethod
    def load_config(config_file: str) -> Dict:
        """Loads the transformation configuration."""
        # Get the absolute path of the current module
        module_path = Path(__file__).resolve()
        relative_config_file = module_path.parent.parent.parent.parent.parent / config_file

        if not os.path.exists(relative_config_file):
            logger.error(f"Config file not found: {relative_config_file}")
            raise FileNotFoundError(f"Config file not found: {relative_config_file}")

        with open(relative_config_file, "r") as file:
            return yaml.safe_load(file)

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