"""A pipeline's configuration, loaded, resolved and validated once, then read by id."""
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils

# Each list section and the id key of its entries
SECTIONS = {
    YC.SOURCES_KEY: YC.SOURCE_ID_KEY,
    YC.TRANSFORMATIONS_KEY: YC.ID_KEY,
    YC.COMBINE_KEY: YC.COMBINE_ID_KEY,
    YC.TARGETS_KEY: YC.TABLE_ID_KEY,
}
AUTO_ENV = "auto"
YAML_SUFFIXES = (".yml", ".yaml")


class PipelineConfig:
    """
    A pipeline's configuration: loaded, `${...}` placeholders resolved, and validated once.

    Load it from a YAML file, or from a folder whose YAML files (including subfolders, in sorted order) are
    merged into one pipeline. Entries are then looked up by id with source(), transformation(), combine()
    and target(). Validation at load time checks that ids are unique and that every id a config refers to
    exists, so config mistakes fail before any data is read or written.
    """

    def __init__(self, data: Dict[str, Any], env_vars: Optional[Dict[str, Any]] = None, origin: str = "<dict>"):
        """Use load() or from_dict(); `data` must already have its placeholders resolved."""
        self.data = data
        self.env_vars = env_vars or {}
        self.origin = origin
        self._lookups = {section: self._index(section, id_key) for section, id_key in SECTIONS.items()}
        self._validate_references()

    # ---- loading -------------------------------------------------------------------------------------

    @classmethod
    def load(cls, path: str, env_config_file: Optional[str] = None) -> "PipelineConfig":
        """
        Loads a pipeline config from a YAML file or a folder of YAML files.

        :param path: A YAML file, or a folder whose YAML files are merged. Found like other config files:
                     absolute, then relative to sys.path entries, then to the current directory.
        :param env_config_file: Values for ${...} placeholders: a file path, "auto" for the current
                                workspace's environment (config/env/environments.yaml), or None to use
                                environment variables only.
        """
        if env_config_file == AUTO_ENV:
            env_config_file = ConfigUtils.env_config_file()
        env_vars = ConfigUtils.load_env_variables(env_config_file)

        resolved_path = ConfigUtils._resolve_path(path)
        files = cls._yaml_files(resolved_path)
        merged: Dict[str, Any] = {}
        for file in files:
            with open(file) as f:
                cls._merge(merged, yaml.safe_load(f) or {}, file)

        if ConfigUtils._has_placeholders(merged):
            merged = ConfigUtils._resolve_placeholders(merged, env_vars)
        return cls(merged, env_vars, origin=str(path))

    @classmethod
    def from_dict(cls, data: Dict[str, Any], env_config_file: Optional[str] = None) -> "PipelineConfig":
        """Builds a pipeline config from a dict, e.g. in tests or when a config is generated in code."""
        if env_config_file == AUTO_ENV:
            env_config_file = ConfigUtils.env_config_file()
        env_vars = ConfigUtils.load_env_variables(env_config_file)
        if ConfigUtils._has_placeholders(data):
            data = ConfigUtils._resolve_placeholders(data, env_vars)
        return cls(data, env_vars)

    @staticmethod
    def _yaml_files(path: Path) -> List[Path]:
        if path.is_file():
            return [path]
        files = sorted(p for p in path.rglob("*") if p.is_file() and p.suffix in YAML_SUFFIXES)
        if not files:
            raise ValueError(f"No YAML files found in config folder: {path}")
        return files

    @staticmethod
    def _merge(merged: Dict[str, Any], content: Dict[str, Any], file: Path) -> None:
        """Adds one file's content: list sections are concatenated, other mappings merged key by key."""
        if not isinstance(content, dict):
            raise ValueError(f"Config file must contain a YAML mapping: {file}")
        for key, value in content.items():
            if key in SECTIONS:
                if not isinstance(value, list):
                    raise ValueError(f"'{key}' must be a list in {file}")
                merged.setdefault(key, []).extend(value)
            elif key not in merged:
                merged[key] = value
            elif isinstance(merged[key], dict) and isinstance(value, dict):
                duplicates = merged[key].keys() & value.keys()
                if duplicates:
                    raise ValueError(f"'{key}' sets {', '.join(sorted(duplicates))} in more than one file, again in {file}")
                merged[key] = {**merged[key], **value}
            else:
                raise ValueError(f"'{key}' is defined in more than one file, again in {file}")

    # ---- validation ----------------------------------------------------------------------------------

    def _index(self, section: str, id_key: str) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for position, entry in enumerate(self.data.get(section) or []):
            if not isinstance(entry, dict) or not entry.get(id_key):
                raise ValueError(f"{self.origin}: {section}[{position}] is missing '{id_key}'")
            entry_id = entry[id_key]
            if entry_id in lookup:
                raise ValueError(f"{self.origin}: {section} has more than one entry with {id_key} '{entry_id}'")
            lookup[entry_id] = entry
        return lookup

    def _validate_references(self) -> None:
        sources, transformations = self._lookups[YC.SOURCES_KEY], self._lookups[YC.TRANSFORMATIONS_KEY]
        combines = self._lookups[YC.COMBINE_KEY]
        errors = []

        both = sorted(sources.keys() & combines.keys())
        if both:
            errors.append(f"ids used as both a source_id and a combine_id: {', '.join(both)}")

        for source_id, source in sources.items():
            transformation_id = source.get(YC.TRANSFORMATION_ID_KEY)
            if transformation_id and transformation_id not in transformations:
                errors.append(f"source '{source_id}' uses unknown transformation_id '{transformation_id}'")

        inputs = sources.keys() | combines.keys()
        for combine_id, combine in combines.items():
            for input_id in self._combine_inputs(combine):
                if input_id not in inputs:
                    errors.append(f"combine '{combine_id}' uses unknown input '{input_id}' (not a source_id or combine_id)")

        for table_id, target in self._lookups[YC.TARGETS_KEY].items():
            source_type, source_id = target.get(YC.SOURCE_TYPE_KEY), target.get(YC.SOURCE_ID_KEY)
            if source_type is None or source_id is None:
                continue
            known = {YC.SOURCES_KEY: sources, YC.COMBINE_KEY: combines}.get(source_type)
            if known is None:
                errors.append(f"target '{table_id}' has source_type '{source_type}'; use '{YC.SOURCES_KEY}' or '{YC.COMBINE_KEY}'")
            elif source_id not in known:
                errors.append(f"target '{table_id}' uses unknown {source_type} id '{source_id}'")

        if errors:
            raise ValueError(f"Invalid pipeline config {self.origin}:\n  - " + "\n  - ".join(errors))

    @staticmethod
    def _combine_inputs(combine: Dict[str, Any]) -> List[str]:
        unions = combine.get(YC.UNIONS_KEY)
        joins = combine.get(YC.JOINS_KEY)
        inputs = []
        if isinstance(unions, dict):
            inputs += list(unions.get(YC.SOURCE_IDS_KEY) or [])
        if isinstance(joins, dict):
            inputs += [s.get(YC.SOURCE_ID_KEY) for s in joins.get(YC.SOURCE_IDS_KEY) or [] if isinstance(s, dict)]
        return [i for i in inputs if i]

    # ---- lookups -------------------------------------------------------------------------------------

    @property
    def global_config(self) -> Dict[str, Any]:
        return self.data.get(YC.GLOBAL_CONFIG_KEY, {}) or {}

    @property
    def sources(self) -> Dict[str, Dict[str, Any]]:
        return self._lookups[YC.SOURCES_KEY]

    @property
    def transformations(self) -> Dict[str, Dict[str, Any]]:
        return self._lookups[YC.TRANSFORMATIONS_KEY]

    @property
    def combines(self) -> Dict[str, Dict[str, Any]]:
        return self._lookups[YC.COMBINE_KEY]

    @property
    def targets(self) -> Dict[str, Dict[str, Any]]:
        return self._lookups[YC.TARGETS_KEY]

    def _get(self, section: str, entry_id: str) -> Dict[str, Any]:
        entry = self._lookups[section].get(entry_id)
        if entry is None:
            known = ", ".join(sorted(self._lookups[section])) or "none"
            raise ValueError(f"'{entry_id}' is not defined in {section} of {self.origin}. Defined: {known}")
        return entry

    def source(self, source_id: str) -> Dict[str, Any]:
        return self._get(YC.SOURCES_KEY, source_id)

    def transformation(self, transformation_id: str) -> Dict[str, Any]:
        return self._get(YC.TRANSFORMATIONS_KEY, transformation_id)

    def combine(self, combine_id: str) -> Dict[str, Any]:
        return self._get(YC.COMBINE_KEY, combine_id)

    def target(self, table_id: str) -> Dict[str, Any]:
        return self._get(YC.TARGETS_KEY, table_id)
