"""QBE rules, read from the YAML files in config/tables or from the rules table, in one shape.

A table config is {"table", "record_columns", "columns": {column: {"column", "business_term", "rules"}},
"table_rules"}; each rule has rule_id, rule_name, description, dq_dimension, criticality, thresholds, an
optional filter, enabled, and check ({"function", "arguments"}, and for table rules optionally
"for_each_column": the columns DQX runs the check on, one by one).

The rules table has one row per rule (RULES_SCHEMA). Table-level settings (table, record_columns) repeat on
each of the table's rows, the DQX check arguments are stored as JSON, and for_each_column as an array.
${name} placeholders are kept as written, so the same rows work in every environment; the validation notebook
resolves them when it runs.

Plain Python (PyYAML only), used by use_cases/qbe/notebooks/qbe_validation.ipynb and qbe_sync_rules.ipynb,
which add use_cases/qbe/src to sys.path.
"""
import json
from pathlib import Path

import yaml

RULES_SCHEMA = (
    "table_name string, record_columns array<string>, column_name string, business_term string, "
    "rule_id string, rule_name string, description string, dq_dimension string, criticality string, "
    "minimum_threshold double, target_threshold double, filter string, check_function string, "
    "check_arguments string, check_for_each_column array<string>, enabled boolean"
)


def read_yaml_tables(tables_dir: Path) -> list:
    """Every table file in tables_dir (<table>.yml), with the column files it lists read into its columns."""
    table_configs = []
    for path in sorted(Path(tables_dir).glob("*.yml")):
        table_config = yaml.safe_load(path.read_text())
        columns = {}
        for column_file in table_config.get("columns") or []:
            column_config = yaml.safe_load((path.parent / column_file).read_text())
            column = column_config.get("column")
            if not column:
                raise ValueError(f"{column_file} (listed in {path.name}) needs a column")
            if column in columns:
                raise ValueError(f"column {column} is in more than one file listed in {path.name}")
            columns[column] = column_config
        table_configs.append({**table_config, "columns": columns})
    return table_configs


def _rule_row(table_config: dict, column_config: dict, rule: dict) -> dict:
    as_float = lambda value: float(value) if value is not None else None
    return {
        "table_name": table_config["table"],
        "record_columns": table_config.get("record_columns"),
        "column_name": column_config.get("column"),
        "business_term": column_config.get("business_term"),
        "rule_id": rule["rule_id"],
        "rule_name": rule.get("rule_name"),
        "description": rule.get("description"),
        "dq_dimension": rule.get("dq_dimension"),
        "criticality": rule.get("criticality", "error"),
        "minimum_threshold": as_float(rule.get("minimum_threshold")),
        "target_threshold": as_float(rule.get("target_threshold")),
        "filter": rule.get("filter"),
        "check_function": rule["check"]["function"],
        "check_arguments": json.dumps(rule["check"].get("arguments") or {}),
        "check_for_each_column": rule["check"].get("for_each_column"),
        "enabled": rule.get("enabled", True),
    }


def to_rule_rows(table_configs: list) -> list:
    """One row per rule (RULES_SCHEMA), disabled rules included; rule_ids must be unique."""
    rows = []
    for table_config in table_configs:
        for column_config in (table_config.get("columns") or {}).values():
            rows += [_rule_row(table_config, column_config, rule) for rule in column_config.get("rules") or []]
        rows += [_rule_row(table_config, {}, rule) for rule in table_config.get("table_rules") or []]
    rule_ids = [row["rule_id"] for row in rows]
    duplicates = sorted({rule_id for rule_id in rule_ids if rule_ids.count(rule_id) > 1})
    if duplicates:
        raise ValueError(f"rule_id used more than once: {', '.join(duplicates)}")
    return rows


def from_rule_rows(rows: list) -> list:
    """Table configs from rules table rows (dicts), in the shape read_yaml_tables returns."""
    table_configs = {}
    for row in sorted(rows, key=lambda row: (row["table_name"], row["column_name"] or "", row["rule_id"])):
        table_config = table_configs.setdefault(row["table_name"], {
            "table": row["table_name"], "record_columns": row["record_columns"], "columns": {}, "table_rules": []})
        if row["record_columns"] != table_config["record_columns"]:
            raise ValueError(f"rules for {row['table_name']} have different record_columns (rule {row['rule_id']})")
        rule = {
            "rule_id": row["rule_id"], "rule_name": row["rule_name"], "description": row["description"],
            "dq_dimension": row["dq_dimension"], "criticality": row["criticality"],
            "minimum_threshold": row["minimum_threshold"], "target_threshold": row["target_threshold"],
            "enabled": row["enabled"] is not False,
            "check": {"function": row["check_function"], "arguments": json.loads(row["check_arguments"] or "{}")},
        }
        if row.get("check_for_each_column"):
            rule["check"]["for_each_column"] = list(row["check_for_each_column"])
        if row["filter"]:
            rule["filter"] = row["filter"]
        if row["column_name"]:
            column_config = table_config["columns"].setdefault(row["column_name"], {
                "column": row["column_name"], "business_term": row["business_term"], "rules": []})
            column_config["rules"].append(rule)
        else:
            table_config["table_rules"].append(rule)
    return list(table_configs.values())
