# Databricks transformation framework

A config-driven framework for Databricks pipelines. You describe a pipeline in YAML (which tables to
read, how to transform, join or union them, and which Delta tables to write) and the framework runs it.
It's packaged as the `wilsonelser-dtf` wheel and imported as `wilsonelser.transformation`.

```yaml
sources:
  - source_id: "customers"
    table: "${catalog}.${schema}.customer_mapics"
    transformation_id: "prepare_customers"

transformations:
  - id: "prepare_customers"
    additional_columns:
      - name: "processed_date"
        type: "timestamp"
        value: "current_timestamp()"

targets:
  - table_id: "silver_customers"
    table: "${catalog}.${schema}.customers"
    write_mode: "overwrite"
    source_type: "sources"
    source_id: "customers"
```

```python
from wilsonelser.transformation.table_writer import TableWriter

TableWriter("use_cases/sales/customers.yml", "config/env/dev.yaml").write_table("silver_customers")
```

## Documentation

| If you... | Read |
|---|---|
| Write pipelines: YAML configs, environments, running and deploying them | [Pipeline guide](docs/pipeline-guide.md) |
| Change the framework: setup, architecture, tests, releases | [Developer guide](docs/developer-guide.md) |

## Quickstart for developers

```bash
uv sync                                   # Python 3.12 environment with dev tools
uv run pytest                             # unit tests (needs a Databricks Connect cluster, see the developer guide)
uv build --wheel                          # dist/wilsonelser_dtf-<version>-py3-none-any.whl
```
