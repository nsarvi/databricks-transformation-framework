# Developer guide

For developers who change the framework itself: the code in `src/wilsonelser/`, its tests, and releases.
To build pipelines with it, see the [pipeline guide](pipeline-guide.md).

- [Setup](#setup)
- [Project layout](#project-layout)
- [Architecture](#architecture)
- [Testing](#testing)
- [Adding a feature](#adding-a-feature)
- [Build and deploy](#build-and-deploy)
- [Conventions](#conventions)
- [Known issues](#known-issues)

## Setup

Requirements: [uv](https://docs.astral.sh/uv/), the Databricks CLI, and a profile for your workspace in
`~/.databrickscfg` (create one with `databricks auth login --host <workspace-url>`).

```bash
uv sync          # creates .venv with Python 3.12 and the dev tools
```

**Databricks Connect.** Tests and local runs execute Spark on a Databricks cluster through Databricks
Connect, which replaces the local `pyspark` package. There is no local Spark.

- **The client must not be newer than the cluster.** `databricks-connect` is pinned to `==18.0.*` in
  `pyproject.toml`, which works with any Runtime 18 cluster. A newer client (for example 18.3 against an
  18 LTS cluster) can hang on some requests instead of failing.
- **When the cluster runtime changes,** update the pin, the `requires-python` comment and the
  `spark_version` of the test jobs in `use_cases/*/resources/` together, then run the integration tests.
- **`[tool.uv] constraint-dependencies`** in `pyproject.toml` is managed by
  `databricks environments setup-local` and keeps local libraries on the cluster's versions. Don't edit it
  by hand.

**Connecting.** In VS Code, the Databricks extension sets the cluster and profile. In a terminal, set
these environment variables:

| Variable | Value | Needed for |
|---|---|---|
| `DATABRICKS_CONFIG_PROFILE` | Profile name in `~/.databrickscfg` | Everything |
| `DATABRICKS_CLUSTER_ID` | **ID** of a Runtime 18 cluster (not its name) | Spark tests, unless `cluster_id` is in the profile |
| `TEST_CATALOG` | Catalog where you can create schemas | Integration tests |
| `TEST_SCHEMA` | Fixed schema to use and keep (optional) | Integration tests |

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export DATABRICKS_CLUSTER_ID=<cluster-id>
export TEST_CATALOG=<test-catalog>
```

For example, with a profile named `dev-workspace`, a cluster whose ID is `0123-456789-abcd1234` and a
sandbox catalog `dev_sandbox`:

```bash
export DATABRICKS_CONFIG_PROFILE=dev-workspace
export DATABRICKS_CLUSTER_ID=0123-456789-abcd1234
export TEST_CATALOG=dev_sandbox
uv run pytest -m ""                   # all tests
```

To avoid exporting the cluster each time, put it in the profile instead. An exported
`DATABRICKS_CLUSTER_ID` still overrides it:

```ini
# ~/.databrickscfg
[dev-workspace]
host       = https://adb-1234567890123456.7.azuredatabricks.net/
cluster_id = 0123-456789-abcd1234
```

Find cluster IDs with `databricks clusters list --profile <profile>`, or in the cluster's URL. The error
`Cluster <name> does not exist` means a cluster name was given where the ID is expected.

## Project layout

```
src/wilsonelser/transformation/    the framework (the only code in the wheel)
  engine.py                        TransformationEngine: the single entry point for pipelines
  pipeline_config.py               PipelineConfig: loads a file or folder, resolves placeholders, validates ids
  yaml_constants.py                every config key, as a constant
  base_integration.py              config loading, Spark session, source reads, column/row operations
  base_data_transformer.py         apply_transformations: runs a transformation's operations
  table_reader.py                  read_source_table
  data_transformer.py              joins, unions, row operations
  table_writer.py                  table creation, batch/stream writes, merge
  utils/config_utils.py            YAML loading, ${placeholder} resolution, path lookup, schema files
  utils/logging_utils.py           LoggingHandler
config/env/                        environments.yaml (workspace per environment) and <env>.yaml values
use_cases/                         example and test pipelines (real ones live in the pipeline repo);
                                   each can have a resources/ folder with its test jobs
tests/                             unit and integration tests, see Testing
docs/                              these guides
```

## Architecture

The public classes form one inheritance chain, so each adds to the previous. `TransformationEngine` is
the entry point for pipelines; the other classes remain for existing callers:

```
BaseIntegration ─► BaseDataTransformer ─► TableReader ─► DataTransformer ─► TableWriter ─► TransformationEngine
 config, session     apply_transformations    read_source_table   joins, unions        write_table, merge
 operation helpers
```

**The config: `PipelineConfig`** (`pipeline_config.py`), loaded once and shared:

1. `PipelineConfig.load(path, env_config_file)` finds the path like other config files, and reads one YAML
   file or merges a folder's YAML files: list sections are concatenated, other mappings merged, and a key
   set twice is an error.
2. It resolves `${name}` placeholders from the env file, then environment variables, and fails on anything
   unresolved (`ConfigUtils`). `env_config_file="auto"` picks the env file for the current workspace
   through `config/env/environments.yaml`.
3. It builds id → entry lookups and validates them: ids present and unique, and every referenced id
   (`transformation_id`, combine inputs, target `source_type`/`source_id`) defined. All problems are
   reported in one error.

`PipelineConfig.from_dict()` builds one in code, for tests or generated configs.

**Construction.** `BaseIntegration.__init__(config, env_config_path=None)` takes a path (loaded with
`PipelineConfig.load`) or a `PipelineConfig`, which isn't reloaded:

1. Gets a Spark session (`_get_spark`): Databricks Connect when it's installed, otherwise the active
   `SparkSession`. On a cluster the wheel runs without `databricks-connect`, so the import is inside the
   method.
2. Takes `config`, `env_vars`, `global_config` and the lookups (`source_lookup`, `transformation_lookup`,
   `combine_lookup`, `target_lookup`) from the `PipelineConfig`.

**Operations.** Each transformation operation is a static method in `BaseIntegration` that takes its
config and returns a `DataFrame -> DataFrame` function, applied with `df.transform(...)`.
`apply_transformations` applies them in the order the keys appear in the YAML.

**Writes.** `TableWriter.write_table`:

1. Resolves the DataFrame, from the argument or the target's `source_type`/`source_id`.
2. Creates the table if needed: `_create_table` writes an empty DataFrame with the schema, plus
   `partition_by`/`cluster_by`.
3. Writes with the target's `write_mode`: batch write, `merge_into_target`, or a streaming query.

**Files.** Config, env and schema files are found by `ConfigUtils._resolve_path`: absolute path,
then each `sys.path` entry, then the current directory. Never build paths relative to the package's own
location, because that breaks once the code is installed as a wheel.

## Testing

```bash
uv run pytest                                   # unit tests
TEST_CATALOG=<test-catalog> uv run pytest -m integration
uv run pytest -m ""                             # everything
uv run pytest tests/joins/union_test.py::TestDataTransformer::test_apply_unions   # one test
```

Logs show live at INFO level (`log_cli` in `pyproject.toml`); add `--log-cli-level=DEBUG` for more.

### Unit tests

- **Spark tests** build DataFrames in memory. They still need a Databricks Connect session, but no tables.
- **`tests/utils/`** needs no Spark:
  - `config_utils_test.py`: placeholder rules.
  - `environments_test.py`: workspace-to-environment lookup.
  - `real_configs_test.py`: parses every `use_cases/**/*.yml` and `config/env/*.yaml` with dummy values.
  - `docs_examples_test.py`: parses every YAML example in `docs/`.

### Integration tests

Modules marked `pytestmark = pytest.mark.integration` read and write real tables. They're skipped unless
selected with `-m integration`, and skipped with a message when `TEST_CATALOG` is unset.

`tests/conftest.py` runs them in a throwaway schema:

- **Setup:** creates `<TEST_CATALOG>.dtf_test_<random>` with a `checkpoints` volume, once per run.
- **Placeholders:** sets the `catalog` and `schema` environment variables, so `${catalog}.${schema}` in
  test configs resolve to that schema.
- **Cleanup:** drops the schema with `CASCADE` at the end. Set `TEST_SCHEMA` to use a fixed schema that
  is kept for inspection.

In Python tests, build table names with `table_name("customers")` from `tests/integration_env.py`.
Never call `spark.stop()`: all tests share one Databricks Connect session.

### Test rules

- **Tests read only `tests/configs/`.** Test configs use `${catalog}.${schema}` for tables, and
  `tests/configs/env/test-env-config.yaml` is the only env file tests use. Its values never include
  `catalog` or `schema`.
- **Tests with mocked reads** (e.g. `union_test.py`) use fixed dummy names like `unit_test.mocked.*`,
  so they need no placeholder values.
- **`tests/legacy/`** holds old tests for client data from the previous workspace. pytest ignores it
  (`collect_ignore` in `conftest.py`); see its README.
- **Log** through the module logger and `df_to_string(df)` from `tests/log_helpers.py`. Don't use
  `print` or `df.show()`.

## Adding a feature

For a new config key or operation:

1. **Add the key** to `yaml_constants.py`. Refer to it only as `YC.<NAME>`.
2. **Implement it** in the right class. For a transformation operation: a static method returning
   `DataFrame -> DataFrame`, plus a branch in `apply_transformations`.
3. **Add a test config** under `tests/configs/` and a test. Unit test if it works on in-memory
   DataFrames; integration test if it needs tables.
4. **Document it** in the [pipeline guide](pipeline-guide.md) in the same pull request: add a row to the
   right reference table and a YAML example. `docs_examples_test.py` checks that the example parses.
5. **Run** `uv run pytest`, and `-m integration` if you touched reads or writes.

## Build and deploy

**Version.** Set `__version__` in `src/wilsonelser/__init__.py`; the wheel reads it from there.

**Publish for notebooks.** Upload each release next to the previous ones; never overwrite a version in use:

```bash
uv build --wheel
databricks workspace import /Workspace/Shared/libraries/wilsonelser-dtf/wilsonelser_dtf-<version>-py3-none-any.whl \
  --file dist/wilsonelser_dtf-<version>-py3-none-any.whl --format RAW --profile <profile>
```

Then update the `%pip install` line in the notebooks that should use the new version.

**Build locally.**

```bash
uv build --wheel      # dist/wilsonelser_dtf-<version>-py3-none-any.whl
```

The wheel contains only `src/wilsonelser` and depends only on `pyyaml`. Spark, Delta and the Databricks
SDK come from the runtime. `python -m build` isn't set up; use `uv build`.

**The bundle.** This repo deploys no production jobs; the wheel is the deliverable. `databricks.yml` is used
by the Databricks VS Code extension to connect to the workspace, and to deploy test jobs for the example
pipelines, which live next to them in `use_cases/<use case>/resources/*.yml`. For example, to run the Expert
Sierra Bronze test job:

```bash
databricks bundle validate --strict --target dev --profile <profile>
databricks bundle deploy --target dev --profile <profile>
databricks bundle run expert_sierra_bronze_job --target dev --profile <profile>
```

The wheel has no entry points; pipelines import it.

## Conventions

- **No hardcoded catalogs or schemas** in code or configs; use `${catalog}`-style placeholders.
- **Logging:**
  - get loggers with `LoggingHandler(__name__).get_logger()`. Loggers live under the `wilsonelser`
    namespace, and the first one configures logging. Records always go to the host's logging (pytest,
    apps), and a console handler prints them only while the host has none (notebooks, jobs), decided per
    record so the order of setup doesn't matter. Level: `DTF_LOG_LEVEL`, INFO by default;
    `configure_logging()` overrides it;
  - use `%s` arguments (`logger.info("Wrote %s rows", n)`), not f-strings;
  - never log DataFrames with `df.show()`.
- **Paths:** resolve files through `ConfigUtils._resolve_path`. Paths in configs and examples are relative
  to the repo root.
- **Custom functions** for pipelines live in `use_cases.custom_transformations.<domain>_tx`, with shared
  ones in `common_tx`.
- **Imports:** keep optional or local-only packages, like `databricks.connect`, as imports inside the
  functions that use them, so the wheel imports on a cluster.
- **Docs:** update `docs/` with every user-visible change.

## Known issues

Found in code review and not fixed yet. Fix them test-first, and update the
[pipeline guide's known limitations](pipeline-guide.md#known-limitations) when behavior changes.

| Issue | Where |
|---|---|
| `distinct_columns: []` raises `IndexError` | `BaseIntegration.apply_distinct_columns` |
| `columns_to_snake_case` can produce duplicate or empty column names (e.g. `CustID` and `cust_id`, or a name made only of symbols) | `BaseIntegration.apply_columns_to_snake_case` |
