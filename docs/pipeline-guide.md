# Pipeline guide

For data engineers who build pipelines with the framework: you write a YAML config, and the framework
reads the source tables, transforms them, and writes the result to Delta tables.
To change the framework itself, see the [developer guide](developer-guide.md).

**Where pipelines live.** Pipelines are built in their own repo, which installs the framework wheel. This
guide's layout and conventions are for that repo. The framework repo's `use_cases/` folder only holds
examples and test pipelines.

- [How a pipeline works](#how-a-pipeline-works)
- [Your first pipeline](#your-first-pipeline)
- [Organizing a pipeline](#organizing-a-pipeline)
- [Environments and placeholders](#environments-and-placeholders)
- [Running a pipeline](#running-a-pipeline)
- [Config reference](#config-reference): [sources](#sources), [transformations](#transformations),
  [combine](#combine), [targets](#targets), [global_config](#global_config)
- [Custom functions](#custom-functions)
- [Known limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)

## How a pipeline works

A pipeline is one YAML file with up to five sections:

```
sources ──► transformations ──► combine (joins or unions) ──► targets
   │              ▲
   └──────────────┘  each source can name a transformation that runs right after it is read
```

| Section | What it defines |
|---|---|
| `sources` | Tables, streams, change feeds or SQL queries to read, each with a `source_id` |
| `transformations` | Named lists of column and row operations, each with an `id` |
| `combine` | Joins or unions of several sources, each with a `combine_id` |
| `targets` | Delta tables to write, each with a `table_id`, fed by a source or a combine |
| `global_config` | Settings for streaming targets (checkpoint location, query names) |

Sections refer to each other by id, for example a target's `source_id` points at a `combine_id`.

## Your first pipeline

This pipeline reads customers and sales, adds and renames columns, joins them, and merges the result
into a silver table:

```yaml
sources:
  - source_id: "customers"
    table: "${catalog}.${schema}.customer_mapics"
    transformation_id: "prepare_customers"

  - source_id: "sales"
    table: "${catalog}.${schema}.sales_mapics"
    transformation_id: "prepare_sales"

transformations:
  - id: "prepare_customers"
    additional_columns:
      - name: "source_system"
        type: "string"
        value: "'MAPICS'"
    column_rename:
      - column: "customer_email_mapics"
        target_column: "customer_email"

  - id: "prepare_sales"
    column_rename:
      - column: "customer_id_mapics"
        target_column: "customer_id"

combine:
  - combine_id: "customer_sales"
    joins:
      source_ids:
        - source_id: "customers"
          alias: "c"
          join_order: 1
        - source_id: "sales"
          alias: "s"
          join_order: 2
          join_type: "inner"
      join_conditions:
        - left: "c"
          right: "s"
          condition: "c.customer_id = s.customer_id"
      select_columns:
        c: ["customer_id", "customer_name", "customer_email"]
        s: ["transaction_id", "quantity", "price"]
      post_join_filters:
        - condition: "price < 50"

targets:
  - table_id: "silver_customer_sales"
    table: "${catalog}.${schema}.customer_sales"
    write_type: "table"
    write_mode: "merge"
    source_type: "combine"
    source_id: "customer_sales"
    merge_condition: "target.customer_id = source.customer_id AND target.transaction_id = source.transaction_id"
    merge_actions:
      when_matched:
        update:
          - "quantity = source.quantity"
          - "price = source.price"
      when_not_matched:
        insert: ["customer_id", "customer_name", "customer_email", "transaction_id", "quantity", "price"]
```

Run it with one call:

```python
from wilsonelser.transformation.engine import TransformationEngine

engine = TransformationEngine("use_cases/sales/customer_sales.yml", env_config_file="auto")
engine.write_table("silver_customer_sales")
```

`write_table` reads the target's `source_id` (here the `customer_sales` join, which reads and transforms
both sources), creates `customer_sales` if it doesn't exist, and runs the merge.

## Organizing a pipeline

In the pipeline repo, keep each pipeline's files together under `use_cases/<domain>/`, and shared code in
`use_cases/custom_transformations/`:

```
config/env/environments.yaml                the workspace of each environment
config/env/dev.yaml                         environment values (catalog, schemas)
use_cases/
  sales/                                    one folder per domain
    customer_sales.yml                      the pipeline config (or a customer_sales/ folder of YAML files)
    customer_sales.ipynb                    the notebook that runs it
    sqls/customer_sales.sql                 SQL used by transformation_sql_file
    schemas/customer_sales.json             table schemas used by schema_file
  custom_transformations/
    sales_tx.py                             custom functions for the sales domain
```

**Write every path relative to the repo root.** This covers the config and env file you pass to the
classes, and `transformation_sql_file` and `schema_file` inside the YAML: for example
`"use_cases/sales/sqls/customer_sales.sql"`, not `/Workspace/...` or `../../...`.

This works because the framework looks for each file, in order:

1. as given, if it's an absolute path;
2. relative to each folder on `sys.path`;
3. relative to the current working directory.

In a Databricks Git folder the repo root is already on `sys.path`, so a notebook anywhere in the repo
finds `use_cases/...` and `config/...` without any path setup. The same repo root makes
`use_cases.custom_transformations` importable for [custom functions](#custom-functions). Where the repo
root isn't on `sys.path` (check with `print(sys.path)`), add it once before creating the classes:

```python
import sys
sys.path.append("/Workspace/Users/<you>/databricks-transformation-framework")   # the repo or bundle root
```

Paths stay the same in every environment and in version control, and only that one line depends on
where the code is deployed.

## Environments and placeholders

Don't hardcode catalogs or schemas. Write `${name}` placeholders instead, and supply the values per
environment:

```yaml
sources:
  - source_id: "customers"
    table: "${catalog}.${bronze_schema}.customer_mapics"
```

Placeholders work in any string value in the config, and in SQL files used by
[`transformation_sql_file`](#transformation_sql_file).

Each placeholder is resolved in this order:

1. **The env config file** passed as the second argument, for example
   `TransformationEngine(config, "config/env/dev.yaml")`, or `"auto"` to pick it from the workspace (below).
   Env files are flat `name: value` mappings; the shared ones live in `config/env/`, one per environment.
2. **An environment variable** with the same name, for example `catalog`. Useful in jobs and CI.

If neither has a value, loading fails with `No value for placeholder '${catalog}'` instead of running
against a wrong table.

```yaml
# config/env/dev.yaml
sales_catalog: sales_dev
sales_bronze_schema: bronze
```

The env files are shared by every pipeline in the repo, so **prefix each key with your domain**
(`sales_catalog`, `expertsierra_catalog`) to avoid clashing with another domain's values.

### Picking the environment automatically

Each environment has its own Databricks workspace, listed in `config/env/environments.yaml`:

```yaml
dev: adb-1234567890123456.7.azuredatabricks.net
uat: adb-2345678901234567.8.azuredatabricks.net
prod: adb-3456789012345678.9.azuredatabricks.net
```

Pass `env_config_file="auto"` and the framework looks up the current workspace's URL in that file and uses
the matching env file, e.g. `config/env/dev.yaml`. The same notebook and config then run unchanged in every
environment, with no parameters:

```python
engine = TransformationEngine("use_cases/sales/customer_sales.yml", env_config_file="auto")
```

For other files, `ConfigUtils.env_config_file()` returns that env file path, e.g.
`ConfigUtils.load_config("use_cases/sales/config/sales_load.yml", ConfigUtils.env_config_file())`.

`ConfigUtils.current_env()` returns just the name (`dev`). Both fail with a clear message when the
workspace isn't listed, or is listed under more than one environment.

## Running a pipeline

**One engine per pipeline.** Create a `TransformationEngine` once from the pipeline's config, then work by id:

```python
from wilsonelser.transformation.engine import TransformationEngine

engine = TransformationEngine("use_cases/sales/customer_sales.yml", env_config_file="auto")
engine.write_table("silver_customer_sales")        # read, transform, combine and write a target
engine.read_source_table("customers")              # a source, with its transformation_id applied
engine.apply_combine("customer_sales")             # a join or union
engine.apply_transformations("clean", df)          # a transformation on any DataFrame
```

| Method | Use it to |
|---|---|
| `write_table(table_id, df=None)` | Write a target; `merge_into_target(table_id)` for a merge on its own |
| `read_source_table(source_id)` | Read a source and apply its `transformation_id` |
| `apply_combine(combine_id)` | Run a join or union; also `apply_joins` and `apply_unions` |
| `apply_transformations(transformation_id, df)` | Run a transformation on any DataFrame |

**The config** is loaded, its placeholders resolved and its ids checked once, when the engine is created:

- **A file or a folder.** Pass one YAML file, or a folder: every `*.yml`/`*.yaml` in it (and its subfolders,
  in sorted order) is merged into one pipeline. `sources`, `transformations`, `combine` and `targets` are
  combined across files; other sections such as `global_config` are merged, and the same key set in two
  files is an error. Split a large pipeline by topic, e.g. `sources.yml`, `joins/`, `targets.yml`.
- **Checked up front.** Ids must be unique, and every id the config refers to must exist: a source's
  `transformation_id`, a combine's inputs, a target's `source_type`/`source_id`. All problems are
  reported together, before any data is read or written.
- **Loaded once, shared.** `PipelineConfig.load(path, env_config_file="auto")` from
  `wilsonelser.transformation.pipeline_config` gives the loaded config; pass it to several engines, or build
  one in code with `PipelineConfig.from_dict({...})`. `TransformationEngine(config)` then doesn't reload.

`TableReader`, `DataTransformer`, `TableWriter` and `BaseDataTransformer` still work the same way, with the
same arguments; `TransformationEngine` has everything they have.

**Logs.** The framework logs at INFO to the notebook or job output (or to your own logging setup, if you
have one). For more detail, set the environment variable `DTF_LOG_LEVEL=DEBUG` on the cluster or job, or call
`configure_logging(level="DEBUG")` from `wilsonelser.transformation.utils.logging_utils` before creating the
classes.

**Passing your own DataFrame.** `write_table(table_id, df)` writes `df` instead of reading the target's
`source_type`/`source_id`. Use it when part of the logic lives in your notebook.

**Paths.** Pass config and env file paths relative to the repo root; see
[Organizing a pipeline](#organizing-a-pipeline).

**Installing the framework.**

- **Jobs:** define them in the pipeline repo's bundle, next to the pipeline
  (`use_cases/<domain>/resources/<name>.job.yml`), running the pipeline's notebook. The notebook installs the
  wheel as below.
- **Notebooks:** install the released wheel from the shared libraries folder, then restart Python:

  ```python
  %pip install /Workspace/Shared/libraries/wilsonelser-dtf/wilsonelser_dtf-<version>-py3-none-any.whl
  %restart_python
  ```

  Each version stays in that folder, so pin the version your notebook was tested with.

## Config reference

Types: *str* string, *list* YAML list, *map* YAML mapping, *bool* `true`/`false`. SQL expressions are
Spark SQL, the same as in `selectExpr` or a `WHERE` clause.

### sources

```yaml
sources:
  - source_id: "customers"                    # required, unique
    table: "${catalog}.${schema}.customers"   # required unless subquery is set
    read_type: "table"                        # table (default) | stream | cdf
    transformation_id: "prepare_customers"    # optional, applied right after reading
```

| Key | Type | Required | Description |
|---|---|---|---|
| `source_id` | str | yes | Id used by `combine`, `targets` and `read_source_table` |
| `table` | str | yes, unless `subquery` | Three-part table name |
| `read_type` | str | no, default `table` | `table`: batch read. `stream`: `spark.readStream` on the Delta table. `cdf`: batch read of the Change Data Feed |
| `stream_options` | map | no | Passed to `readStream.options(...)` when `read_type: stream`, e.g. `maxFilesPerTrigger: 1` |
| `cdf_options` | map | no | Passed to the reader when `read_type: cdf`, e.g. `startingVersion: 1` |
| `subquery` | str | no | Spark SQL to run instead of reading `table`. The result is also registered as a temp view named after `source_id` |
| `transformation_id` | str | no | A transformation `id` to apply to the result |

A change-feed source:

```yaml
sources:
  - source_id: "customer_changes"
    table: "${catalog}.${schema}.customers"
    read_type: "cdf"
    cdf_options:
      startingVersion: 1
      endingVersion: 5
```

A SQL source:

```yaml
sources:
  - source_id: "loyalty_totals"
    subquery: |
      SELECT customer_id, SUM(customer_loyalty_points) AS total_loyalty_points
      FROM ${catalog}.${schema}.customer_mapics
      GROUP BY customer_id
```

### transformations

```yaml
transformations:
  - id: "prepare_sales"            # required, unique
    additional_columns: [...]      # any of the operations below, in any order
    filters: [...]
```

**Operations run in the order you write them.** In the example above, `additional_columns` runs before
`filters`, so a filter can use a column added by `additional_columns`. Reorder the keys to change the order.

| Operation | Type | What it does |
|---|---|---|
| [`additional_columns`](#additional_columns) | list | Add columns from SQL expressions, cast to a type |
| [`column_expressions`](#column_expressions) | list | Select or derive columns with SQL expressions |
| [`columns_expressions`](#columns_expressions) | map | Add or replace columns: `name: expression` |
| [`column_concatenations`](#column_concatenations) | list | Join several expressions into one string column |
| [`column_rename`](#column_rename) | list | Rename columns one by one |
| [`columns_mapping`](#columns_mapping) | map | Rename many columns: `old: new` |
| [`columns_to_snake_case`](#columns_to_snake_case) | bool | Rename all columns to snake_case |
| [`columns_to_select`](#columns_to_select) | list | Keep only these columns |
| [`filters`](#filters) | list | Keep rows matching every condition |
| [`drop_duplicates`](#drop_duplicates) | list or bool | Remove duplicate rows |
| [`distinct_columns`](#distinct_columns) | list | Distinct values of some columns |
| [`custom_functions`](#custom_functions) | list | Apply your own Python functions |
| [`transformation_sql_file`](#transformation_sql_file) | str | Build the DataFrame from a SQL file |

#### additional_columns

```yaml
transformations:
  - id: "audit_columns"
    additional_columns:
      - name: "processed_date"
        type: "timestamp"
        value: "current_timestamp()"
      - name: "source_system"
        type: "string"
        value: "'MAPICS'"            # quote string literals inside the expression
```

`value` is a SQL expression and can use existing columns, e.g. `regexp_replace(state, ' ', '')`.
`type` is any Spark SQL type.

#### column_expressions

```yaml
transformations:
  - id: "pricing"
    column_expressions:
      - column: "*"                  # keep all existing columns
        expression: "*"
      - column: "total_price"
        expression: "quantity * price"
      - column: "price_category"
        expression: "CASE WHEN price > 30 THEN 'High' ELSE 'Low' END"
```

- **With a `column: "*"` entry,** all columns are kept. An entry whose `column` already exists replaces
  that column in place.
- **Without it,** the result has only the listed columns.

#### columns_expressions

The map form: each key is a column to add or replace.

```yaml
transformations:
  - id: "clean_ids"
    columns_expressions:
      order_id: "lower(trim(order_id))"
      customer_id: "trim(customer_id)"
```

#### column_concatenations

```yaml
transformations:
  - id: "region_label"
    column_concatenations:
      - target_column: "region_state"
        columns:
          - expression: "trim(region)"
          - expression: "trim(state)"
        delimiter: " - "
```

Uses `concat_ws`, so null values are skipped rather than making the whole result null.

#### column_rename

```yaml
transformations:
  - id: "rename"
    column_rename:
      - column: "customer_id_mapics"
        target_column: "customer_id"
```

#### columns_mapping

```yaml
transformations:
  - id: "rename_jde"
    columns_mapping:
      SDDOC: invoice_number
      SDDCT: document_type
```

#### columns_to_snake_case

```yaml
transformations:
  - id: "snake"
    columns_to_snake_case: true      # CustomerID -> customer_id
```

Check the result when column names contain symbols. Two names can end up the same (`CustID` and
`cust_id`), and a name made only of symbols becomes empty.

#### columns_to_select

```yaml
transformations:
  - id: "narrow"
    columns_to_select: ["order_id", "customer_id"]
```

#### filters

```yaml
transformations:
  - id: "open_items"
    filters:
      - condition: "Entity != '1606'"
      - condition: "TR_REC_ID in ('AC', 'AI', 'AB', 'AT')"
```

A row is kept only if it matches every condition.

#### drop_duplicates

```yaml
transformations:
  - id: "dedupe_by_keys"
    drop_duplicates:
      - column: "order_id"
      - column: "customer_id"
  - id: "dedupe_all"
    drop_duplicates: true            # compare all columns
```

Keeps one row per key and **all columns**.

#### distinct_columns

```yaml
transformations:
  - id: "unique_products"
    distinct_columns:
      - column: "sales_id"
      - column: "product_id"
```

Unlike `drop_duplicates`, the result has **only the listed columns**. Use `- column: "*"` for a
distinct over all columns.

#### custom_functions

See [Custom functions](#custom-functions).

#### transformation_sql_file

```yaml
transformations:
  - id: "customer_sales_sql"
    transformation_sql_file: "use_cases/sales/sqls/customer_sales.sql"
```

Runs the SQL file (with placeholders resolved) to create the DataFrame, then applies the other operations
in the transformation. Call it without a DataFrame: `apply_transformations("customer_sales_sql")`.
Passing a DataFrame as well is an error.

### combine

A combine has exactly one of `joins` or `unions`, each a **mapping** (not a list). Their inputs can be
sources or other combines, see [Chaining combines](#chaining-combines).

#### joins

```yaml
combine:
  - combine_id: "customer_sales"
    joins:
      source_ids:
        - source_id: "customers"
          alias: "c"
          join_order: 1
        - source_id: "sales"
          alias: "s"
          join_order: 2
          join_type: "left"
        - source_id: "stores"
          alias: "st"
          join_order: 3
      join_conditions:
        - left: "c"
          right: "s"
          condition: "c.customer_id = s.customer_id"
        - left: "s"
          right: "st"
          condition: "s.store_id = st.store_id"
      select_columns:
        c: ["customer_id", "customer_name"]
        s: ["transaction_id", "store_id", "price"]
        st: ["store_name"]
      post_join_filters:
        - condition: "price < 50"
      row_operations:
        partition_by: ["customer_id"]
        order_by:
          - column: "price"
            order: "desc"
        rank_column: "row_number"
        filter_condition: "row_number = 1"
      alias: "customer_sales"
```

| Key | Type | Description |
|---|---|---|
| `source_ids` | list | Sources to join, at least two. Each has `source_id`, `alias`, `join_order` (joined in ascending order) and optional `join_type`: `inner` (default), `left`, `right`, `full`. The join type goes on the source being joined in, not the first one |
| `join_conditions` | list | One per joined source: `left` (an alias already joined), `right` (the alias being joined) and a SQL `condition` using the aliases |
| `select_columns` | map | Optional. Alias → columns to keep. Aliases not listed keep all their columns; without `select_columns`, every column of every input is kept. When a column name appears in more than one alias (e.g. the join key), the first is kept and the others are skipped (logged). Listing an alias that isn't joined is an error |
| `post_join_filters` | list | `condition`s applied after the join and `select_columns`; use the output column names |
| `row_operations` | map | Keep the top rows per group: `partition_by`, `order_by` (`column` and `order`: `asc` or `desc`), `rank_column` (default `row_number`), `filter_condition` (default `row_number = 1`) |
| `alias` | str | Alias for the joined result |

#### unions

```yaml
combine:
  - combine_id: "all_stores"
    unions:
      source_ids: ["store_region_1", "store_region_2"]
      allow_missing: true          # fill columns missing in some sources with nulls
      distinct: true               # remove duplicate rows after the union
```

Sources are matched by column name (`unionByName`).

#### Chaining combines

An input in `source_ids` can be a `source_id` or the `combine_id` of another combine, so combines build on each
other. To union two regions and then join sales to the result, use two combines:

```yaml
combine:
  - combine_id: "all_customers"
    unions:
      source_ids: ["customers_region_1", "customers_region_2"]

  - combine_id: "customer_sales"
    joins:
      source_ids:
        - source_id: "all_customers"     # the union above
          alias: "c"
          join_order: 1
        - source_id: "sales"
          alias: "s"
          join_order: 2
      join_conditions:
        - left: "c"
          right: "s"
          condition: "c.customer_id = s.customer_id"
      select_columns:
        c: ["customer_id", "customer_name"]
        s: ["transaction_id", "price"]
```

Running `customer_sales` (for example as a target's `source_id` with `source_type: "combine"`) builds
`all_customers` first. Chains can be any depth, in either direction. Ids must be unique across `sources` and
`combine`, and combines can't refer to each other in a loop.

### targets

```yaml
targets:
  - table_id: "silver_customers"                 # required, unique
    table: "${catalog}.${schema}.customers"      # required
    write_type: "table"                          # table (default) | stream
    write_mode: "overwrite"                      # see below
    source_type: "sources"                       # sources | combine
    source_id: "customers"                       # a source_id or combine_id
    cluster_by: ["customer_id"]
    options:
      mergeSchema: "true"
    schema_file: "use_cases/sales/schemas/customers.json"
```

| Key | Type | Description |
|---|---|---|
| `table_id` | str | Id passed to `write_table` |
| `table` | str | Three-part table name |
| `write_type` | str | `table` (batch, default) or `stream` |
| `write_mode` | str | Batch: `append` (default), `overwrite`, `merge`. Stream: `append`, `update`, `complete` |
| `source_type`, `source_id` | str | What to write when `write_table` gets no DataFrame: a `sources` entry or a `combine` entry |
| `partition_by` / `cluster_by` | list | Applied when the table is created. Use one, not both; `partition_by` wins if both are set |
| `options` | map | Passed to the Spark writer. For streams, also holds `trigger` (see below) |
| `schema_file` | str | Spark StructType JSON, used to create the table |
| `table_properties` | map | Delta table properties, e.g. `delta.autoOptimize.optimizeWrite: "true"`. Checked on every write: only missing or different values are set, so config changes reach existing tables. Properties removed from the config stay on the table |

**First write.** If the table doesn't exist, it is created empty (with `partition_by`/`cluster_by`),
then the data is written with `write_mode`. The table's schema comes from, in order: the existing table,
`schema_file`, then the DataFrame being written. Partition and cluster columns must exist in that schema.

#### Merge

```yaml
targets:
  - table_id: "silver_customer_sales"
    table: "${catalog}.${schema}.customer_sales"
    write_mode: "merge"
    source_type: "combine"
    source_id: "customer_sales"
    merge_source_alias: "source"        # default "source"
    target_alias: "target"              # default "target"
    merge_condition: "target.customer_id = source.customer_id"
    merge_actions:
      when_matched:
        update:
          - "customer_name = source.customer_name"
      when_not_matched:
        insert: ["customer_id", "customer_name"]
```

- **`update` entries** are `column = expression`, split on the first `=` with any spacing, so the expression
  can contain `=` itself (e.g. `flag = CASE WHEN source.x = 1 THEN 'Y' END`).
- **`insert`** lists the columns to copy from the source for new rows.

#### Streaming targets

```yaml
global_config:
  base_checkpoint_location: "/Volumes/${catalog}/${schema}/checkpoints"
  query_name_prefix: "customer_stream"

targets:
  - table_id: "customers_stream"
    table: "${catalog}.${schema}.customers_stream"
    write_type: "stream"
    write_mode: "append"
    source_type: "sources"
    source_id: "customers_raw"           # a source with read_type: stream
    options:
      trigger: "availableNow"            # availableNow (default) | processingTime=10 seconds | continuous=1 second
```

- **Checkpoint:** stored at `<base_checkpoint_location>/<table_id>`.
- **Query name:** `<query_name_prefix>_<table_id>`.
- **Triggers:**
  - `availableNow` (the default): processes everything available, then stops. Use it for scheduled runs.
  - `processingTime=<interval>` or `continuous=<interval>`: the query keeps running until it's stopped.
  - `once` is deprecated in Spark. It still works, runs as `availableNow`, and logs a warning.
  - `trigger` only configures the query; it isn't passed to Spark as a writer option.
- **Waiting:** `write_table` waits for the query to finish, logging progress every 30 seconds. Two optional
  target keys change that, for example for a bounded run or to run several streams from one notebook:

  ```yaml
  targets:
    - table_id: "customers_stream"
      table: "${catalog}.${schema}.customers_stream"
      write_type: "stream"
      options:
        trigger: "processingTime=10 seconds"
      timeout_seconds: 3600     # stop the query cleanly after an hour and return (needs wait: true)
  ```

  | Key | Default | Effect |
  |---|---|---|
  | `timeout_seconds` | none | Stop the query cleanly after this many seconds, then return normally |
  | `wait` | `true` | `false`: start the query and return it (`StreamingQuery`) without waiting; manage it yourself, e.g. `spark.streams.awaitAnyTermination()` |

  A query that fails raises its error, and is stopped.
- **`for_each_batch_function: "package.module.function"`** runs your own `(batch_df, batch_id)` function
  for each micro-batch instead of writing directly.

### global_config

| Key | Description |
|---|---|
| `base_checkpoint_location` | Required for streaming targets; usually a Unity Catalog volume |
| `query_name_prefix` | Prefix for streaming query names (default `default_query`) |

## Custom functions

Put custom functions in one module per domain and reference them by their full import path:

```
use_cases.custom_transformations.<domain>_tx.<function_name>
```

| Part | Convention | Example |
|---|---|---|
| Package | always `use_cases.custom_transformations` | |
| Module | `<domain>_tx`, one per domain, lowercase | `sales_tx`, `r2r_tx`, `hn_tx` |
| Function | snake_case verb phrase describing the result | `calculate_total_price`, `remove_leading_trailing_spaces` |

Functions shared by several domains go in `common_tx`. The package is importable because the repo root is
on `sys.path` (see [Organizing a pipeline](#organizing-a-pipeline)).

There are two kinds of function:

```yaml
transformations:
  - id: "custom"
    custom_functions:
      # Column function: takes Columns, returns a Column, written to output_column
      - name: "use_cases.custom_transformations.sales_tx.calculate_total_price"
        column_mappings:
          - input_columns: ["price", "quantity"]
            output_column: "total_price"
      # DataFrame function: takes and returns a DataFrame; kwargs are passed as keyword arguments
      - name: "use_cases.custom_transformations.common_tx.remove_leading_trailing_spaces"
```

```python
# use_cases/custom_transformations/sales_tx.py
from pyspark.sql import Column

def calculate_total_price(price: Column, quantity: Column) -> Column:
    return price * quantity
```

```python
# use_cases/custom_transformations/common_tx.py
from pyspark.sql import DataFrame

def remove_leading_trailing_spaces(df: DataFrame) -> DataFrame:
    ...
```

## Known limitations

- **`source_alias` is not a key.** Use `merge_source_alias`; older configs with `source_alias` only work
  because the default is also `source`.

## Troubleshooting

| Error | Cause and fix |
|---|---|
| `No value for placeholder '${catalog}'` | Add `catalog` to the env file you pass, or set a `catalog` environment variable |
| `Workspace ... must be listed exactly once in config/env/environments.yaml` | Add the workspace URL under its environment, or remove a duplicate |
| `Invalid pipeline config ...` | Listed problems: a missing or duplicate id, or a reference to an id that isn't defined |
| `No YAML files found in config folder` / `... in more than one file` | A config folder with no `*.yml`/`*.yaml` files, or two files setting the same key |
| `Config file '...' not found. Attempted paths: ...` | Write the path relative to the repo root, and check the repo root is on `sys.path`; the error lists every location tried |
| `ModuleNotFoundError: No module named 'use_cases'` | The repo root isn't on `sys.path`, see [Organizing a pipeline](#organizing-a-pipeline) |
| `TABLE_OR_VIEW_NOT_FOUND` / `SCHEMA_NOT_FOUND` | Placeholders resolved to the wrong catalog or schema; check the env file first, it wins over environment variables |
| `COLUMN_NOT_FOUND_IN_SCHEMA` when creating a table | A `partition_by` or `cluster_by` column isn't in the data |
| `No join condition found for <alias>` | Each joined source needs a `join_conditions` entry with `right: <alias>` and a `left` alias that's already joined |
| `select_columns lists aliases that aren't joined` | An alias in `select_columns` doesn't match any `alias` in `source_ids` |
| `'unions' in combine '<id>' must be a mapping` | Write `unions:` as a mapping with `source_ids`, not a list of entries |
| `Source '<id>' is not defined in the config's sources` | A `source_id` (in a call, a combine or a target) doesn't match any `sources` entry; the error lists the defined ones |
| `Combine configuration not found` / `Configuration not found for table_id` | The id in the call doesn't match the config |
| `Combine '<id>' must define exactly one of 'unions' or 'joins'` | Split the combine in two and [chain them](#chaining-combines), or add the missing section |
| `Combines refer to each other in a loop: a -> b -> a` | A combine uses itself as an input, directly or through others |
| `'<id>' is both a source_id and a combine_id` | Rename one of them; ids must be unique across `sources` and `combine` |
| `write_table` never returns | A streaming target with a `processingTime` or `continuous` trigger runs until stopped; use `availableNow`, or set `timeout_seconds` or `wait: false` |
| `'timeout_seconds' ... needs 'wait: true'` / `must be a positive number` | Use a positive number of seconds, and don't combine it with `wait: false` |
| `Unsupported trigger '...'` | Use `availableNow`, `processingTime=<interval>` or `continuous=<interval>` |
