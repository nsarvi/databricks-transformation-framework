# Integration Framework

This framework performs reads, transformes and writes to a Delta table based on the connfigurations provided

## Build 

To develop and build a binary distribution of the framework, follow the steps

git checkout <git-url>

Setup the Python Virtual environment

source .venv/bin/activate

python -m build --wheel

This should generate  adm_ingestion_framework-1.0.0-py3-none-any.whl

## Usage

Notebook scoped Usage

```
%pip install /Workspace/Common-Libs/adm_ingestion_framework-1.0.0-py3-none-any.whl

%restart_python

src_path = (Path.cwd() ).as_posix()
sys.path.append(src_path)
print(src_path)
config_path = Path(src_path) / "configs/config.yaml"

```
## Configurations

Example Config

```
# Configurations for the source table(s)
sources:
  - source_id: "source_1"
    table: "sandbox.integration_framework.customer_mapics"
    transformation_id: "transformation_id_1"
    read_type: "table"  # Options: "table" for static table, "stream" for streaming source, "cdf" for Change Data Feed

  - source_id: "source_2"
    table: "sandbox.integration_framework.sales_mapics"
    transformation_id: "transformation_id_2"
    read_type: "table"  # Options: "table" for static table, "stream" for streaming source, "cdf" for Change Data Feed



# Transformatin will be applied on the target tables      
transformations:
  # Unique name for the transformation 
  - id: "transformation_id_1"
    # Additional columns to target table
    additional_columns:          
      - name: "processed_date"
        type: "timestamp"
        value: "current_timestamp()"
      - name: "source_system"
        type: "string"
        value: "'MAPICS'"
    column_rename:
      - column: "customer_email_mapics"
        target_column: "customer_email"

  - id: "transformation_id_2"
    # Additional columns to target table
    additional_columns:          
      - name: "source_system"
        type: "string"
        value: "'MDM'"
    column_rename:
      - column: "customer_id_mapics"
        target_column: "customer_id"

combine:
  - combine_id: "combine_id_1"
    joins:
      join_id: "join_id_1"
      source_ids: 
        - source_id: "source_1"
          alias: "c"
          join_order: 1
        - source_id: "source_2"
          alias: "s"
          join_order: 2
      join_type: "inner"
      join_condition: ["customer_id"]
      select_columns:
        c: ["customer_id", "customer_name", "customer_address", "customer_phone_mapics", "customer_email"]
        s: ["customer_id","transaction_id", "product_id_mapics", "store_id_mapics", "quantity", "price"]
      post_join_filters:
        - condition: "s.price < 50"
      alias: "customer_sales"  # Alias for the combined table


# Configurations for the target table(s)
targets:
  - table_id: "target_customer_sales"
    table: "sandbox.integration_framework.customer_sales"
    write_type: "table"
    write_mode: "overwrite"
    partition_by: ["store_id"]  # Clustering and partitioning cannot both be specified
    options:
      compression: "snappy"
      mergeSchema: "true"  # Option to merge schema
      overwriteSchema: "true"  # Option to overwrite schema
    schema_file: "tests/configs/schemas/silver/customer_sales.json"
    table_properties:
      delta.autoOptimize.optimizeWrite: "true"
      delta.autoOptimize.autoCompact: "true"

```

## Source Table Reads

```
    from wilsonelser.transformation.table_reader import TableReader

    table_reader = TableReader("tests/configs/sources/read_tables_config.yaml")
    result_df = table_reader.read_source_table("source_1")

```

## Transformations

```
    transformer = BaseDataTransformer("tests/configs/transformers/additional_column_config.yaml")
    transformation_id = "transformation_id_1"
    transformed_df = transformer.apply_transformations(transformation_id, self.sample_dataframe,)

```
## Joins

```
    src_path = (Path.cwd() ).as_posix()
    sys.path.append(src_path)
    print(src_path)
    config_path = Path(src_path) / "configs/joins_config.yaml"
    transformer = DataTransformer(config_path.as_posix())

    result_df = transformer.apply_joins("combine_id_1")
```

## Target Table Writes

```     writer = TableWriter("tests/configs/merge/source_config.yaml")
        writer.write_table("source_customer_mapics",customer_df)
```

## Unit Tests

Unit tests are under tests




