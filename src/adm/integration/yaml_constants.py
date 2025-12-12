# Sources section keys
SOURCES_KEY = "sources"
TABLE_ID_KEY = "table_id"
TABLE_KEY = "table"
TABLE_STREAM_KEY = "stream"
TABLE_CDF_KEY = "cdf"
TRANSFORMATION_ID_KEY = "transformation_id"
READ_TYPE_KEY = "read_type"
STREAM_OPTIONS_KEY = "stream_options"
CHECKPOINT_LOCATION_KEY = "checkpoint_location"
CDF_OPTIONS_KEY = "cdf_options"
COLUMN_MAPPING_KEY = "column_mapping"

# Transformations section keys
TRANSFORMATIONS_KEY = "transformations"
ID_KEY = "id"
ADDITIONAL_COLUMNS_KEY = "additional_columns"
TRANSFORMATION_SQL_FILE_KEY = "transformation_sql_file"

# Additional column attributes
COLUMN_NAME_KEY = "name"
COLUMN_TYPE_KEY = "type"
COLUMN_VALUE_KEY = "value"

# Filter condition key
FILTERS_KEY = "filters"
CONDITION_KEY = "condition"

# Drop duplicates keys
DROP_DUPLICATES_KEY = "drop_duplicates"

# Distinct columns keys
DISTINCT_COLUMNS_KEY = "distinct_columns"

# Column expressions keys
COLUMN_EXPRESSIONS_KEY = "column_expressions"
COLUMN_KEY = "column"
EXPRESSION_KEY = "expression"

# Column concatenations keys
COLUMN_CONCATENATIONS_KEY = "column_concatenations"
TARGET_COLUMN_KEY = "target_column"
COLUMNS_KEY = "columns"
DELIMITER_KEY = "delimiter"

# Column rename 
COLUMN_RENAME_KEY = "column_rename"

# Combine section keys
COMBINE_KEY = "combine"
COMBINE_ID_KEY = "combine_id"
UNION_ID_KEY = "union_id"
UNIONS_KEY = "unions"
TABLE_IDS_KEY = "table_ids"
TRANSFORMATION_IDS_KEY = "transformation_ids"
DISTINCT_KEY = "distinct"

JOINS_KEY = "joins"
JOIN_ON_KEY = "on"
JOIN_TYPE_KEY = "type"
JOIN_ID_KEY = "join_id"
SOURCE_ID_KEY = "source_id"
SOURCE_IDS_KEY = "source_ids"
ALIAS_KEY = "alias"
JOIN_ORDER_KEY = "join_order"
JOIN_CONDITIONS_KEY = "join_conditions"
CONDITION_KEY = "condition"
SELECT_COLUMNS_KEY = "select_columns"
FILTER_CONDITIONS_KEY = "filter_conditions"
POST_JOIN_FILTERS_KEY = "post_join_filters"
JOIN_CONDITIONS_LEFT_KEY = "left"
JOIN_CONDITIONS_RIGHT_KEY = "right"
JOIN_TYPE_INNER = "inner"
JOIN_TYPE_LEFT = "left"
JOIN_TYPE_RIGHT = "right"
JOIN_TYPE_FULL = "full"

# Constants for row_operations
ROW_OPERATIONS_KEY = "row_operations"
PARTITION_BY_KEY = "partition_by"
ORDER_BY_KEY = "order_by"
RANK_COLUMN_KEY = "rank_column"
FILTER_CONDITION_KEY = "filter_condition"
COLUMN_KEY = "column"
ORDER_KEY = "order"

# Target section keys
TARGET_TABLE_KEY = "target_table"
TARGET_TABLE_NAME = "name"
TARGET_SCHEMA_FILE = "schema_file"
TARGETS_KEY = "targets"
WRITE_TYPE_KEY = "write_type"
WRITE_MODE_KEY = "write_mode"
PARTITION_BY_KEY = "partition_by"
CLUSTER_BY_KEY = "cluster_by"
OPTIONS_KEY = "options"
SCHEMA_FILE_KEY = "schema_file"
TABLE_PROPERTIES_KEY = "table_properties"
MERGE_SOURCE_ID_KEY = "merge_source_id"
MERGE_SOURCE_TYPE_KEY = "merge_source_type"
MERGE_CONDITION_KEY = "merge_condition"
MERGE_SOURCE_ALIAS_KEY = "merge_source_alias"
TARGET_ALIAS_KEY = "target_alias"
MERGE_ACTIONS_KEY = "merge_actions"
WHEN_MATCHED_KEY = "when_matched"
UPDATE_KEY = "update"
WHEN_NOT_MATCHED_KEY = "when_not_matched"
INSERT_KEY = "insert"

# Write modes
WRITE_MODE_OVERWRITE = "overwrite"
WRITE_MODE_APPEND = "append"
WRITE_MODE_IGNORE = "ignore"
WRITE_MODE_ERROR = "error"

# Write types
WRITE_TYPE_TABLE = "table"
WRITE_TYPE_STREAM = "stream"
WRITE_TYPE_MERGE = "merge"