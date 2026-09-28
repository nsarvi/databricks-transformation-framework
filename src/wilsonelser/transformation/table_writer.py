from struct import Struct
import time
from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils
from wilsonelser.transformation.data_transformer import DataTransformer
from delta.tables import DeltaTable
from typing import Optional, Dict, Any, overload, Callable
import importlib

class TableWriter(DataTransformer):

    # How often a waiting streaming write logs progress
    STREAM_PROGRESS_INTERVAL_SECONDS = 30

    def __init__(self, config_file: str, env_config_path: Optional[str] = None):
        super().__init__(config_file, env_config_path)


    @staticmethod
    def _resolve_function(function_path: str):
        """Dynamically resolves a function from a string path."""
        module_name, function_name = function_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, function_name)

    def _apply_trigger(self, writer, trigger_option: Optional[str]):
        """Applies the target's `trigger` option to a streaming writer.

        availableNow (the default) processes all available data and stops. "once" is deprecated in Spark and
        runs as availableNow. processingTime=<interval> and continuous=<interval> keep the query running.
        """
        if trigger_option is None or trigger_option == "availableNow":
            return writer.trigger(availableNow=True)
        if trigger_option == "once":
            self.logger.warning("Trigger 'once' is deprecated in Spark; running as 'availableNow'. Update the config.")
            return writer.trigger(availableNow=True)
        if trigger_option.startswith("processingTime="):
            return writer.trigger(processingTime=trigger_option.split("=", 1)[1].strip())
        if trigger_option.startswith("continuous="):
            return writer.trigger(continuous=trigger_option.split("=", 1)[1].strip())
        raise ValueError(
            f"Unsupported trigger '{trigger_option}'. Use availableNow, processingTime=<interval> or continuous=<interval>"
        )

    def _stream_wait_settings(self, table_id: str) -> tuple:
        """Reads and validates a streaming target's `wait` (default true) and `timeout_seconds` (default none)."""
        target_config = self.target_lookup[table_id]
        wait = target_config.get(YC.STREAM_WAIT_KEY, True)
        timeout_seconds = target_config.get(YC.STREAM_TIMEOUT_SECONDS_KEY)
        if not isinstance(wait, bool):
            raise ValueError(f"'wait' for target '{table_id}' must be true or false, got: {wait!r}")
        if timeout_seconds is not None:
            if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
                raise ValueError(f"'timeout_seconds' for target '{table_id}' must be a positive number, got: {timeout_seconds!r}")
            if not wait:
                raise ValueError(f"'timeout_seconds' for target '{table_id}' needs 'wait: true'")
        return wait, timeout_seconds

    def _await_stream(self, query: StreamingQuery, table_name: str, timeout_seconds: Optional[float]) -> None:
        """Waits for a streaming query to finish, logging progress. With a timeout, stops it cleanly when the
        time is up. A failed query raises its error."""
        deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
        while True:
            interval = self.STREAM_PROGRESS_INTERVAL_SECONDS
            if deadline is not None:
                interval = max(0, min(interval, deadline - time.monotonic()))
            if query.awaitTermination(interval):
                self.logger.info("Streaming query for %s finished", table_name)
                return
            progress = query.lastProgress
            if progress:
                self.logger.info("Streaming query for %s: %s rows in the last batch", table_name, progress["numOutputRows"])
            if deadline is not None and time.monotonic() >= deadline:
                self.logger.info("Stopping the streaming query for %s after %s seconds (timeout_seconds)", table_name, timeout_seconds)
                query.stop()
                return

    @staticmethod
    def _parse_update_assignment(assignment: str) -> tuple:
        """Splits a merge `update` entry such as "name = source.name" into (column, expression).

        Splits on the first "=", with any spacing, so the expression may contain "=" itself
        (e.g. "flag = CASE WHEN source.x = 1 THEN 'Y' END").
        """
        column, separator, expression = str(assignment).partition("=")
        column, expression = column.strip(), expression.strip()
        if not separator or not column or not expression:
            raise ValueError(f"Merge update entry must look like '<column> = <expression>', got: {assignment!r}")
        return column, expression

    @staticmethod
    def _property_value(value: Any) -> str:
        """Table property values as Delta expects them: YAML true/false become "true"/"false"."""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _apply_table_properties(self, table_id: str) -> None:
        """Sets the target's `table_properties` on its table, changing only properties that differ.

        Runs on every write, so config changes reach existing tables; properties removed from the config
        are left on the table.
        """
        target_config = self.target_lookup[table_id]
        desired = {
            str(key): self._property_value(value)
            for key, value in (target_config.get(YC.TABLE_PROPERTIES_KEY) or {}).items()
        }
        if not desired:
            return
        table_name = target_config.get(YC.TABLE_KEY)
        current = {row["key"]: row["value"] for row in self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()}
        changed = {key: value for key, value in desired.items() if current.get(key) != value}
        if not changed:
            return
        quote = lambda text: "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"
        assignments = ", ".join(f"{quote(key)} = {quote(value)}" for key, value in changed.items())
        self.logger.info("Setting table properties on %s: %s", table_name, changed)
        self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({assignments})")

    def _get_table_schema(self, table_id: str, df: Optional[DataFrame] = None) -> StructType:
        # Get the table schema in the following order of preference
        # 1) Schema config file defined
        # 2) Existing target table schema - unfortunately I can't reuse the source reader without some refactoring
        #    as it looks for the source key in the config
        # 3) dataframe to be written schema

        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")
        table_name = target_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(f"Table name not found in configuration for table_id: {table_id}")
        schema_file = target_config.get(YC.SCHEMA_FILE_KEY)
        schema = None
        if self.spark.catalog.tableExists(table_name):
            self.logger.info(f"Getting schema from existing table {table_name}")
            schema = self.spark.table(table_name).schema
        elif schema_file:
            self.logger.info(f"Getting schema from schema file {schema_file} for {table_name}")
            schema = ConfigUtils.get_schema(schema_file)
        elif df:
            self.logger.info(f"Getting schema from dataframe to be written to {table_name}")
            schema = df.schema
        else:
            raise ValueError("Cannot get schema from schema file, existing table, nor dataframe.")
        return schema

    def _log_latest_operation_metrics(self, table_name: str) -> None:
        """Logs the latest history entry for the given table name."""
        metrics = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").select(
                F.get_json_object(F.to_json(F.col("operationMetrics")), "$.numOutputRows").alias("num_records")
            ).collect()[0]
        num_records = int(metrics["num_records"]) if metrics["num_records"] else 0
        self.logger.info(f"Number of records written to table {table_name}: {num_records}")
        
    @overload
    def _create_table(self, table_id: str) -> None: ...
    
    @overload
    def _create_table(self, table_id: str, df: DataFrame) -> None: ...
    
    def _create_table(self, table_id: str, df: Optional[DataFrame] = None) -> None:
        """Creates an empty target table from the schema file, or the DataFrame's schema as a fallback.

        Only the table is created here; the data is written by write_table. A batch write of an
        empty DataFrame is used for both table and stream targets.
        """
        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")
        table_name = target_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(f"Table name not found in configuration for table_id: {table_id}")
        if self.spark.catalog.tableExists(table_name):
            self.logger.info(f"Table {table_name} already exists. Skipping creation.")
            return

        self.logger.debug(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_id}")
        self.logger.debug(f"Table configuration: {target_config}")

        schema = self._get_table_schema(table_id, df)
        writer = self.spark.createDataFrame([], schema).write.format("delta").mode(YC.WRITE_MODE_APPEND)
        partition_by = target_config.get(YC.PARTITION_BY_KEY, [])
        cluster_by = target_config.get(YC.CLUSTER_BY_KEY, [])
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        elif cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table_name)
        self.logger.info(f"Table {table_name} created successfully.")
     
     
    def _get_source_df(self, source_id: str, source_type: str) -> DataFrame:
        """
        Retrieves the source DataFrame based on the provided source ID and source type.

        Returns:
            DataFrame: The DataFrame corresponding to the specified source ID and source type.

        Raises:
            ValueError: If the provided source type is not supported.
        """
        """Gets the source DataFrame for the merge operation based on the merge_source_id and merge_source_type configuration."""
        if source_type == YC.COMBINE_KEY:
            return self.apply_combine(source_id)
        elif source_type == YC.SOURCES_KEY:
            return self.read_source_table(source_id)
        else:
            raise ValueError(f"Unsupported merge or source type: {source_type}. Supported types are: {YC.COMBINE_KEY}, {YC.SOURCES_KEY}")

    def merge_into_target(self, table_id: str, source_df: Optional[DataFrame] = None):
        """
        Perform a merge operation into the target Delta table based on the provided table_id.
        This method looks up the target configuration using the given table_id, retrieves the source DataFrame for the merge,
        and performs the merge operation based on the specified conditions and actions.
        Args:
            table_id (str): The identifier for the target table configuration.
            source_df (Optional[DataFrame]): The source DataFrame; read from the configured source when not provided.
        Raises:
            ValueError: If the configuration for the given table_id is not found.
        Notes:
            - The target configuration should contain the necessary keys for table name, merge source type, merge source ID,
            merge condition, merge actions, merge source alias, and target alias.
            - The merge actions should specify the update expressions for matched rows and insert expressions for not matched rows.
        """
        # Lookup the table from the given table_id and create a table with all the properties for options and partitions
        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")

        table_name = target_config.get(YC.TABLE_KEY)
        merge_source_type = target_config.get(YC.SOURCE_TYPE_KEY)
        merge_source_id = target_config.get(YC.SOURCE_ID_KEY)
        merge_condition = target_config.get(YC.MERGE_CONDITION_KEY)
        merge_actions = target_config.get(YC.MERGE_ACTIONS_KEY, {})
        merge_source_alias = target_config.get(YC.MERGE_SOURCE_ALIAS_KEY, "source")
        target_alias = target_config.get(YC.TARGET_ALIAS_KEY, "target")

        # Get the source DataFrame for the merge operation
        if source_df is None:
            source_df = self._get_source_df(merge_source_id, merge_source_type)

        when_matched = merge_actions.get(YC.WHEN_MATCHED_KEY, {}).get(YC.UPDATE_KEY, [])
        when_not_matched = merge_actions.get(YC.WHEN_NOT_MATCHED_KEY, {}).get(YC.INSERT_KEY, [])

        delta_table = DeltaTable.forName(self.spark, table_name)
        merge_builder = delta_table.alias(target_alias).merge(
            source_df.alias(merge_source_alias),
            merge_condition
        )

        if when_matched:
            update_expr: Dict[str, Any] = dict(
                (column, F.expr(expression)) for column, expression in map(self._parse_update_assignment, when_matched)
            )
            merge_builder = merge_builder.whenMatchedUpdate(set=update_expr)

        # Handling not matched inserts
        if when_not_matched:
            insert_expr : Optional[Dict[str, Any]] = {col: F.col(f"{merge_source_alias}.{col}") for col in when_not_matched}
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_expr)

        merge_builder.execute()

    @overload
    def write_table(self, table_id: str) -> Optional[StreamingQuery]: ...

    @overload
    def write_table(self, table_id: str, df: DataFrame) -> Optional[StreamingQuery]: ...


    def write_table(self, table_id: str, df: Optional[DataFrame] = None) -> Optional[StreamingQuery]:
        """Writes the DataFrame, or the configured source when none is given, to the target table based on the table_id.

        The target table is created first when it does not exist, then the data is always written,
        so the first run behaves the same as later runs for append, overwrite, merge and stream.

        Streaming targets wait for the query to finish (or `timeout_seconds` to pass) and return None. With
        `wait: false`, the query is started and returned without waiting, for the caller to manage.
        """

        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")

        table_name = target_config.get(YC.TABLE_KEY)
        write_type = target_config.get(YC.WRITE_TYPE_KEY, YC.WRITE_TYPE_TABLE)
        write_mode = target_config.get(YC.WRITE_MODE_KEY, YC.WRITE_MODE_APPEND)
        options = target_config.get(YC.OPTIONS_KEY, {})

        # Read the configured source when no DataFrame is passed in
        if df is None:
            source_type = target_config.get(YC.SOURCE_TYPE_KEY)
            source_id = target_config.get(YC.SOURCE_ID_KEY)
            df = self._get_source_df(source_id, source_type)

        # Create the target table (schema only) before writing to it
        if not self.spark.catalog.tableExists(table_name):
            self.logger.info(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_id}")
            self._create_table(table_id, df)
        self._apply_table_properties(table_id)

        if write_type == YC.WRITE_TYPE_TABLE:
            self.logger.debug(f"Writing DataFrame to target table with table_id: {table_id}")
            if write_mode == YC.WRITE_MODE_MERGE:
                self.merge_into_target(table_id, df)
            else:
                writer = df.write.mode(write_mode).options(**options)
                writer.saveAsTable(table_name)
            self._log_latest_operation_metrics(table_name)
        elif write_type == YC.WRITE_TYPE_STREAM:
            self.logger.info(f"Writing DataFrame to target stream with table_id: {table_id}")
            checkpoint = self._get_checkpoint_location(table_id)
            if not checkpoint:
                raise ValueError(f"Checkpoint location is missing for table_id: {table_id}")

            query_prefix = self.global_config.get(YC.QUERY_NAME_PREFIX_KEY, "default_query")
            query_name = f"{query_prefix}_{table_id}"

            # Check for for_each_batch_function
            for_each_batch_function_name = target_config.get(YC.FOR_EACH_BATCH_FUNCTION_KEY)
            wait, timeout_seconds = self._stream_wait_settings(table_id)
            # Set before the try so the finally block can check it even if the query never starts
            streaming_query = None
            leave_running = False
            # Start streaming query
            try:
                # Copy the options so the loaded config isn't changed; `trigger` configures the query, not the writer
                options = dict(target_config.get(YC.OPTIONS_KEY, {}))
                trigger_option = options.pop("trigger", None)
                options["checkpointLocation"] = checkpoint

                query = df.writeStream \
                        .outputMode(write_mode) \
                        .options(**options) \
                        .queryName(query_name)
                # Resolve the function if it's provided as a string
                if for_each_batch_function_name and isinstance(for_each_batch_function_name, str):
                    for_each_batch_function: Callable[[DataFrame, int], None]  = self._resolve_function(for_each_batch_function_name)
                    # Log the function name
                    function_name = getattr(for_each_batch_function, "__name__", str(for_each_batch_function))
                    self.logger.info(f"Using for_each_batch_function '{function_name}' for table: {table_name}")
                    if not callable(for_each_batch_function):
                        raise ValueError(f"The provided for_each_batch_function '{for_each_batch_function}' is not a function, provide provide function name.")
                    query = query.foreachBatch(for_each_batch_function) 

                query = self._apply_trigger(query, trigger_option)

                self.logger.info(f"Starting streaming query for table: {table_name} with query name: {query_name}")
                self.logger.debug(f"Streaming query options: {options}")
            
                streaming_query = query.toTable(table_name)
                if not wait:
                    self.logger.info("Started streaming query %s for %s; not waiting for it", query_name, table_name)
                    leave_running = True
                    return streaming_query
                self._await_stream(streaming_query, table_name, timeout_seconds)
            except Exception as e:
                self.logger.error(f"Error while running streaming query for table {table_name}: {str(e)}")
                raise
            finally:
                if streaming_query is not None and not leave_running and streaming_query.isActive:
                    streaming_query.stop()
                    self.logger.info(f"Streaming query stopped for table: {table_name}")
       
        else:
            raise ValueError(f"Unsupported write type: {write_type}")

    
