from struct import Struct
import time
from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from adm.integration import yaml_constants as YC
from adm.integration.utils.config_utils import ConfigUtils
from adm.integration.adm_data_transformer import AdmDataTransformer
from delta.tables import DeltaTable
from typing import Optional, Dict, Any, overload, Callable
import importlib

class AdmTableWriter(AdmDataTransformer):
    
    def __init__(self, config_file: str, env_config_path: Optional[str] = None):
        super().__init__(config_file, env_config_path)


    @staticmethod
    def _resolve_function(function_path: str):
        """Dynamically resolves a function from a string path."""
        module_name, function_name = function_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return getattr(module, function_name)

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
        """Creates a table based on the table_id and optional DataFrame schema."""
        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")
        table_name = target_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(f"Table name not found in configuration for table_id: {table_id}")
        table_exists = self.spark.catalog.tableExists(table_name)
        if table_exists:
            self.logger.info(f"Table {table_name} already exists. Skipping creation.")
            return

        self.logger.debug(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_id}")
        self.logger.debug(f"Table configuration: {target_config}")

        schema = self._get_table_schema(table_id, df)
        if df is None:
            new_df = self.spark.createDataFrame([], schema)
        else:
            new_df = df.select(*schema.names)
        partition_by = target_config.get(YC.PARTITION_BY_KEY, [])
        cluster_by = target_config.get(YC.CLUSTER_BY_KEY, [])
        write_type = target_config.get(YC.WRITE_TYPE_KEY, YC.WRITE_TYPE_TABLE)
        if write_type == YC.WRITE_TYPE_TABLE:
            writer = new_df.write.mode(YC.WRITE_MODE_APPEND).options(**target_config.get(YC.OPTIONS_KEY, {}))
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            elif cluster_by:
                writer = writer.clusterBy(*cluster_by)
            writer = writer.saveAsTable(table_name)
            self.logger.info(f"Table {table_name} created successfully.")
        elif write_type == YC.WRITE_TYPE_STREAM:
            checkpoint = self._get_checkpoint_location(table_id)
            if not checkpoint:
                raise ValueError(f"Checkpoint location is missing for table_id: {table_id}")
            
            options = target_config.get(YC.OPTIONS_KEY, {})
            options.update({"checkpointLocation": checkpoint})
            
            query_name = f"streaming-{table_name}"
            trigger_option = options.get("trigger", "once")  # Default to "once" if not specified
            
            writer = new_df.writeStream \
                .outputMode(YC.WRITE_MODE_APPEND) \
                .options(**options) \
                .queryName(query_name)
            
            # Apply the trigger option
            if trigger_option == "once":
                writer = writer.trigger(once=True)
            elif trigger_option.startswith("processingTime="):
                interval = trigger_option.split("=")[1].strip()
                writer = writer.trigger(processingTime=interval)
            elif trigger_option.startswith("continuous="):
                interval = trigger_option.split("=")[1].strip()
                writer = writer.trigger(continuous=interval)
            else:
                raise ValueError(f"Unsupported trigger option: {trigger_option}")
            
            self.logger.info(f"Starting streaming table creation for {table_name} with query name {query_name}")
            streaming_query = writer.toTable(table_name)
            
            try:
                self.logger.info(f"Waiting for the streaming query to finish for table: {table_name}")
                streaming_query.awaitTermination()
            except Exception as e:
                self.logger.error(f"Error while running streaming query for table {table_name}: {str(e)}")
                raise
            finally:
                if streaming_query and streaming_query.isActive:
                    streaming_query.stop()
                    self.logger.info(f"Streaming query stopped for table: {table_name}")
        else:
            raise ValueError(f"Unsupported write type: {write_type}")
     
     
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

    def merge_into_target(self, table_id: str):
        """
        Perform a merge operation into the target Delta table based on the provided table_id.
        This method looks up the target configuration using the given table_id, retrieves the source DataFrame for the merge,
        and performs the merge operation based on the specified conditions and actions.
        Args:
            table_id (str): The identifier for the target table configuration.
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
        source_df = self._get_source_df(merge_source_id, merge_source_type)

        when_matched = merge_actions.get(YC.WHEN_MATCHED_KEY, {}).get(YC.UPDATE_KEY, [])
        when_not_matched = merge_actions.get(YC.WHEN_NOT_MATCHED_KEY, {}).get(YC.INSERT_KEY, [])

        delta_table = DeltaTable.forName(self.spark, table_name)
        merge_builder = delta_table.alias(target_alias).merge(
            source_df.alias(merge_source_alias),
            merge_condition
        )

        if when_matched:
            update_expr: Optional[Dict[str, Any]] = {col.split(" = ")[0].strip(): F.expr(col.split(" = ", 1)[1].strip()) for col in when_matched}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_expr)

        # Handling not matched inserts
        if when_not_matched:
            insert_expr : Optional[Dict[str, Any]] = {col: F.col(f"{merge_source_alias}.{col}") for col in when_not_matched}
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_expr)

        merge_builder.execute()

    @overload
    def write_table(self, table_id: str) -> None: ...
    
    @overload
    def write_table(self, table_id: str, df: DataFrame) -> None: ...
    
    
    def write_table(self, table_id: str, df: Optional[DataFrame] = None) -> None:
        """Writes the DataFrame to the target table based on the table_id."""
        
        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")

        table_name = target_config.get(YC.TABLE_KEY)
        if not self.spark.catalog.tableExists(table_name):
            self.logger.info(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_id}")
            if df is None:
                schema = self._get_table_schema(table_id)
                df = self.spark.createDataFrame([], schema)
            self._create_table(table_id, df)
        else:
            write_type = target_config.get(YC.WRITE_TYPE_KEY, YC.WRITE_TYPE_TABLE)
            write_mode = target_config.get(YC.WRITE_MODE_KEY, YC.WRITE_MODE_APPEND)
            options = target_config.get(YC.OPTIONS_KEY, {})
        
            if write_type == YC.WRITE_TYPE_TABLE:
                self.logger.debug(f"Writing DataFrame to target table with table_id: {table_id}")
                if write_mode == YC.WRITE_MODE_MERGE:
                    self.merge_into_target(table_id)
                else:
                    if df is None:
                        source_type = target_config.get(YC.SOURCE_TYPE_KEY)
                        source_id = target_config.get(YC.SOURCE_ID_KEY)
                        df = self._get_source_df(source_id, source_type)
                    writer = df.write.mode(write_mode).options(**options)
                    writer.saveAsTable(table_name)
                self._log_latest_operation_metrics(table_name)
            elif write_type == YC.WRITE_TYPE_STREAM:
                self.logger.info(f"Writing DataFrame to target stream with table_id: {table_id}")
                checkpoint = self._get_checkpoint_location(table_id)
                if not checkpoint:
                    raise ValueError(f"Checkpoint location is missing for table_id: {table_id}")
                # Get or create DataFrame
                if df is None:
                    source_type = target_config.get(YC.SOURCE_TYPE_KEY)
                    source_id = target_config.get(YC.SOURCE_ID_KEY)
                    df = self._get_source_df(source_id, source_type)
                    
                query_prefix = self.global_config.get(YC.QUERY_NAME_PREFIX_KEY, "default_query")
                query_name = f"{query_prefix}_{table_id}"
                
                # Check for for_each_batch_function
                for_each_batch_function_name = target_config.get(YC.FOR_EACH_BATCH_FUNCTION_KEY)
                # Start streaming query
                try:
                    options = target_config.get("options", {})
                    options.update({"checkpointLocation": checkpoint})
                    # Extract the trigger option
                    trigger_option = options.get("trigger", "once")  # Default to "once" if not specified

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

                    # Apply the trigger if specified
                    if trigger_option:
                        if trigger_option.startswith("processingTime="):
                            interval = trigger_option.split("=")[1].strip()
                            query = query.trigger(processingTime=interval)
                        elif trigger_option == "once":
                            query = query.trigger(once=True)
                        elif trigger_option.startswith("continuous="):
                            interval = trigger_option.split("=")[1].strip()
                            query = query.trigger(continuous=interval)
        
                    self.logger.info(f"Starting streaming query for table: {table_name} with query name: {query_name}")
                    self.logger.debug(f"Streaming query options: {options}")
                
                    streaming_query =query.toTable(table_name)            
                    # Continuously monitor streaming progress with a delay
                    while streaming_query.isActive:
                        last_progress = streaming_query.lastProgress
                        if last_progress and "numOutputRows" in last_progress:
                            num_records = last_progress["numOutputRows"]
                            self.logger.info(f"Records written in last batch: {num_records}")
                        else:
                            self.logger.info("No new records processed yet.")
                        time.sleep(5)  # Sleep for 5 seconds before checking again
                        
                    streaming_query.awaitTermination()
                except Exception as e:
                    self.logger.error(f"Error while running streaming query for table {table_name}: {str(e)}")
                    raise
                finally:
                    if streaming_query and streaming_query.isActive:
                        streaming_query.stop()
                        self.logger.info(f"Streaming query stopped for table: {table_name}")
           
            else:
                raise ValueError(f"Unsupported write type: {write_type}")

        
