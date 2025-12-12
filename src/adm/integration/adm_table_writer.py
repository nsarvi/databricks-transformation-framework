from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from adm.integration import yaml_constants as YC
from adm.integration.utils.config_utils import ConfigUtils
from adm.integration.adm_data_transformer import AdmDataTransformer
from delta.tables import DeltaTable
from typing import Optional, Dict, Any, overload

class AdmTableWriter(AdmDataTransformer):
    
    def __init__(self, config_file: str):
        super().__init__(config_file)

    def _create_table(self, table_id: str):
          # Lookup the table from the given table_id and create a table with all the properties for options and partitions
        target_config = self.target_lookup.get(table_id)
        if not target_config:
            raise ValueError(f"Configuration not found for table_id: {table_id}")

        table_name = target_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(f"Table name not found in configuration for table_id: {table_id}")
        table_exists = self.spark.catalog.tableExists(table_name)
        if not table_exists:
            schema_file = target_config.get(YC.SCHEMA_FILE_KEY)
            if not schema_file:
                raise ValueError(f"Table {table_name} does not exist and no schema file provided. Please provide the schema file.")
            self.logger.info(f"Table {table_name} does not exist. Proceeding to create for table_id: {table_id}")
            self.logger.debug(f"Table configuration: {target_config}")
            
            if not table_exists and schema_file:
                self.logger.info(f"Creating table {table_name} as it does not exist.")
                schema = ConfigUtils.get_schema(schema_file)
                df = self.spark.createDataFrame([], schema)
                writer = df.write.mode(YC.WRITE_MODE_APPEND).options(**target_config.get(YC.OPTIONS_KEY, {}))
                partition_by = target_config.get(YC.PARTITION_BY_KEY, [])
                cluster_by = target_config.get(YC.CLUSTER_BY_KEY, [])
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                if cluster_by:
                    writer = writer.clusterBy(*cluster_by)
                
                writer.saveAsTable(table_name)
                self.logger.info(f"Table {table_name} created successfully.")
            
            self.table_name = table_name
            self.write_type = target_config.get(YC.WRITE_TYPE_KEY, YC.WRITE_TYPE_TABLE)
            self.write_mode = target_config.get(YC.WRITE_MODE_KEY, YC.WRITE_MODE_APPEND)
            self.options = target_config.get(YC.OPTIONS_KEY, {})

     
    def get_merge_source_df(self, merge_source_id: str, merge_source_type: str) -> DataFrame:
        """Gets the source DataFrame for the merge operation based on the merge_source_id and merge_source_type configuration."""
        if merge_source_type == YC.COMBINE_KEY:
            return self.apply_combine(merge_source_id)
        elif merge_source_type == YC.SOURCES_KEY:
            return self.read_source_table(merge_source_id)
        else:
            raise ValueError(f"Unsupported merge or source type: {merge_source_type}. Supported types are: {YC.COMBINE_KEY}, {YC.SOURCES_KEY}")

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
        merge_source_type = target_config.get(YC.MERGE_SOURCE_TYPE_KEY, "source")
        merge_source_id = target_config.get(YC.MERGE_SOURCE_ID_KEY)
        merge_condition = target_config.get(YC.MERGE_CONDITION_KEY)
        merge_actions = target_config.get(YC.MERGE_ACTIONS_KEY, {})
        merge_source_alias = target_config.get(YC.MERGE_SOURCE_ALIAS_KEY, "source")
        target_alias = target_config.get(YC.TARGET_ALIAS_KEY, "target")

        # Get the source DataFrame for the merge operation
        source_df = self.get_merge_source_df(merge_source_id, merge_source_type)

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
        self._create_table(table_id)
        table_name = target_config.get(YC.TABLE_KEY)
        write_type = target_config.get(YC.WRITE_TYPE_KEY, YC.WRITE_TYPE_TABLE)
        write_mode = target_config.get(YC.WRITE_MODE_KEY, YC.WRITE_MODE_APPEND)
        options = target_config.get(YC.OPTIONS_KEY, {})
   
        if write_type == YC.WRITE_TYPE_TABLE:
            if df is None:
                raise ValueError("DataFrame cannot be None for WRITE_TYPE_TABLE")
            self.logger.debug(f"Writing DataFrame to target table with table_id: {table_id}")
            writer = df.write.mode(write_mode).options(**options)
            writer.saveAsTable(table_name)
            # Fetch latest history entry
            metrics = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").select(
                F.get_json_object(F.to_json(F.col("operationMetrics")), "$.numOutputRows").alias("num_records")
            ).collect()[0]

            num_records = int(metrics["num_records"]) if metrics["num_records"] else 0
        elif write_type == YC.WRITE_TYPE_STREAM:
            if df is None:
                raise ValueError("DataFrame cannot be None for WRITE_TYPE_STREAM")
            query = df.writeStream.outputMode(write_mode).options(**options)

            self.logger.info(f"Starting streaming query for table: {table_name}")
            self.logger.debug(f"Streaming query options: {options}")
            streaming_query = query.start(table_name)
            self.logger.info(f"Streaming query started for table: {table_name}")
            streaming_query.awaitTermination()

            # Continuously monitor streaming progress with a delay
            while streaming_query.isActive:
                last_progress = streaming_query.lastProgress
                if last_progress and "numOutputRows" in last_progress:
                    num_records = last_progress["numOutputRows"]
                    self.logger.info(f"Records written in last batch: {num_records}")
                else:
                    self.logger.info("No new records processed yet.")
        elif write_type == YC.WRITE_TYPE_MERGE:
            self.merge_into_target(table_id)
            # Fetch latest history entry
            metrics = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").select(
                F.get_json_object(F.to_json(F.col("operationMetrics")), "$.numOutputRows").alias("num_records")
            ).collect()[0]
            num_records = int(metrics["num_records"]) if metrics["num_records"] else 0
        else:
            raise ValueError(f"Unsupported write type: {write_type}")

        self.logger.info(f"Number of records written to table {table_name}: {num_records}")
