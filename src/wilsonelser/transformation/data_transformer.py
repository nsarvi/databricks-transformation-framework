from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from wilsonelser.transformation.table_reader import TableReader
from pyspark.sql import DataFrame
from wilsonelser.transformation  import yaml_constants as YC
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional

class DataTransformer(TableReader):
    
    def __init__(self, config_file: str, env_config_path: Optional[str] = None):
        super().__init__(config_file, env_config_path)
  
    def _read_and_select(self, source_id: str, alias: str, select_columns: dict) -> DataFrame:
        """
        Reads the source table and selects columns based on the alias and select_columns configuration.

        :param source_id: The source table ID to read from.
        :param alias: The alias for the source table.
        :param select_columns: A dictionary mapping aliases to columns to select.
        :return: A DataFrame with selected columns.
        """
        df = self.read_source_table(source_id)
        
        if alias in select_columns:
            df = df.select(*select_columns[alias])
        else:
            df = df.select("*")
        
        return df.alias(alias)

    def _apply_joins(self, left_df: DataFrame, right_df: DataFrame, join_condition: str, join_type: str) -> DataFrame:
        """
        Applies a join between two DataFrames.

        :param left_df: The left DataFrame.
        :param right_df: The right DataFrame.
        :param join_condition: The join condition as a string expression.
        :param join_type: The type of join (e.g., "inner", "left", "right").
        :return: The joined DataFrame.
        """
        return left_df.join(
            right_df,
            on=F.expr(join_condition),
            how=join_type
        )
    
    def _select_final_columns(self, select_columns: dict, alias_to_df: dict) -> list:
        """
        Selects the final columns for the result DataFrame, handling duplicates.

        :param select_columns: The dictionary of columns to select per alias.
        :param alias_to_df: The dictionary of DataFrames indexed by alias.
        :return: A list of columns to select.
        """
        final_columns = []
        seen_columns = set()

        for alias, columns in select_columns.items():
            for col in columns:
                qualified_col = f"{alias}.{col}"  # Fully qualified column name

                if col not in seen_columns:
                    seen_columns.add(col)
                    final_columns.append(F.col(qualified_col).alias(col))
                else:
                    self.logger.info(f"Duplicate column detected and skipped: {qualified_col}")

        return final_columns


    def _apply_row_operations(self, df: DataFrame, row_operations: dict) -> DataFrame:
        """
        Applies row operations (like ranking and filtering) to the DataFrame.

        :param df: The DataFrame to apply operations on.
        :param row_operations: The dictionary of row operations configuration.
        :return: The DataFrame with row operations applied.
        """
        partition_by = row_operations.get(YC.PARTITION_BY_KEY, [])
        order_by = row_operations.get(YC.ORDER_BY_KEY, [])
        rank_column = row_operations.get(YC.RANK_COLUMN_KEY, "row_number")
        row_filter_condition = row_operations.get(YC.FILTER_CONDITION_KEY, "row_number = 1")

        partition_by = [col.split(".")[1] if "." in col else col for col in partition_by]
        order_by = [{"column": order[YC.COLUMN_KEY].split(".")[1] if "." in order[YC.COLUMN_KEY] else order[YC.COLUMN_KEY], "order": order[YC.ORDER_KEY]} for order in order_by]

        self.logger.info(f"Row operations - Partition by: {partition_by}, Order by: {order_by}, Rank column: {rank_column}, Filter condition: {row_filter_condition}")

        window_spec = Window.partitionBy(*[F.col(col) for col in partition_by]).orderBy(
            *[F.col(order["column"]).desc() if order["order"] == "desc" else F.col(order["column"]).asc() for order in order_by]
        )
        df = df.withColumn(rank_column, F.row_number().over(window_spec))
        return df.filter(F.expr(row_filter_condition))


    def apply_joins(self, combine_id: str) -> DataFrame:
        """
        Applies union operations based on the configuration for the specified combine_id.
    
        :param combine_id: The combine_id to look up the union configuration
        :return: A DataFrame resulting from the union operations
        """
        self.logger.info(f"Applying joins for combine_id: {combine_id}")
        # Retrieve the combine configuration
        combine_config = self.combine_lookup.get(combine_id)
        if not combine_config:
            raise ValueError(f"Combine configuration not found for combine_id: {combine_id}")
         # Retrieve the join configuration
        join_config = combine_config.get(YC.JOINS_KEY)
        if not join_config:
            raise ValueError(f"No join configuration found for combine_id: {combine_id}")
        # Retrieve source IDs and validate
        source_ids = join_config.get(YC.SOURCE_IDS_KEY, [])
        if len(source_ids) < 2:
            raise ValueError(f"Join configuration for combine_id {combine_id} must have at least two source_ids")
        # Sort source IDs by join order
        source_ids = sorted(source_ids, key=lambda x: x[YC.JOIN_ORDER_KEY])
        join_conditions = join_config.get(YC.JOIN_CONDITIONS_KEY, [])
        select_columns = join_config.get(YC.SELECT_COLUMNS_KEY, {})
        post_join_filters = join_config.get(YC.POST_JOIN_FILTERS_KEY, [])
        row_operations = join_config.get(YC.ROW_OPERATIONS_KEY, None)
        alias = join_config.get(YC.ALIAS_KEY, None)

        # Process the first source table
        result_df = self._read_and_select(source_ids[0][YC.SOURCE_ID_KEY], source_ids[0][YC.ALIAS_KEY], select_columns)
        alias_to_df = {source_ids[0][YC.ALIAS_KEY]: result_df}

        join_lookup = {
            (jc[YC.JOIN_CONDITIONS_LEFT_KEY], jc[YC.JOIN_CONDITIONS_RIGHT_KEY]): jc[YC.CONDITION_KEY]
            for jc in join_conditions
        }

        # Process the remaining source tables
        for i in range(1, len(source_ids)):
            right = source_ids[i]
            right_alias = right[YC.ALIAS_KEY]
            self.logger.info(f"Reading and selecting columns for source_id: {right[YC.SOURCE_ID_KEY]} with alias: {right_alias}")
            right_df = self._read_and_select(right[YC.SOURCE_ID_KEY], right_alias, select_columns)

            found_join = False
            for left_alias in alias_to_df.keys():
                if (left_alias, right_alias) in join_lookup:
                    join_condition = join_lookup[(left_alias, right_alias)]
                    join_type = right.get(YC.JOIN_TYPE_KEY, YC.JOIN_TYPE_INNER)
                    self.logger.info(f"Joining {left_alias} with {right_alias} using condition: {join_condition} and join type: {join_type}")

                    result_df = self._apply_joins(result_df, right_df, join_condition, join_type)
                    alias_to_df[right_alias] = result_df
                    # Print the schema after the join
                    self.logger.info(f"Schema after joining {left_alias} with {right_alias}: {result_df.schema}")
                    found_join = True
                    break
            if not found_join:
                raise ValueError(f"No join condition found for {right_alias}")

        self.logger.info("Done applying joins")
        final_columns = self._select_final_columns(select_columns, alias_to_df)
      
        result_df = result_df.select(*final_columns)  # Start from the first alias
        # Print the schema after all joins
        self.logger.info(f"Final columns after join: {result_df.columns}")

        if post_join_filters:
            result_df = result_df.transform(BaseDataTransformer.apply_filters(post_join_filters))

        if row_operations:
            result_df = self._apply_row_operations(result_df, row_operations)

        if alias:
            result_df = result_df.alias(alias)

        self.logger.info(f"Completed joins for combine_id: {combine_id}")
        return result_df


    def apply_unions(self, combine_id: str) -> DataFrame:
        """
        Applies union operations based on the configuration for the specified combine_id.
    
        :param combine_id: The combine_id to look up the union configuration
        :return: A DataFrame resulting from the union operations
        """
        combine_config = self.combine_lookup.get(combine_id)
        self.logger.info(f"Applying unions for combine_id: {combine_id}")
        if not combine_config:
            raise ValueError(f"Combine configuration not found for combine_id: {combine_id}")
    
        union_config = combine_config.get(YC.UNIONS_KEY)
        if not union_config:
            raise ValueError(f"No union configuration found for combine_id: {combine_id}")
    
        source_ids = union_config.get(YC.SOURCE_IDS_KEY, [])
        if not source_ids:
            raise ValueError(f"No table_ids found in union configuration for combine_id: {combine_id}")
    
        dataframes = [self.read_source_table(source_id) for source_id in source_ids]
    
        if not dataframes:
            raise ValueError(f"No DataFrames to union for combine_id: {combine_id}")
    
        # Todo : revisit this logic to handle using reduce function
        result_df = dataframes[0]
        allow_missing = union_config.get(YC.UNION_ALLOW_MISSING, False)
        for df in dataframes[1:]:
            result_df = result_df.unionByName(df, allowMissingColumns=allow_missing)
        if union_config.get(YC.DISTINCT_KEY, False):
            result_df = result_df.distinct()
    
        return result_df
        
    
    def apply_combine(self, combine_id: str) -> DataFrame:
        """
        Applies combine operations (unions and joins) based on the configuration for the specified combine_id.

        :param combine_id: The combine_id to look up the combine configuration
        :return: A DataFrame resulting from the combine operations
        """
        combine_config = self.combine_lookup.get(combine_id)
        if not combine_config:
            raise ValueError(f"Combine configuration not found for combine_id: {combine_id}")

        # Apply union operations only if union configuration exists
        union_config = combine_config.get(YC.UNIONS_KEY)
        if union_config:
            union_df = self.apply_unions(combine_id)
            final_df=union_df

        # Apply join operations only if join configuration exists
        join_config = combine_config.get(YC.JOINS_KEY)
        if join_config:
            join_df = self.apply_joins(combine_id)
            final_df = join_df

        
        return final_df

   