from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer
from adm.integration.adm_table_reader import AdmTableReader
from pyspark.sql import DataFrame
from adm.integration  import yaml_constants as YC
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class AdmDataTransformer(AdmTableReader):
    
    def __init__(self, config_file: str):
        super().__init__(config_file)
  

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
        for df in dataframes[1:]:
            result_df = result_df.unionByName(df)
        if union_config.get(YC.DISTINCT_KEY, False):
            result_df = result_df.distinct()
    
        return result_df
        
        
    def apply_joins(self, combine_id: str) -> DataFrame:
        """
        Applies join operations based on the configuration for the specified combine_id.

        :param combine_id: The combine_id to look up the join configuration
        :return: A DataFrame resulting from the join operations
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
            raise ValueError(f"Join configuration for join_id {join_config[YC.JOIN_ID_KEY]} must have at least two table_ids")

        # Sort source IDs by join order
        source_ids = sorted(source_ids, key=lambda x: x[YC.JOIN_ORDER_KEY])

        # Retrieve join parameters
        
        join_conditions = join_config.get(YC.JOIN_CONDITIONS_KEY, [])
        select_columns = join_config.get(YC.SELECT_COLUMNS_KEY, {})
        post_join_filters = join_config.get(YC.POST_JOIN_FILTERS_KEY, [])
        row_operations = join_config.get(YC.ROW_OPERATIONS_KEY, None)
        alias = join_config.get(YC.ALIAS_KEY, None)

        # Read the DataFrame for the first table
        result_df = self.read_source_table(source_ids[0][YC.SOURCE_ID_KEY])

        #Select specific columns from the first DataFrame
        if source_ids[0][YC.ALIAS_KEY] in select_columns:
            result_df = result_df.select(*select_columns[source_ids[0][YC.ALIAS_KEY]])
        else:
            result_df = result_df.select("*")
        result_df = result_df.alias(source_ids[0]["alias"])
        
        # Iterate through the remaining tables and join them
        for i in range(1, len(source_ids)):
            df = self.read_source_table(source_ids[i][YC.SOURCE_ID_KEY])
                # Apply alias to the current DataFrame
            
            # Select specific columns from the current DataFrame
            if source_ids[i][YC.ALIAS_KEY] in select_columns:
                df = df.select(*select_columns[source_ids[i][YC.ALIAS_KEY]])
            else:
                df = df.select("*")
            df = df.alias(source_ids[i]["alias"])
            # Log the columns in the current DataFrame
            self.logger.info(f"Columns in {source_ids[i]['alias']}: {df.columns}")

            # Find the correct join condition for this pair
            join_condition = None
            for condition in join_conditions:
                if condition[YC.JOIN_CONDITIONS_LEFT_KEY] == source_ids[i-1][YC.ALIAS_KEY] and condition[YC.JOIN_CONDITIONS_RIGHT_KEY] == source_ids[i][YC.ALIAS_KEY]:
                    join_condition = condition[YC.CONDITION_KEY]
                    break
            
            if not join_condition:
                raise ValueError(f"No join condition found for {source_ids[i-1][YC.ALIAS_KEY]} and {source_ids[i][YC.ALIAS_KEY]}")
            
            # Retrieve the join type for this specific table (from source_ids)
            join_type = source_ids[i].get(YC.JOIN_TYPE_KEY, YC.JOIN_TYPE_INNER)  # Default to 'inner' join if not specified

            # Apply the join dynamically based on the join condition
            result_df = result_df.join(
                df,
                on=F.expr(join_condition),
                how=join_type
            )
            
            # Drop duplicate columns if the join condition uses the same column name
            # for condition in join_conditions:
            #     if condition[YC.CONDITION_KEY] == join_condition:
            #         # Split the join condition into individual conditions (e.g., "c.customer_id = s.customer_id" and "c.customer_preferred_store = s.store_id_mapics")
            #         conditions = join_condition.split("AND")
            #         for cond in conditions:
            #             # Extract the left and right columns from the condition
            #             left_column = cond.split("=")[0].strip()
            #             right_column = cond.split("=")[1].strip()

            #             # Check if the column names are the same
            #             if left_column.split(".")[1] == right_column.split(".")[1]:
            #                 # Drop the right column from the result DataFrame
            #                 result_df = result_df.drop(F.col(right_column))
            #                 self.logger.info(f"Dropped duplicate column: {right_column}")
                
            # Log the columns in the resulting DataFrame
            # self.logger.info(f" >>> Columns in result_df after joining with : {result_df.columns}")
        
        # Flatten available columns and log them
    
        final_columns = []
        seen_columns = set()

        # Iterate through the select_columns and add them to the final_columns list
        for alias, columns in select_columns.items():
            for col in columns:
                qualified_col = f"{alias}.{col}"  # Fully qualified column name
                
                # Check if the column has already been added (to handle duplicates)
                if col not in seen_columns:  # Check based on column name (not qualified column name)
                    seen_columns.add(col)  # Add the column name to the set of seen columns
                    final_columns.append(F.col(qualified_col).alias(col))  # Add to final columns list
                else:
                    # If it's already added, skip adding this column
                    self.logger.info(f"Duplicate column detected and skipped: {qualified_col}")

        # Apply the final columns selection (ensure duplicates are dropped by column name)
        result_df = result_df.select(*final_columns)
        self.logger.info(f"Final columns after join: {result_df.columns}")
       
        if post_join_filters:
            result_df = result_df.transform(AdmBaseDataTransformer.apply_filters(post_join_filters))
        
         # Apply row operations if specified
        if row_operations:
            partition_by = row_operations.get(YC.PARTITION_BY_KEY, [])
            order_by = row_operations.get(YC.ORDER_BY_KEY, [])
            rank_column = row_operations.get(YC.RANK_COLUMN_KEY, "row_number")
            row_filter_condition = row_operations.get(YC.FILTER_CONDITION_KEY, "row_number = 1")

            # Map aliased columns to actual column names
            partition_by = [col.split(".")[1] if "." in col else col for col in partition_by]
            order_by = [{"column": order[YC.COLUMN_KEY].split(".")[1] if "." in order[YC.COLUMN_KEY] else order[YC.COLUMN_KEY], "order": order[YC.ORDER_KEY]} for order in order_by]

            self.logger.info(f"Row operations - Partition by: {partition_by}, Order by: {order_by}, Rank column: {rank_column}, Filter condition: {row_filter_condition}")

            # Add row_number column
            window_spec = Window.partitionBy(*[F.col(col) for col in partition_by]).orderBy(
                *[F.col(order["column"]).desc() if order["order"] == "desc" else F.col(order["column"]).asc() for order in order_by]
            )
            result_df = result_df.withColumn(rank_column, F.row_number().over(window_spec))

            # Filter rows based on the row operations condition
            result_df = result_df.filter(F.expr(row_filter_condition))
        
                
        # Assign alias to the resulting DataFrame
        if alias:
            result_df = result_df.alias(alias)
        self.logger.info(f"Completed joins for combine_id: {combine_id}")
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

   