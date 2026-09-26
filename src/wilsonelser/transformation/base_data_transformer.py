from wilsonelser.transformation  import yaml_constants as YC
from wilsonelser.transformation.base_integration import BaseIntegration
from pyspark.sql import DataFrame
from typing import Optional


class BaseDataTransformer(BaseIntegration):
    
    
    def apply_transformations(self, transformation_id:str, df: Optional[DataFrame] = None) -> DataFrame:
        """Apply all the transformations to the dataframe as defined in the config."""
        
        self.logger.info(f"Starting transformations for ID: {transformation_id}")
      
        
        transformation_config = self.transformation_lookup.get(transformation_id)
        if transformation_config is None or "id" not in transformation_config or not transformation_config["id"]:
            self.logger.error(f"Transformation '{transformation_id}' not found in the YAML configuration.")
            raise ValueError(f"Transformation '{transformation_id}' not found in the YAML configuration.")
        
        # Check if both DataFrame and SQL file are provided
        if df is not None and YC.TRANSFORMATION_SQL_FILE_KEY in transformation_config:
            raise ValueError("Both a DataFrame and a SQL file are provided. Please provide only one.")
        
        # If no DataFrame is provided, initialize it using the SQL file
        if df is None:
            if YC.TRANSFORMATION_SQL_FILE_KEY in transformation_config:
                sql_file_path = transformation_config[YC.TRANSFORMATION_SQL_FILE_KEY]
                self.logger.info(f"Initializing DataFrame from SQL file: {sql_file_path}")
                try:
                    with open(sql_file_path, "r") as sql_file:
                        sql_query = sql_file.read()
                    df = self.spark.sql(sql_query)
                except Exception as e:
                    self.logger.error(f"Failed to initialize DataFrame from SQL file: {sql_file_path}. Error: {e}")
                    raise ValueError(f"Error initializing DataFrame from SQL file: {e}")
            else:
                raise ValueError("No DataFrame provided and no SQL file specified in the transformation configuration.")


        for tx_k, tx_v in transformation_config.items():
            # Apply extra columns 
            if tx_k == YC.ADDITIONAL_COLUMNS_KEY:
                column_mapping = tx_v
                df = df.transform(BaseDataTransformer.add_additional_columns(column_mapping))
            
            # Apply column expressions
            if tx_k == YC.COLUMN_EXPRESSIONS_KEY:
                column_expressions = tx_v
                df = df.transform(BaseDataTransformer.apply_column_expressions(column_expressions))    

            # Apply concatenations
            if tx_k == YC.COLUMN_CONCATENATIONS_KEY:
                concatenations = tx_v
                df = df.transform(BaseDataTransformer.apply_column_concatenations(concatenations))
            
            # Apply column renames
            if tx_k == YC.COLUMN_RENAME_KEY:
                column_renames = tx_v
                df = df.transform(BaseDataTransformer.apply_column_renames(column_renames))
            
            # Drop duplicates
            if tx_k == YC.DROP_DUPLICATES_KEY:
                drop_duplicate_columns = tx_v
                df = df.transform(BaseDataTransformer.apply_drop_duplicates(drop_duplicate_columns))
                
            # Apply filters
            if tx_k == YC.FILTERS_KEY:
                conditions = tx_v
                df = df.transform(BaseDataTransformer.apply_filters(conditions))
                
            # Apply distinct columns
            if tx_k == YC.DISTINCT_COLUMNS_KEY:
                distinct_columns =tx_v
                df = df.transform(BaseDataTransformer.apply_distinct_columns(distinct_columns))

            if tx_k == YC.COLUMNS_MAPPING_KEY:
                columns_mapping = tx_v
                df = df.transform(BaseDataTransformer.apply_columns_mapping(columns_mapping))
        
            if tx_k == YC.COLUMNS_EXPRESSIONS_KEY:
                columns_expressions = tx_v
                df = df.transform(BaseDataTransformer.apply_columns_expressions(columns_expressions)) 
            
            if tx_k == YC.COLUMNS_TO_SNAKE_CASE_KEY:
                columns_to_snake_case = tx_v
                df = df.transform(BaseDataTransformer.apply_columns_to_snake_case(columns_to_snake_case))
    
            if tx_k == YC.COLUMNS_TO_SELECT:
                cols = tx_v
                df = df.transform(BaseDataTransformer.apply_columns_to_select(cols))
            
            if tx_k == YC.CUSTOM_FUNCTIONS_KEY:
                custom_functions = tx_v  # List of custom functions
                for custom_function_params in custom_functions:
                    df = df.transform(BaseDataTransformer.apply_custom_function(custom_function_params))
        
        return df

    
