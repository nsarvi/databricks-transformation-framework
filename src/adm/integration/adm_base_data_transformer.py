from adm.integration  import yaml_constants as YC
from adm.integration.base_integration import BaseIntegration
from pyspark.sql import DataFrame
from typing import Optional


class AdmBaseDataTransformer(BaseIntegration):
    
    
    def apply_transformations(self, transformation_id:str, df: Optional[DataFrame] = None) -> DataFrame:
        """Apply all the transformations to the dataframe as defined in the config."""
        
        self.logger.info(f"Starting transformations for ID: {transformation_id}")
      
        
        transformation_config = self.transformation_lookup.get(transformation_id)
        if transformation_config is None or "id" not in transformation_config or not transformation_config["id"]:
            logger.error(f"Transformation '{transformation_id}' not found in the YAML configuration.")
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


        
        # Apply extra columns 
        if YC.ADDITIONAL_COLUMNS_KEY in transformation_config:
            column_mapping = transformation_config[YC.ADDITIONAL_COLUMNS_KEY]
            df = df.transform(AdmBaseDataTransformer.add_additional_columns(column_mapping))
        
        # Apply column expressions
        if YC.COLUMN_EXPRESSIONS_KEY in transformation_config:
            column_expressions = transformation_config[YC.COLUMN_EXPRESSIONS_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_column_expressions(column_expressions))    

        # Apply concatenations
        if YC.COLUMN_CONCATENATIONS_KEY in transformation_config:
            concatenations = transformation_config[YC.COLUMN_CONCATENATIONS_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_column_concatenations(concatenations))
        
        # Apply column renames
        if YC.COLUMN_RENAME_KEY in transformation_config:
            column_renames = transformation_config[YC.COLUMN_RENAME_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_column_renames(column_renames))
        
        # Drop duplicates
        if YC.DROP_DUPLICATES_KEY in transformation_config:
            drop_duplicate_columns = transformation_config[YC.DROP_DUPLICATES_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_drop_duplicates(drop_duplicate_columns))
            
        # Apply filters
        if YC.FILTERS_KEY in transformation_config:
            conditions=transformation_config[YC.FILTERS_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_filters(conditions))
            
        # Apply distinct columns
        if YC.DISTINCT_COLUMNS_KEY in transformation_config:
            distinct_columns = transformation_config[YC.DISTINCT_COLUMNS_KEY]
            df = df.transform(AdmBaseDataTransformer.apply_distinct_columns(distinct_columns))
        
    
            
        return df

    
