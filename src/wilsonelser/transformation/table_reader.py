from wilsonelser.transformation.base_data_transformer import BaseDataTransformer
from pyspark.sql import DataFrame
from typing import Optional
from wilsonelser.transformation import yaml_constants as YC


class TableReader(BaseDataTransformer):
    
    def __init__(self, config_file: str, env_config_path: Optional[str] = None):
        super().__init__(config_file, env_config_path)
        
    def read_source_table(self, source_id: str) -> DataFrame:
        self.logger.debug("Reading source with ID: %s", source_id)
        
        # Lookup the source table from the given source_id
        source_config = self.source_lookup.get(source_id)
        if not source_config:
            self.logger.error("Source ID '%s' not found in source lookup.", source_id)
        
        # Check if the source is a subquery
        subquery = source_config.get(YC.SUBQUERY_KEY) if source_config else None
        if subquery:
            self.logger.debug("Executing subquery for source ID: %s", source_id)
            df = self.spark.sql(subquery)  # Execute the subquery
            df.createOrReplaceTempView(source_id)  # Create a temporary view for the subquery
        else:
            self.logger.debug("Reading table for source ID: %s", source_id)
            df = self._read_table(source_id)
        # Apply transformations
        transformation_id = source_config.get(YC.TRANSFORMATION_ID_KEY) if source_config else None
        if transformation_id:
            self.logger.debug("Applying transformations with transformation_id: %s", transformation_id)
            df = self.apply_transformations(transformation_id,df)
        return df


