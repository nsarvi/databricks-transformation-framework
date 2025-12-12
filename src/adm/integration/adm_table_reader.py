from adm.integration.adm_base_data_transformer import AdmBaseDataTransformer
from pyspark.sql import DataFrame
from adm.integration import yaml_constants as YC


class AdmTableReader(AdmBaseDataTransformer):
    
    def __init__(self, config_file: str):
        super().__init__(config_file)
        
    def read_source_table(self, source_id: str) -> DataFrame:
        self.logger.debug("Reading source with ID: %s", source_id)
        
        # Lookup the source table from the given source_id
        source_config = self.source_lookup.get(source_id)
        df = self._read_adm_table(source_id)
        # Apply transformations
        transformation_id = source_config.get(YC.TRANSFORMATION_ID_KEY) if source_config else None
        if transformation_id:
            self.logger.debug("Applying transformations with transformation_id: %s", transformation_id)
            df = self.apply_transformations(transformation_id,df)
        return df


