from typing import Dict, List, Any, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from adm.integration  import yaml_constants as YC
from adm.integration.utils.logging_utils import LoggingHandler
from adm.integration.utils.config_utils import ConfigUtils



class BaseIntegration:

    def __init__(self, config_file: str ):
        self.spark = self._get_spark()
        self.config = ConfigUtils.load_config(config_file)
        self.source_lookup = self._build_source_lookup()
        self.transformation_lookup= self._build_transformation_lookup()
        self.combine_lookup = self._build_combine_lookup()
        self.target_lookup = self._build_target_lookup()
        self.logger=LoggingHandler(__name__).get_logger()
        
    @staticmethod
    def _get_spark() -> SparkSession:
        try:
            return DatabricksSession.builder.getOrCreate()
        except ImportError as ir:
            logger.error(f"Error while importing Databricks Connect library {ir.__cause__}")
            return SparkSession.builder.getOrCreate()
        except Exception as e:
            logger.error(f"Error while getting SparkSession via Databricks Connect {e.__cause__}")
            return SparkSession.builder.getOrCreate()

        
    def _build_source_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary."""
        return {source[YC.SOURCE_ID_KEY]: source for source in self.config.get(YC.SOURCES_KEY, [])}
    
    
    def _build_transformation_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for transformations."""
        return {
            transformation[YC.ID_KEY]: transformation for transformation in self.config.get(YC.TRANSFORMATIONS_KEY, [])
        }
        
    def _build_combine_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for combine operations."""
        return {
            combine[YC.COMBINE_ID_KEY]: combine for combine in self.config.get(YC.COMBINE_KEY, [])
        }

    def _build_target_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for targets."""
        return {
            target[YC.TABLE_ID_KEY]: target for target in self.config.get(YC.TARGETS_KEY, [])
        }
        
    def _read_adm_table(self, source_id: str) -> DataFrame:
        """Reads the source table based on the table_id."""
        
        # Lookup the source table from the given source_id
        source_config = self.source_lookup.get(source_id)
        if not source_config:
            raise ValueError(f"Configuration not found for source_id: {source_id}")
        table_name = source_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(f"Table ID not found in configuration for source_id: {source_id}")
        self.logger.debug("Reading the table: %s", table_name)
        
        table_name = source_config[YC.TABLE_KEY]
        read_type = source_config.get(YC.READ_TYPE_KEY, "table")
        
        if read_type == YC.TABLE_KEY:
            df = self.spark.table(table_name)
        elif read_type == YC.TABLE_STREAM_KEY:
            stream_options = source_config.get(YC.STREAM_OPTIONS_KEY, {})
            df = self.spark.readStream.format("delta").options(**stream_options).table(table_name)
        elif read_type == YC.TABLE_CDF_KEY:
            cdf_options = source_config.get(YC.CDF_OPTIONS_KEY, {})
            df = self.spark.read.format("delta").option("readChangeData", "true").options(**cdf_options).table(table_name)
        else:
            raise ValueError(f"Unsupported read type: {read_type}")
        
        return df
    
    @staticmethod
    def add_additional_columns(extra_columns: List[Dict[str, Any]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that adds extra columns to a DataFrame.
        Additional colummn section is used to add new columns that are not derived from existing columns
        
        :param extra_columns: List of column definitions with name, type, and value
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            for col_def in extra_columns:
                df = df.withColumn(col_def[YC.COLUMN_NAME_KEY], F.expr(col_def[YC.COLUMN_VALUE_KEY]).cast(col_def[YC.COLUMN_TYPE_KEY]))
            return df

        return transform

    
    @staticmethod
    def apply_column_expressions(column_expressions: List[Dict[str, str]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column expressions to a DataFrame.
        Column expressions section is used to apply expressions to existing columns and create new derived columns.
        
        :param column_expressions: List of column expressions (dictionaries with column and expression)
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            include_all_columns = any(col_expr[YC.COLUMN_KEY] == "*" for col_expr in column_expressions)

            if include_all_columns:
                # Include all existing columns by default
                existing_columns = df.columns
                for col in existing_columns:
                    df = df.withColumn(col, df[col])  # Retain all existing columns

            for col_expr in column_expressions:
                column_name = col_expr[YC.COLUMN_KEY]
                expression = col_expr[YC.EXPRESSION_KEY]

                if column_name != "*":
                    # Use withColumn to replace or create the column
                    df = df.withColumn(column_name, F.expr(expression))
            return df

        return transform
    
    @staticmethod
    def apply_filters(filters: List[Dict[str, str]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies filter conditions to a DataFrame.

        :param filters: List of filter conditions (dictionaries with condition expressions)
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            for filter_def in filters:
                df = df.filter(F.expr(filter_def[YC.CONDITION_KEY]))
            return df

        return transform


    @staticmethod
    def apply_distinct_columns(distinct_columns: List[Dict[str, str]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies distinct operation on specified columns to a DataFrame.

        :param distinct_columns: List of columns to apply distinct operation
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            if distinct_columns[0][YC.COLUMN_KEY] == "*":
                return df.distinct()
            else:
                columns = [col[YC.COLUMN_KEY] for col in distinct_columns]
                return df.select(*columns).distinct()

        return transform
    
    
    @staticmethod
    def apply_drop_duplicates(drop_duplicates: List[Dict[str, str]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies drop duplicates operation on specified columns to a DataFrame.
        This method handles the case where drop_duplicates exists but does not contain a list of columns.
        
        :param drop_duplicates: List of columns to apply drop duplicates operation
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            if isinstance(drop_duplicates, list):
                columns = [col[YC.COLUMN_KEY] for col in drop_duplicates]
                if columns:
                    return df.dropDuplicates(columns)
                else:
                    return df.dropDuplicates()
            else:
                return df.dropDuplicates()

        return transform
    
    
    @staticmethod
    def apply_column_concatenations(column_concatenations: List[Dict[str, Any]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column concatenation expressions to a DataFrame.

        :param column_concatenations: List of column concatenation expressions (dictionaries with target_column, columns, and delimiter)
        :return: A callable function that transforms a DataFrame
        """
        
        def transform(df: DataFrame) -> DataFrame:
            for concat_expr in column_concatenations:
                target_column = concat_expr[YC.TARGET_COLUMN_KEY]
                columns = concat_expr[YC.COLUMNS_KEY]
                delimiter = concat_expr[YC.DELIMITER_KEY]
                
                # Apply expressions and concatenation
                expressions = [F.expr(col_expr[YC.EXPRESSION_KEY]) for col_expr in columns]
                concatenated_expr = F.concat_ws(delimiter, *expressions)
                
                df = df.withColumn(target_column, concatenated_expr)
            return df
        return transform
    


    @staticmethod
    def apply_column_renames(column_renames: List[Dict[str, str]]) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """
        def transform(df: DataFrame) -> DataFrame:
            for rename_def in column_renames:
                df = df.withColumnRenamed(rename_def[YC.COLUMN_KEY], rename_def[YC.TARGET_COLUMN_KEY])
            return df

        return transform