import importlib
from typing import Any, Callable, Dict, List, Optional, cast
import re

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F

from wilsonelser.transformation import yaml_constants as YC
from wilsonelser.transformation.utils.config_utils import ConfigUtils
from wilsonelser.transformation.utils.logging_utils import LoggingHandler


class BaseIntegration:
    def __init__(self, config_file: str, env_config_path: Optional[str] = None):
        self.spark = self._get_spark()
        self.env_vars = ConfigUtils.load_env_variables(env_config_path)
        self.config = ConfigUtils.load_config(config_file, env_config_path)
        self.global_config = self._load_global_config()
        self.source_lookup = self._build_source_lookup()
        self.transformation_lookup = self._build_transformation_lookup()
        self.combine_lookup = self._build_combine_lookup()
        self.target_lookup = self._build_target_lookup()
        self.logger = LoggingHandler(__name__).get_logger()

    @staticmethod
    def _get_spark() -> SparkSession:
        logger = LoggingHandler(__name__).get_logger()
        try:
            return DatabricksSession.builder.getOrCreate()
        except ImportError as ir:
            logger.error(
                f"Error while importing Databricks Connect library {ir.__cause__}"
            )
            return SparkSession.builder.getOrCreate()
        except Exception as e:
            logger.error(
                f"Error while getting SparkSession via Databricks Connect {e.__cause__}"
            )
            return SparkSession.builder.getOrCreate()

    def _load_global_config(self) -> dict:
        """Loads the global configuration from the config file."""
        return self.config.get(YC.GLOBAL_CONFIG_KEY, {})


    def _build_source_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary."""
        return {
            source[YC.SOURCE_ID_KEY]: source
            for source in self.config.get(YC.SOURCES_KEY, [])
        }

    def _build_transformation_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for transformations."""
        return {
            transformation[YC.ID_KEY]: transformation
            for transformation in self.config.get(YC.TRANSFORMATIONS_KEY, [])
        }


    def _build_combine_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for combine operations."""
        return {
            combine[YC.COMBINE_ID_KEY]: combine
            for combine in self.config.get(YC.COMBINE_KEY, [])
        }

    def _build_target_lookup(self) -> dict:
        """Preprocess configs to lookup dictionary for targets."""
        return {
            target[YC.TABLE_ID_KEY]: target
            for target in self.config.get(YC.TARGETS_KEY, [])
        }

    def _read_table(self, source_id: str) -> DataFrame:
        """Reads the source table based on the table_id."""

        # Lookup the source table from the given source_id
        source_config = self.source_lookup.get(source_id)
        if not source_config:
            raise ValueError(f"Configuration not found for source_id: {source_id}")
        table_name = source_config.get(YC.TABLE_KEY)
        if not table_name:
            raise ValueError(
                f"Table ID not found in configuration for source_id: {source_id}"
            )
        self.logger.debug("Reading the table: %s", table_name)

        table_name = source_config[YC.TABLE_KEY]
        read_type = source_config.get(YC.READ_TYPE_KEY, "table")

        if read_type == YC.TABLE_KEY:
            df = self.spark.table(table_name)
        elif read_type == YC.TABLE_STREAM_KEY:
            stream_options = source_config.get(YC.STREAM_OPTIONS_KEY, {})
            df = (
                self.spark.readStream.format("delta")
                .options(**stream_options)
                .table(table_name)
            )
        elif read_type == YC.TABLE_CDF_KEY:
            cdf_options = source_config.get(YC.CDF_OPTIONS_KEY, {})
            df = (
                self.spark.read.format("delta")
                .option("readChangeData", "true")
                .options(**cdf_options)
                .table(table_name)
            )
        else:
            raise ValueError(f"Unsupported read type: {read_type}")

        return df
    
    def _get_checkpoint_location(self, table_id: str) -> str:
        """Constructs the checkpoint location dynamically using global_config."""
        base_checkpoint_location = self.global_config.get(YC.BASE_CHECKPOINT_LOCATION_KEY)
        if not base_checkpoint_location:
            self.logger.error("Base checkpoint location is not provided in the global configuration.")
            raise ValueError("Base checkpoint location is missing in the global configuration.")
        return f"{base_checkpoint_location}/{table_id}"

    @staticmethod
    def add_additional_columns(
        extra_columns: List[Dict[str, Any]],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that adds extra columns to a DataFrame.
        Additional colummn section is used to add new columns that are not derived from existing columns

        :param extra_columns: List of column definitions with name, type, and value
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            for col_def in extra_columns:
                df = df.withColumn(
                    col_def[YC.COLUMN_NAME_KEY],
                    F.expr(col_def[YC.COLUMN_VALUE_KEY]).cast(
                        col_def[YC.COLUMN_TYPE_KEY]
                    ),
                )
            return df

        return transform

    @staticmethod
    def apply_column_expressions(
        column_expressions: List[Dict[str, str]],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column expressions to a DataFrame.
        Column expressions section is used to apply expressions to existing columns and create new derived columns.

        :param column_expressions: List of column expressions (dictionaries with column and expression)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            expressions = []
            include_all_columns = any(
                col_expr[YC.COLUMN_KEY] == "*" for col_expr in column_expressions
            )

            if include_all_columns:
                in_place_columns = [col_expr[YC.COLUMN_KEY] for col_expr in column_expressions if col_expr[YC.COLUMN_KEY] in df.columns] #ol_expr.get("replace", False)]
                if len(in_place_columns) > 0:
                    expressions.append("* except (" + ", ".join(in_place_columns) + ")")
                else:
                    expressions.append("*")

            for col_expr in column_expressions:
                if col_expr[YC.COLUMN_KEY] != "*":
                    expressions.append(
                        f"{col_expr[YC.EXPRESSION_KEY]} as {col_expr[YC.COLUMN_KEY]}"
                    )

            df = df.selectExpr(*expressions)
            return df

        return transform

    @staticmethod
    def apply_filters(
        filters: List[Dict[str, str]],
    ) -> Callable[[DataFrame], DataFrame]:
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
    def apply_distinct_columns(
        distinct_columns: List[Dict[str, str]],
    ) -> Callable[[DataFrame], DataFrame]:
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
    def apply_drop_duplicates(
        drop_duplicates: List[Dict[str, str]],
    ) -> Callable[[DataFrame], DataFrame]:
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
    def apply_column_concatenations(
        column_concatenations: List[Dict[str, Any]],
    ) -> Callable[[DataFrame], DataFrame]:
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
                expressions = [
                    F.expr(col_expr[YC.EXPRESSION_KEY]) for col_expr in columns
                ]
                concatenated_expr = F.concat_ws(delimiter, *expressions)

                df = df.withColumn(target_column, concatenated_expr)
            return df

        return transform

    @staticmethod
    def apply_column_renames(
        column_renames: List[Dict[str, str]],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            for rename_def in column_renames:
                df = df.withColumnRenamed(
                    rename_def[YC.COLUMN_KEY], rename_def[YC.TARGET_COLUMN_KEY]
                )
            return df

        return transform
    

    @staticmethod
    def apply_columns_mapping(
        columns_mapping: Dict[str, str],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            df = df.withColumnsRenamed(
                columns_mapping
            )
            return df

        return transform


    @staticmethod
    def apply_columns_expressions(
        columns_expressions: Dict[str, str],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            expr_map = {k: F.expr(v) for k, v in columns_expressions.items()}
            df = df.withColumns(
                expr_map
            )
            return df

        return transform
    

    @staticmethod
    def apply_columns_to_snake_case(
        columns_to_snake_case: bool,
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            if columns_to_snake_case:
                pattern = re.compile(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+')
                snake_cols = ["_".join(map(str.lower, (re.findall(pattern, col)))) for col in df.columns]
                df= df.toDF(*snake_cols)
            return df
        return transform
    

    @staticmethod
    def apply_columns_to_select(
        cols: List[str],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies column renaming to a DataFrame.

        :param column_renames: List of column renaming mappings (dictionaries with old_name and new_name)
        :return: A callable function that transforms a DataFrame
        """

        def transform(df: DataFrame) -> DataFrame:
            df = df.select(*cols)
            return df

        return transform
    

    @staticmethod
    def apply_custom_function(
        custom_function: Dict[str, Any],
    ) -> Callable[[DataFrame], DataFrame]:
        """
        Returns a callable function that applies a custom function to specific columns in a DataFrame.

        :param custom_function: Dictionary containing the function name and column mappings
        :return: A callable function that transforms a DataFrame
        """
        # Extract the function name and column mappings
        function_name = custom_function.get(YC.CUSTOM_FUNCTION_NAME_KEY)
        column_mappings = custom_function.get(YC.COLUMN_MAPPINGS_KEY, [])
        kwargs = custom_function.get(YC.KWARGS_KEY, {})
        
        logger = LoggingHandler(__name__).get_logger()
        logger.info(f"Applying function Name: {function_name}")
        # Resolve the custom function dynamically
        if isinstance(function_name, str):
            module_name, func_name = function_name.rsplit(".", 1)
            logger.debug(f"Importing module: {module_name}, function: {func_name}")
            module = importlib.import_module(module_name)
            func: Callable[..., Column] = getattr(module, func_name)
        elif callable(function_name):
            func = cast(Callable[..., Column], function_name)
        else:
            raise ValueError(f"Invalid custom_function name: {function_name}")

        def transform(df: DataFrame) -> DataFrame:
            if len(column_mappings) > 0:
                for mapping in column_mappings:
                    input_columns = mapping.get(YC.INPUT_COLUMNS_KEY, [])
                    output_column = mapping.get(YC.OUTPUT_COLUMN_KEY)

                    if not input_columns or not output_column:
                        raise ValueError(
                            f"Invalid column mapping: input_columns={input_columns}, output_column={output_column}"
                        )

                    # Apply the custom function to the specified input columns and write to the output column
                    if len(input_columns) == 1:
                        # Single input column
                        df = df.withColumn(output_column, func(F.col(input_columns[0])))
                    else:
                        # Multiple input columns
                        df = df.withColumn(output_column, func(*[F.col(col) for col in input_columns]))
            elif len(kwargs) > 0:
                df = df.transform(func, **kwargs)
            else:
                df = df.transform(func)
            return df

        return transform