from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType
from typing import List
from pyspark.sql.functions import trim, col

def remove_leading_trailing_spaces(df:DataFrame) -> DataFrame:
    """
    Removes leading and trailing spaces from all string columns in the DataFrame.
    Parameters:  df : DataFrame     
    Returns: DataFrame
        The DataFrame with leading and trailing spaces removed from all string columns.
    """
    # Loop through all columns and apply `trim` to string column
    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

    for column_name in string_columns:
        df = df.withColumn(column_name, trim(col(column_name)))
    return df

def schema_conversion(source_df:DataFrame, target_schema:StructType) -> DataFrame:
        """ schema conversion that: Validates column names exist in source.
                                    Enforces data types.
                                    Enforces nullable constraints.
                                    Maintains column order
        Args: source_df (DataFrame): Source DataFrame to convert.
            target_schema (StructType): Target schema with nullable constraints
        Returns: DataFrame: DataFrame with enforced schema
        Raises: ValueError: If schema cannot be enforced
        """
        
        # Validation Phase
        missing_cols = [f.name for f in target_schema if f.name not in source_df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in source data frame: {', '.join(missing_cols)}")     
        result_df = source_df
        # Cast to correct data types
        for field in target_schema:
            result_df = result_df.withColumn(field.name, col(field.name).cast(field.dataType))

        # Select only target columns in correct order
        result_df = result_df.select([col(f.name) for f in target_schema])
        # Validate NULL constraints
        null_violations = []
        for field in target_schema:
            if not field.nullable:
                null_count = result_df.filter(col(field.name).isNull()).count()
                if null_count > 0:
                    null_violations.append(f"{null_count} null values found in NOT NULL field '{field.name}'")
        if null_violations:
            raise ValueError(f"Nullable constraint violations in source data:\n  - " + 
                        "\n  - ".join(null_violations))
        return result_df