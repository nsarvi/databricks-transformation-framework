from pyspark.sql import Column
from pyspark.sql.functions import col
from pyspark.sql.functions import when, lit
from pyspark.sql.functions import trim, col
from pyspark.sql.types import StringType

def calculate_total_price(price: Column, quantity: Column) -> Column:
    """
    Custom transformation logic for calculating total price
    """
    return price * quantity

def get_state_full_name(state: Column) -> Column:
    """
    Returns the full name of a state given its code as a Column.
    """
    return when(state == "CA", lit("California")) \
        .when(state == "NY", lit("New York")) \
        .when(state == "TX", lit("Texas")) \
        .when(state == "FL", lit("Florida")) \
        .when(state == "WA", lit("Washington")) \
        .otherwise(lit("Unknown State"))


def remove_leading_trailing_spaces(df):
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