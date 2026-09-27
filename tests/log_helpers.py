from pyspark.sql import DataFrame


def df_to_string(df: DataFrame, n: int = 20) -> str:
    """Returns the first n rows of a DataFrame as a table string, for use in log messages.

    DataFrame.show() prints to stdout and returns None, so it can't be passed to a logger.
    """
    return df.limit(n).toPandas().to_string()
