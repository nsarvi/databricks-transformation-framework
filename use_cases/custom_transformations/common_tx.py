"""Custom transformations shared by several domains."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import CharType, StringType, VarcharType

# ASCII control characters except tab (\x09), line feed (\x0A) and carriage return (\x0D)
CONTROL_CHARACTERS = r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"


def standardize_strings(df: DataFrame) -> DataFrame:
    """Technical cleanup of every string column, for source-aligned layers such as Bronze.

    For each STRING, CHAR or VARCHAR column: removes control characters, trims leading and trailing
    spaces (CHAR padding), and turns empty or whitespace-only values into NULL. CHAR and VARCHAR
    columns become STRING. Other columns are returned unchanged, in the same order.
    """
    columns = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (StringType, CharType, VarcharType)):
            cleaned = F.trim(F.regexp_replace(F.col(field.name).cast("string"), CONTROL_CHARACTERS, ""))
            columns.append(F.when(cleaned == "", None).otherwise(cleaned).alias(field.name))
        else:
            columns.append(F.col(field.name))
    return df.select(*columns)
