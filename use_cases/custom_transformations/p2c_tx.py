from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from typing import List

def with_col_lit(df, col_name, lit_value="x"):
    """
    Adds a new column with a literal value to the DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.
        col_name (str): The name of the new column.
        lit_value: The literal value to assign to the new column.
    
    Returns:
        DataFrame: The DataFrame with the new column added.
    """
    return df.withColumn(col_name, F.lit(lit_value))



def currency_convert_from_source_currency_to_target_currency_fun(df, target_currency_code_column):
    df = (
        df.filter(F.col("KURST") == 'M')
        .withColumn("date", ((99999999 - F.col("GDATU"))))
        .select(
            "KURST", "FCURR", "TCURR", "GDATU",
            F.to_date(
                F.concat(
                    F.substring(
                        F.col("date").cast("int"), 1, 4
                        ),  
                        F.lit('-'),
                        F.substring(
                            F.col("date").cast("int"), 5, 2
                            ), 
                            F.lit('-'),
                            F.substring(
                                F.col("date").cast("int"), 7, 2)
                                )
                ).alias("date_converted"),
                F.when(F.col("tfact") < 0, 
                       1 / F.abs(F.col("ukurs"))
                       )
                .when(F.col("tfact") == 0, 
                      F.col("ukurs")
                      )
                .otherwise(F.col("ukurs") / F.col("ffact")).alias("exchange_rate"))
    )

    # Filter currency data for conversion to target currency code
    df = (
        df.filter(F.col("TCURR") == target_currency_code_column)
        .select("FCURR", "TCURR", "date_converted", "exchange_rate")
    )
    return df


# Define UDF to convert Julian date to calendar date
def julian_to_date(julian_date):
    """
    Convert Julian date to calendar date.

    Args:
        julian_date (int): Julian date.

    Returns:
        str: Calendar date in 'YYYY-MM-DD' format.
        Input Julian Date : 2459216
        Output Date : 2020-12-31
    """
    year = 1900 + int(julian_date / 1000)
    days = julian_date % 1000
    date = datetime(year, 1, 1) + timedelta(days - 1)
    return date.strftime('%Y-%m-%d')


def convert_julian_dates(df: DataFrame, julian_date_cols: List) -> DataFrame:
    """
    Convert Julian date columns to calendar date format.

    Args:
        df (DataFrame): Input DataFrame.
        julian_date_cols (List): List of Julian date column names.

    Returns:
        DataFrame: DataFrame with converted date columns.
    """
    for col in julian_date_cols:
        df = df.withColumn(f"{col}_str", F.col(col).cast("string"))
        df = df.withColumn(f"{col}", 
                           F.when(
                               F.length(F.col(f"{col}_str")) == 6,  F.to_date(F.substring(F.col(f"{col}_str"),2,5), 'yyDDD')
                           )
                           .otherwise(F.to_date(F.col(f"{col}_str"), 'yyDDD'))
        ).drop(f"{col}_str")
    return df




def convert_due_date_cols(df: DataFrame, due_date_cols: List) -> DataFrame: 

    for col in due_date_cols:
        df = df.withColumn(f"{col}", 
                           F.when(
                               F.substring(F.col(f"{col}"), 1, 1) == 'C',
                               F.to_date(F.concat(F.lit("202"), 
                                                  F.substring(F.col(f"{col}"), 2, 1),
                                                  F.substring(F.col(f"{col}"), 3, 2), 
                                                  F.substring(F.col(f"{col}"), 5, 2)), 
                                                  'yyyyMMdd')
                           )
                           .when(
                                 F.substring(F.col(f"{col}"), 1, 1) == 'B',
                                 F.to_date(F.concat(F.lit("201"), 
                                                    F.substring(F.col(f"{col}"), 2, 1), 
                                                    F.substring(F.col(f"{col}"), 3, 2), 
                                                    F.substring(F.col(f"{col}"), 5, 2)), 
                                                    'yyyyMMdd')
                            )
                            .when(
                                 F.substring(F.col(f"{col}"), 1, 1) == 'A',
                                 F.to_date(F.concat(F.lit("200"), 
                                                    F.substring(F.col(f"{col}"), 2, 1), 
                                                    F.substring(F.col(f"{col}"), 3, 2), 
                                                    F.substring(F.col(f"{col}"), 5, 2)), 
                                                    'yyyyMMdd')
                            )
                           .otherwise(F.to_date(F.concat(F.lit("19"), 
                                                         F.substring(F.col(f"{col}"), 1, 2), 
                                                         F.substring(F.col(f"{col}"), 3, 2),  
                                                         F.substring(F.col(f"{col}"), 5, 2)), 
                                                         'yyyyMMdd'))
        )
    return df