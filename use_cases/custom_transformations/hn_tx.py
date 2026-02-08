from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql import SparkSession


def with_exchange_rate(df, currency_tbl, date_dim_tbl):
    spark = SparkSession.builder.getOrCreate()

    df_f3 = spark.table(currency_tbl).filter(F.col("tcurr") == 'USD').filter(F.col("kurst") == 'M')
    df_f3_eff_dat = spark.table(date_dim_tbl)

    df_f2 = df_f3.join(df_f3_eff_dat, df_f3["gdatu"] == df_f3_eff_dat["days_until_99999999"], "inner") 
    df_f2 = df_f2.groupBy(F.col("fcurr")).agg(F.max(F.col("dna_date")).alias("max_gdatu")).drop("fcurr")

    df_f1 = spark.table(currency_tbl).filter(F.col("tcurr") == 'USD').filter(F.col("kurst") == 'M')
    df_f1_eff_dat = spark.table(date_dim_tbl)
    df_exch_rt_latest = df_f1.join(df_f1_eff_dat, df_f1["gdatu"] == df_f1_eff_dat["days_until_99999999"], "inner")
    
    df_exch_rt_latest = df_exch_rt_latest.join(df_f2, df_exch_rt_latest.dna_date == df_f2.max_gdatu, "inner")
    
    df_exch_rt_latest = (
          df_exch_rt_latest.groupBy(F.col("fcurr"), F.expr("ukurs/ffact*tfact").alias("latest_rate"))
          .agg(F.max(F.col("dna_date")).alias("maxdt"))
          ).drop("ukurs","ffact", "kurst","tfact","ffact")

    df = df.join(df_exch_rt_latest, df.curr_code == df_exch_rt_latest.fcurr, "left").drop(df_exch_rt_latest["fcurr"])
    return df