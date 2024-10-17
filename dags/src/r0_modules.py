"""
Refined
"""
import os
os.chdir("..")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


from src.resources.spark_module import create_spark_session
from src.resources.variables import parquet_trusted_path, parquet_refined_r0_path
    

def calculate_r0_weekly(df):
    """
    Calculates the R0 value for each country in the given DataFrame.

    Parameters:
        df (DataFrame): The DataFrame containing the data to calculate R0.
            The DataFrame should have the following columns:
            - "pais" (string): The name of the country.
            - "data" (datetime): The date of the data.
            - "quantidade_confirmados" (int): The number of confirmed cases.

    Returns:
        DataFrame: The input DataFrame with an additional column "R0".
            The "R0_week" column contains the calculated weekly R0 value for each country.
            If the previous day's cases are not available, the "R0" value is set to None.
    """
    # Convert the date column to week start dates
    df = df.withColumn("semana", F.date_trunc("week", F.col("data")))

    # Aggregate the data by country and week
    df_weekly = df.groupBy("pais",  "ano", "semana") \
                  .agg(F.sum("quantidade_confirmados").alias("casos_semana"))


    # Shifts previous days's cases 
    window_lag = Window.partitionBy("pais", "ano").orderBy(F.col("semana"))
    df_weekly = df_weekly.withColumn("casos_semana_anterior", 
                       F.lag("casos_semana").over(window_lag))

    df_weekly = df_weekly.withColumn(
        "R0_semanal", 
        F.when(
            F.col("casos_semana_anterior") > 0, 
            F.col("casos_semana") / F.col("casos_semana_anterior")
        ).otherwise(None)
    )

    return df_weekly


def f_r0_main():
    
    spark = create_spark_session()
    df = spark.read.parquet(parquet_trusted_path)
    
    df_r0 = calculate_r0_weekly(df)
    
    df_r0.coalesce(1) \
        .write.mode('overwrite') \
        .partitionBy('ano') \
        .parquet(parquet_refined_r0_path)

    spark.stop()


    