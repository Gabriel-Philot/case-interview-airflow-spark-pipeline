"""
Refined
"""
import os
os.chdir("..")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


from src.resources.spark_module import create_spark_session
from src.resources.variables import parquet_trusted_path, parquet_refined_path
    
def f_refined_main():
    """
    Generates the refined data by performing aggregation and calculation of moving averages on the grouped data.
    
    This function reads the data from the parquet file specified by `parquet_trusted_path`, groups it 
    by "pais", "ano", "mes", and "data", and calculates the sum of "quantidade_confirmados", 
    "quantidade_mortes", and "quantidade_recuperados" for each group. 
    
    Then, it applies a window function to calculate the moving averages of "quantidade_confirmados", "quantidade_mortes", 
    and "quantidade_recuperados" for each "pais" using a sliding window of 7 days. 
    
    Finally, it selects the required columns, writes the refined data to the 
    parquet file specified by `parquet_refined_path`, and prints the schema of the refined data.
    
    Parameters:
    None
    
    Returns:
    None
    """
    
    spark = create_spark_session()
    df = spark.read.parquet(parquet_trusted_path)
    
    df_grouped = df.groupBy("pais", "ano", "mes", "data") \
        .agg(F.sum("quantidade_confirmados").alias("quantidade_confirmados"),
             F.sum("quantidade_mortes").alias("quantidade_mortes"),
             F.sum("quantidade_recuperados").alias("quantidade_recuperados")
             )
    
    # Here our fomous window function thats perfect
    # for this kind of agregation
    window_moving_avarege  = Window.partitionBy("pais") \
        .orderBy(F.col("data")).rowsBetween(-6, 0)
             

    df_window_ma = df_grouped \
        .withColumn("media_movel_confirmados", F.avg(F.col("quantidade_confirmados")).over(window_moving_avarege)) \
        .withColumn("media_movel_mortes", F.avg(F.col("quantidade_mortes")).over(window_moving_avarege)) \
        .withColumn("media_movel_recuperados", F.avg(F.col("quantidade_recuperados")).over(window_moving_avarege))
    
    df_refined = df_window_ma \
        .withColumn("media_movel_confirmados", F.round("media_movel_confirmados").cast("long")) \
        .withColumn("media_movel_mortes", F.round("media_movel_mortes").cast("long")) \
        .withColumn("media_movel_recuperados", F.round("media_movel_recuperados").cast("long")) \
        .select(
            "pais", "data", 
            "media_movel_confirmados", "media_movel_mortes", 
            "media_movel_recuperados", "ano"
        )
    
    df_refined.coalesce(1) \
        .write.mode('overwrite') \
        .partitionBy('ano') \
        .parquet(parquet_refined_path)

    spark.stop()


    