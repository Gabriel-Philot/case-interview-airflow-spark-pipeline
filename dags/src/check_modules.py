"""
Check
"""

import os
os.chdir("..")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.resources.spark_module import create_spark_session
from src.resources.data_quality import count_csv_rows, group_validation
from src.resources.variables import csv_confirmed_file_path, csv_deaths_file_path, csv_recovered_file_path, \
    parquet_trusted_path, parquet_refined_path


# This function its only a example of things you can check in the ingestion/raw files to see if
# ingestion/files metrics like rows/size /schema are ok.
def f_check_main(**kwargs):
    """
    Function that verifies if the csv files are available and have rows.

    Verifies if the csv files have rows, without considering exceptions.
    """
    try:
        spark = create_spark_session()
        confirmed_count = count_csv_rows(csv_confirmed_file_path, spark)
        deaths_count = count_csv_rows(csv_deaths_file_path, spark)
        recover_count =count_csv_rows(csv_recovered_file_path, spark)
        spark.stop()
        return confirmed_count

    except Exception as e:
        spark.stop()
        raise Exception("Error while checking the files. Verify if the files are available and without problems.") from e


def branch_check(**kwargs):
    ti = kwargs['ti']
    confirmed = ti.xcom_pull(task_ids='check_task_ingestion')

    if confirmed  > 250:
        return 'trusted_task'
    return 'simulated_action'


def simulated_task():
    print("This is the simulated task that in case the branch check returns false, will be executed.")



# Here same as check 1, but i did actually use it to see if after the join the values with agregations wil
# still be matching the source.
def f_check_02_main():
    spark = create_spark_session()
    
    df_loaded = spark.read.parquet(parquet_trusted_path)
    df_loaded.printSchema()
    df_loaded.orderBy(F.col("data").asc(), F.col("pais").asc()).show()

    confirmed_agregation_check = group_validation(df_loaded, "Australia", "quantidade_confirmados")
    confirmed_agregation_check.show()

    death_agregation_check = group_validation(df_loaded, "Brazil", "quantidade_mortes")
    death_agregation_check.show()

    recover_agregation_check = group_validation(df_loaded, "Egypt", "quantidade_recuperados")
    recover_agregation_check.show()

    spark.stop()


# Same as the other one, but now just, in the context of completition and observing the results
def f_check_03_main():
    spark = create_spark_session()

    df_loaded = spark.read.parquet(parquet_refined_path)
    df_loaded.printSchema()
    df_loaded.orderBy(F.col("media_movel_confirmados").desc()).show()
    df_loaded.orderBy(F.col("media_movel_mortes").desc()).show()
    df_loaded.orderBy(F.col("media_movel_recuperados").desc()).show()

    spark.stop()