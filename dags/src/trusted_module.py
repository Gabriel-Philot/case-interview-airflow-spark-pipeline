"""
TRUSTED
"""
import os

os.chdir("..")

#unpivoted_df.coalesce(1).write.options(header='true').csv(csv_unpivot_test_path)

from pyspark.sql.functions import col

#from spark_module import create_spark_session, unpivot_via_sql
#from data_quality import count_csv_rows
from src.resources.spark_module import create_spark_session
from src.resources.process_tools import prejoin_function
from src.resources.variables import csv_confirmed_file_path, csv_deaths_file_path, csv_recovered_file_path, \
   parquet_trusted_path



col_qtde_conf = "quantidade_confirmados"
col_qtde_deaths = "quantidade_mortes"
col_qted_reco = "quantidade_recuperados"


def f_trusted_main():
    """
    Executes the main logic of the trusted module.

    This function reads data from CSV files containing confirmed cases, deaths, and recoveries of COVID-19. 
    It prepares the data for joining by unpivoting the tables and creating a join column. 
    Then, it performs an outer join on the three tables. After filtering rows with null values in the core columns, 
    it selects the desired columns and saves the result in a Parquet file.

    Parameters:
    None

    Returns:
    None
    """

    spark = create_spark_session()

    # Preparing for join
    # - Reading the data
    # - Unpivoting
    # - Creating the join column

    df_confirmed = spark.read.options(inferSchema='true', header='true').csv(csv_confirmed_file_path)
    df_prejoin_conf = prejoin_function(df_confirmed, col_qtde_conf)
    df_prejoin_conf = df_prejoin_conf.filter(df_prejoin_conf.join_col != "") 

    df_deaths = spark.read.options(inferSchema='true', header='true').csv(csv_deaths_file_path)
    df_prejoin_deaths = prejoin_function(df_deaths, col_qtde_deaths)
    df_prejoin_deaths = df_prejoin_deaths.filter(df_prejoin_deaths.join_col != "") \
        .select("join_col", col_qtde_deaths)

    df_recovered = spark.read.options(inferSchema='true', header='true').csv(csv_recovered_file_path)
    df_prejoin_rec = prejoin_function(df_recovered, col_qted_reco)
    df_prejoin_rec = df_prejoin_rec.filter(df_prejoin_rec.join_col != "") \
        .select("join_col", col_qted_reco)

    # Join
    # Here we are going with outer [join] because that in case there's some null
    # values in some of the columns, its better to lose precious data witch other joins.
    # but will prob have some impact on the KPI's and other metrics that we are searching
    # and its the role of a enginner to aproach business stakholders to define witch case wil
    # be the best for the goals of the solution.
    df_join = df_prejoin_conf.join(df_prejoin_deaths, on="join_col", how="outer") \
        .join(df_prejoin_rec, on="join_col", how="outer")
    
    print("Columns after join:", df_join.columns)
    
    # Filter rows with null values in core columns
    df_filter = df_join.filter(
        (col("join_col") != "") & (col("join_col").isNotNull()) &
        (col("pais").isNotNull()) & (col("pais") != "")
        ) \
        .select(
            "pais", "estado", "latitude", "longitude", "data",
            col_qtde_conf, col_qtde_deaths, col_qted_reco, "ano", "mes"
        )

    #df_filter.printSchema()

    # Saving in trusted zone
    df_filter.coalesce(1) \
        .write.mode('overwrite') \
        .partitionBy('ano', 'mes') \
        .parquet(parquet_trusted_path)

    spark.stop()








        
    

