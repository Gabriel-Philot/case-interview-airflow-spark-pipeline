from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    """
    Creates a SparkSession object with the specified configuration and returns it.

    Returns:
        SparkSession: A SparkSession object with the specified configuration.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("airflow_app") \
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .config("spark.driver.maxResultSize", "1048MB") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()
    return spark



    
