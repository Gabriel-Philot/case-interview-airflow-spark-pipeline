from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def count_csv_rows(file_path: str, spark: SparkSession) -> int:
    """
    Counts the number of rows in a CSV file using PySpark.
    
    Args:
        file_path (str): The path to the CSV file.
    
    Returns:
        int: The number of rows in the CSV file.
    """
    df = spark.read.options(inferSchema='true', header='true').csv(file_path)
    row_count = df.count()
    print(f"Number of rows in {file_path}: {row_count}")
    return row_count


def group_validation(df: DataFrame, country: str, quantity: str) -> DataFrame:
    """
    Groups and filters deaths by country, year, and month, and orders the results.
    
    Args:
    df (DataFrame): Input DataFrame.
    country (str): Name of the country to filter.
    quantity (str): Name of the column containing the quantity of deaths.
    
    Returns:
    DataFrame: Grouped, filtered, and ordered DataFrame.
    """
    dfgrouped_check = df \
        .groupBy("pais", "ano", "mes") \
        .agg(F.sum(quantity).alias(quantity)) \
        .filter(F.col("pais") == country) \
        .drop("pais") \
        .orderBy(F.col("ano").asc(), F.col("mes").asc())
    
    return dfgrouped_check