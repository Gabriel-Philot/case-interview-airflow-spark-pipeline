
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, col, lit, when
from pyspark.sql.functions import to_date, date_format, to_timestamp, col


def unpivot_via_sql(df_spark: DataFrame, name_column: str) -> DataFrame:
    """
    Performs an unpivot operation on a CSV file using SQL.
    obs: # much more easy than python rs

    Args:
        df_spark (DataFrame): The DataFrame containing the CSV data.

    Returns:
        DataFrame: The resulting DataFrame after the unpivot operation.
    """
    
    # Registers the DataFrame as a temporary view
    df_spark.createOrReplaceTempView("covid_data")
    
    # Generates a list of date columns dynamically
    columns = df_spark.columns
    non_date_columns = ['Province/State', 'Country/Region', 'Lat', 'Long']
    date_columns = [col for col in columns if col not in non_date_columns]
    
    # Creates a SQL query for unpivot and grouping
    query = f"""
    WITH unpivoted AS (
    SELECT `Country/Region`, `Province/State`, `Lat`, `Long`,
           stack({len(date_columns)}, {', '.join([f"'{date}', `{date}`" for date in date_columns])}) AS (Date, Value)
    FROM covid_data
    )
    SELECT `Country/Region` AS pais,
           `Province/State` AS estado,
           `Lat` AS latitude,
           `Long` AS longitude,
           to_timestamp(Date, 'M/d/yy') AS data, 
           cast(date_format(to_date(Date, 'M/d/yy'), 'M') as int) AS mes,  -- Extraindo o mês como inteiro
           cast(date_format(to_date(Date, 'M/d/yy'), 'yyyy') as int) AS ano,  -- Extraindo o ano como inteiro
           cast(first(Value) AS long) AS {name_column}
    FROM unpivoted
    GROUP BY `Country/Region`, `Province/State`, `Lat`, `Long`, to_timestamp(Date, 'M/d/yy'), cast(date_format(to_date(Date, 'M/d/yy'), 'M') as int), cast(date_format(to_date(Date, 'M/d/yy'), 'yyyy') as int)
    ORDER BY pais, estado, data
    """
    
    # Execute the SQL query using the Spark session from the DataFrame
    result_df = df_spark.sql_ctx.sql(query)
    return result_df
    


def prejoin_function(df_spark: DataFrame, col_qtde_name: str) -> DataFrame:
    """
    
    The unpivot operation is performed using a SQL query that stacks the date columns and 
    groups by the country, state, latitude, longitude, date, month, and year.
    
    The resulting DataFrame is then modified by adding a new column called "join_col", 
    which is used to join this DataFrame with other DataFrames in the DAG.
    
    Args:
        df_spark (DataFrame): The DataFrame containing the CSV data.
    
    Returns:
        DataFrame: The resulting DataFrame after the unpivot operation.
    """
    unpivoted_df = unpivot_via_sql(df_spark, col_qtde_name)

    unpivoted_df = unpivoted_df \
        .withColumn("estado", when(col("estado").isNull(), lit("")).otherwise(col("estado"))) \
        .withColumn("data", when(col("data").isNull(), lit("")).otherwise(col("data"))) \
        .withColumn("pais", when(col("pais").isNull(), lit("")).otherwise(col("pais"))) \
        .withColumn("join_col", concat(col("pais"), lit('_'), col('estado'), lit('_'), col("data")))

    return unpivoted_df#, spark
