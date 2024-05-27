# variables for dags

datalake_path = '/home/airflow/datalake'

# Csv paths

# Raw
csv_raw_path = f'{datalake_path}/raw/covid19'
csv_confirmed_file_path = f'{csv_raw_path}/time_series_covid19_confirmed_global.csv'
csv_deaths_file_path = f'{csv_raw_path}/time_series_covid19_deaths_global.csv'
csv_recovered_file_path = f'{csv_raw_path}/time_series_covid19_recovered_global.csv'

# Trusted
parquet_trusted_path = f'{datalake_path}/trusted/covid19'

# Refined
parquet_refined_path = f'{datalake_path}/refined/covid19'

# R0_v0
parquet_refined_r0_path = f'{datalake_path}/research_r0/covid19'


# dev/test

# csv_unpivot_test_path = f'{datalake_path }/test.csv'

