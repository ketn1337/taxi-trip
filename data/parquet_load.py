import pandas as pd
from sqlalchemy import create_engine

# Чтение parquet
df_yellow = pd.read_parquet('data/yellow_tripdata_2025-11.parquet', engine='pyarrow')
df_green = pd.read_parquet('data/green_tripdata_2025-11.parquet', engine='pyarrow')
df_taxi_zone = pd.read_csv('data/taxi_zone_lookup.csv', engine='pyarrow')

# Подключение к Postgres
engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')

# Запись в таблицу
# df_yellow.to_sql('yellow_tripdata', engine, if_exists='append', index=False)
# df_green.to_sql('green_tripdata', engine, if_exists='append', index=False)
df_taxi_zone.to_sql('taxi_zone', engine, if_exists='append', index=False)