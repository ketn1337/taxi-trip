from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta

# Базовые аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Список таблиц для переноса
TABLES = ['yellow_tripdata', 'green_tripdata', 'taxi_zone']

with DAG(
    dag_id='pg_to_clickhouse',
    default_args=default_args,
    schedule_interval=None, # Запускаем вручную (или можно задать cron-выражение)
    catchup=False,
    tags=['postgres', 'clickhouse', 'manually'],
    description='Перенос таблиц такси из Postgres в ClickHouse',
) as dag:

    for table in TABLES:
        # 1. Создаем таблицу в ClickHouse, если ее нет. 
        # ClickHouse умеет копировать схему таблицы прямо из Postgres!
        create_table = ClickHouseOperator(
            task_id=f'create_table_{table}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"""
                CREATE TABLE IF NOT EXISTS default.{table}
                ENGINE = MergeTree
                ORDER BY tuple()
                AS SELECT * FROM postgresql('postgres:5432', 'airflow', '{table}', 'airflow', 'airflow')
                LIMIT 0
            """
        )

        # 2. Очищаем таблицу перед загрузкой (чтобы при перезапуске DAG данные не дублировались)
        truncate_table = ClickHouseOperator(
            task_id=f'truncate_{table}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"TRUNCATE TABLE IF EXISTS default.{table}"
        )

        # 3. Напрямую переливаем данные: ClickHouse сам запрашивает их из Postgres
        insert_data = ClickHouseOperator(
            task_id=f'insert_{table}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"""
                INSERT INTO default.{table}
                SELECT * FROM postgresql('postgres:5432', 'airflow', '{table}', 'airflow', 'airflow')
            """
        )

        # Выстраиваем зависимости: Создать -> Очистить -> Загрузить
        create_table >> truncate_table >> insert_data