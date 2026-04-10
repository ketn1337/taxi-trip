from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime
from include.datasets import PG_TO_CLICK_YELLOW_DATASET, PG_TO_CLICK_GREEN_DATASET


with DAG(
    dag_id='nrt_taxi_ingestion',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly', # Запуск в начале каждого часа
    catchup=False,               # Не грузить исторические данные при активации
    max_active_runs=1            # Чтобы запуски не обгоняли друг друга
) as dag:

    tables = [('yellow_tripdata', 'tpep_pickup_datetime'), ('green_tripdata', 'lpep_pickup_datetime')]

    for table_name, time_column in tables:

        delete_last_hour = ClickHouseOperator(
            task_id=f'delete_last_hour_{table_name}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"""
                DELETE FROM default.{table_name}
                WHERE {time_column} >= '{{{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}}}'
                AND {time_column} <  '{{{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}}}'
            """
        )

        load_data = ClickHouseOperator(
            task_id=f'load_last_hour_{table_name}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"""
                INSERT INTO default.{table_name}
                SELECT * FROM postgresql(
                    'postgres:5432', 'airflow', {table_name}, 'airflow', 'airflow'
                )
                WHERE {time_column} >= '{{{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}}}'
                AND {time_column} <  '{{{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}}}'
            """,
            outlets=[PG_TO_CLICK_YELLOW_DATASET, PG_TO_CLICK_GREEN_DATASET]
        )

        
        delete_last_hour >> load_data
