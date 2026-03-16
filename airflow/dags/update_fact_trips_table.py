from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime

with DAG(
    dag_id='fact_table_update',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly', # Запуск в начале каждого часа
    catchup=False,               # Не грузить исторические данные при активации
    max_active_runs=1            # Чтобы запуски не обгоняли друг друга
) as dag:

    delete_last_hour = ClickHouseOperator(
        task_id=f'delete_last_hour',
        clickhouse_conn_id='clickhouse_default',
        sql="""
            DELETE FROM fact_trips 
            WHERE pickup_datetime >= '{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}' 
            AND pickup_datetime < '{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}';
        """
    )

    update_yellow = ClickHouseOperator(
        task_id='update_yellow',
        clickhouse_conn_id='clickhouse_default',
        sql="""
            INSERT INTO fact_trips
            SELECT DISTINCT
                1,
                y.VendorID,
                y.RatecodeID,
                y.PULocationID,
                y.DOLocationID,
                y.payment_type,
                toDateTime(y.tpep_pickup_datetime) as pickup_datetime,
                toDayOfMonth(pickup_datetime),
                toHour(pickup_datetime),
                toDateTime(y.tpep_dropoff_datetime) as dropoff_datetime,
                toDayOfMonth(dropoff_datetime),
                toHour(dropoff_datetime),
                y.passenger_count,
                y.trip_distance,
                y.fare_amount,
                y.extra,
                y.mta_tax,
                y.tip_amount,
                y.tolls_amount,
                y.improvement_surcharge,
                y.total_amount,
                y.congestion_surcharge,
                y.Airport_fee,
                y.cbd_congestion_fee
            FROM yellow_tripdata y
            WHERE pickup_datetime >= '{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}'
              AND pickup_datetime <  '{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}'
        """
    )

    update_green = ClickHouseOperator(
        task_id='update_green',
        clickhouse_conn_id='clickhouse_default',
        sql="""
            INSERT INTO fact_trips
            SELECT DISTINCT
                2,
                g.VendorID,
                g.RatecodeID,
                g.PULocationID,
                g.DOLocationID,
                g.payment_type,
                toDateTime(g.lpep_pickup_datetime) as pickup_datetime,
                toDayOfMonth(pickup_datetime),
                toHour(pickup_datetime),
                toDateTime(g.lpep_dropoff_datetime) as dropoff_datetime,
                toDayOfMonth(dropoff_datetime),
                toHour(dropoff_datetime),
                g.passenger_count,
                g.trip_distance,
                g.fare_amount,
                g.extra,
                g.mta_tax,
                g.tip_amount,
                g.tolls_amount,
                g.improvement_surcharge,
                g.total_amount,
                g.congestion_surcharge,
                0,
                g.cbd_congestion_fee
            FROM green_tripdata g
            WHERE pickup_datetime >= '{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}'
              AND pickup_datetime <  '{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}'
        """
    )

    delete_last_hour >> [update_yellow, update_green]