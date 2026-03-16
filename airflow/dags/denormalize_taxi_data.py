from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Базовые аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='star_schema_create',
    default_args=default_args,
    schedule_interval=None, # Запускаем вручную (или можно задать cron-выражение)
    catchup=False,
    tags=['star schema', 'manually', 'clickhouse'],
    description='Денормализация данных такси из Postgres в ClickHouse',
) as dag:

    tables = {
        'fact_trips': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS fact_trips (
                    -- Keys
                    taxi_type_id UInt8,
                    vendor_id UInt8,
                    ratecode_id UInt8,
                    PU_location_id UInt16,
                    DO_location_id UInt16,
                    payment_id UInt8,

                    -- Time
                    pickup_datetime DateTime,
                    pickup_day UInt8,
                    pickup_hour UInt8,
                    dropoff_datetime DateTime,
                    dropoff_day UInt8,
                    dropoff_hour UInt8,

                    -- Metrics
                    passenger_count UInt8,
                    trip_distance Float32,
                    fare_amount Decimal(10,2),
                    extra Decimal(10,2),
                    mta_tax Decimal(10,2),
                    tip_amount Decimal(10,2),
                    tolls_amount Decimal(10,2),
                    improvement_surcharge Decimal(10,2),
                    total_amount Decimal(10,2),
                    congestion_surcharge Decimal(10,2),
                    airport_fee Decimal(10,2),
                    cbd_congestion_fee Decimal(10,2)
                )
                ENGINE = MergeTree()
                PARTITION BY toDate(pickup_datetime)
                ORDER BY (pickup_day, pickup_hour, taxi_type_id, vendor_id);
            """,
            'insert': {
                'green': f"""
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
                """,
                'yellow': f"""
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
                    FROM yellow_tripdata y;
                """
            }
        },
        'dim_taxi_type': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS dim_taxi_type (
                    taxi_type_id UInt8,
                    name String
                )
                ENGINE = MergeTree()

                ORDER BY (taxi_type_id);
            """,
            'insert': f"""
                INSERT INTO dim_taxi_type VALUES
                (1, 'Yellow'),
                (2, 'Green');
            """
        },
        'dim_vendor': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS dim_vendor (
                    vendor_id UInt8,
                    name String
                )
                ENGINE = MergeTree()

                ORDER BY (vendor_id);
            """,
            'insert': f"""
                INSERT INTO dim_vendor VALUES
                (1, 'Creative Mobile Technologies, LLC'),
                (2, 'Curb Mobility, LLC'),
                (6, 'Myle Technologies Inc'),
                (7, 'Helix');
            """
        },
        'dim_ratecode': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS dim_ratecode (
                    ratecode_id UInt8,
                    name String
                )
                ENGINE = MergeTree()

                ORDER BY (ratecode_id);
            """,
            'insert': f"""
                INSERT INTO dim_ratecode VALUES
                (1, 'Standard rate'),
                (2, 'JFK'),
                (3, 'Newark'),
                (4, 'Nassau or Westchester'),
                (5, 'Negotiated fare'),
                (6, 'Group ride'),
                (99, 'Null/unknown');
            """
        },
        'dim_location': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS dim_location (
                    location_id UInt16,
                    borough String,
                    zone String,
                    service_zone String
                )
                ENGINE = MergeTree()

                ORDER BY (location_id);
            """,
            'insert': f"""
                INSERT INTO dim_location
                SELECT *
                FROM taxi_zone;
            """
        },
        'dim_payment': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS dim_payment (
                    payment_id UInt8,
                    payment_type String
                )
                ENGINE = MergeTree()

                ORDER BY (payment_id);
            """,
            'insert': f"""
                INSERT INTO dim_payment VALUES
                (0, 'Flex Fare trip'),
                (1, 'Credit card'),
                (2, 'Cash'),
                (3, 'No charge'),
                (4, 'Dispute'),
                (5, 'Unknown'),
                (6, 'Voided trip');
            """
        },
    }

    # Проходим циклом по нашему словарю таблиц
    for table_name, queries in tables.items():
        
        # 1. Задача создания таблицы
        create_task = ClickHouseOperator(
            task_id=f'create_{table_name}',
            clickhouse_conn_id='clickhouse_default',
            sql=queries['create']
        )

        # 2. Задача очистки таблицы (для идемпотентности)
        truncate_task = ClickHouseOperator(
            task_id=f'truncate_{table_name}',
            clickhouse_conn_id='clickhouse_default',
            sql=f"TRUNCATE TABLE IF EXISTS {table_name}"
        )

        # 3. Задача вставки данных
        insert_task = ClickHouseOperator(
            task_id=f'insert_{table_name}',
            clickhouse_conn_id='clickhouse_default',
            sql=queries['insert']
        ) if table_name != 'fact_trips' else [ClickHouseOperator(
            task_id=f'insert_{table_name}_{i}',
            clickhouse_conn_id='clickhouse_default',
            sql=queries['insert'][i]
        ) for i in ('yellow', 'green')]
            

        # Выстраиваем зависимость внутри итерации:
        create_task >> truncate_task >> insert_task
