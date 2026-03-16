from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

def migrate_entire_database():
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    dest_hook = PostgresHook(postgres_conn_id='postgres_destination')

    # 1. Получаем список всех таблиц и их схем
    # Исключаем системные схемы Postgres
    tables_query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND table_type = 'BASE TABLE';
    """
    tables_to_migrate = source_hook.get_records(tables_query)
    
    for schema, table in tables_to_migrate:
        full_table_name = f'"{schema}"."{table}"'
        print(f"Processing {full_table_name}...")

        # 2. Создаем схему в базе-приемнике, если её нет
        dest_hook.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        # 3. Получаем структуру колонок для конкретной таблицы
        columns_query = f"""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position;
        """
        columns_info = source_hook.get_records(columns_query)
        
        # Собираем DDL
        col_definitions = []
        for col in columns_info:
            name, dtype, length, nullable = col
            definition = f'"{name}" {dtype}'
            if length:
                definition += f"({length})"
            if nullable == 'NO':
                definition += " NOT NULL"
            col_definitions.append(definition)
        
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {full_table_name} ({", ".join(col_definitions)});'
        
        # 4. Создаем таблицу в приемнике
        dest_hook.run(create_table_sql)

        # 5. Переливаем данные
        # Важно: используем кавычки для имен, чтобы избежать проблем с регистром или спецсимволами
        records = source_hook.get_records(f"SELECT * FROM {full_table_name}")
        
        if records:
            # Преобразуем сложные типы данных (словари и списки) в JSON-строки
            prepared_records = []
            for row in records:
                new_row = []
                for cell in row:
                    if isinstance(cell, (dict, list)):
                        new_row.append(json.dumps(cell))
                    else:
                        new_row.append(cell)
                prepared_records.append(tuple(new_row))

            dest_hook.run(f"TRUNCATE TABLE {full_table_name} CASCADE;")
            
            dest_hook.insert_rows(
                table=full_table_name,
                rows=prepared_records, # Используем подготовленные данные
                target_fields=[f'"{c[0]}"' for c in columns_info],
                commit_every=1000
            )
            print(f"Migration of {full_table_name} successful. Rows: {len(records)}")

with DAG(
    dag_id='postgres_full_db_migration',
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='migrate_all_schemas_and_tables',
        python_callable=migrate_entire_database
    )