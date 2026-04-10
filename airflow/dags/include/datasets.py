from airflow.datasets import Dataset

PG_TO_CLICK_YELLOW_DATASET = Dataset("clickhouse://yellow_tripdata")
PG_TO_CLICK_GREEN_DATASET = Dataset("clickhouse://green_tripdata")
FACT_TABLE_DATASET = Dataset("clickhouse://fact_trips")