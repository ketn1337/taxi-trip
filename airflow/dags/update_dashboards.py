from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
from include.datasets import FACT_TABLE_DATASET

# Конфигурация
SUPERSET_URL = "http://superst:8088"
USERNAME = "admin"
PASSWORD = "admin"

def refresh_all_dashboards():
    session = requests.Session()
    
    auth_res = session.post(f"{SUPERSET_URL}/api/v1/security/login", 
                            json={"password": PASSWORD, "provider": "db", "username": USERNAME})
    session.headers.update({"Authorization": f"Bearer {auth_res.json()['access_token']}"})

    dashboards = session.get(f"{SUPERSET_URL}/api/v1/dashboard").json()
    dashboards_ids = dashboards['ids']

    datasources = set()

    for dashboard_id in dashboards_ids:
        charts = session.get(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}/charts").json()['result']
        for i in charts:
            datasources.add(i['form_data']['datasource'])

    invalidate = session.post(f"{SUPERSET_URL}/api/v1/cachekey/invalidate", json={"datasource_uids": list(datasources)})
    print(f'Invalidate datasources {datasources} status: {invalidate.status_code}')

    for chart in charts:
        warmup_cache = session.put(f"{SUPERSET_URL}/api/v1/chart/warm_up_cache", json={"chart_id": int(chart['id'])})
        print(f'Warmup cache chart {chart['slice_name']} status: {warmup_cache.status_code}')


with DAG(
    dag_id="refresh_all_dashboards",
    start_date=datetime(2024, 1, 1),
    schedule_interval=[FACT_TABLE_DATASET],
    catchup=False
) as dag:

    refresh_task = PythonOperator(
        task_id="refresh_all_dashboards",
        python_callable=refresh_all_dashboards
    )