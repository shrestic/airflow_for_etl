from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="airflow_for_etl",
    schedule_interval="@weekly",
    start_date=datetime.now(),
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="plugins/scripts/sql",
    catchup=True,
) as dag:
    
    empty_task = EmptyOperator(task_id='empty_task')

