from airflow import DAG
from datetime import datetime
from airflow.operators.http_operator import SimpleHttpOperator

with DAG(
    dag_id='call_heartbeat',
    description='Call Heartbeat',
    schedule_interval='/1 * * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    heartbeat = SimpleHttpOperator(
        task_id='pnl_airflow_heartbeat',
        http_conn_id='open_faas',
        endpoint='pnl-airflow-heartbeat',
        method='GET',
    )

    heartbeat