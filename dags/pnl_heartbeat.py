from airflow import DAG
from datetime import datetime
from time import sleep
import json

from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


with DAG('pnl_heartbeat', schedule_interval='*/1 * * * *', start_date=datetime(2021, 1, 1), catchup=False) as dag:

    t1 = SimpleHttpOperator(
        task_id='trigger_dag',
        http_conn_id='airflow_api',
        endpoint='/api/v1/dags/pnl_dag/dagRuns',
        method='POST',
        data=json.dumps({
            "conf": {}
        }),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic YWRtaW46YWRtaW4="
        },
        response_check=lambda response: True if response.status_code == 200 else False,
    )
    delay_python_task: PythonOperator = PythonOperator(task_id="trigger_dag",
                                                   dag=t1,
                                                   python_callable=lambda: sleep(20))

    
    t1 >> delay_python_task >> delay_python_task
        