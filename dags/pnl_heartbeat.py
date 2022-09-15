from airflow import DAG
from datetime import datetime
from time import sleep
# calls per minute 
cpm = 6
# import requests
# import json

# url = "http://localhost:4169//api/v1/dags/pnl_dag/dagRuns"

# payload = json.dumps({
#   "conf": {}
# })
# headers = {
#   'Authorization': 'Basic YWRtaW46YWRtaW4=',
#   'Content-Type': 'application/json',
#   'Cookie': 'session=e442f902-cd0e-40f1-990f-bc6279b60ee1.PS4-bs6NEuW7AIiKDdE6HLRWTwU'
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(response.text)
# the dag should call the api every 10 seconds
# 6 calls per minute
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


with DAG('pnl_heartbeat', schedule_interval='*/1 * * * *', start_date=datetime(2021, 1, 1), catchup=False) as dag:

    t1 = SimpleHttpOperator(
        task_id='trigger_dag',
        http_conn_id='airflow_api',
        endpoint='/api/v1/dags/pnl_dag/dagRuns',
        method='POST',
        data={
            {
                "conf": { }
            }
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic YWRtaW46YWRtaW4="
        },
        response_check=lambda response: True if response.status_code == 200 else False,
    )

    
    def trigger(frequency):
        t1
        sleep(60/frequency)
        