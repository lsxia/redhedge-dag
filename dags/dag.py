from airflow import DAG
from datetime import datetime
from airflow.operators.http_operator import SimpleHttpOperator

open_faas_endpoint = 'http://gateway.openfaas:8080/function/'
endpoints = {
    "gross_reduction": "gross-reduction",
}
with DAG(
    dag_id='my_dag',
    description='My DAG',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    gross_reduction = SimpleHttpOperator(
        task_id='gross_reduction',
        http_conn_id='http_default',
        endpoint=open_faas_endpoint + endpoints['gross_reduction'],
        method='GET',
    )
