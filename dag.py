rom airflow import DAG
from datetime import datetime
from airflow.operators.faas_operator import FaasOperator

with DAG(
    dag_id='my_dag',
    description='My DAG',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    gross_reduction = FaasOperator(
        task_id='gross_reduction',
        function_name='gross_reduction',
        inlets=['{{ ds }}'],
        outlets=['{{ ds }}'],
    )
