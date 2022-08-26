from airflow import DAG
from datetime import datetime
from airflow.operators.http_operator import SimpleHttpOperator

with DAG(
    dag_id='pnl_dag',
    description='PNL DAG',
    schedule_interval='*/20 * * * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    spread_pnl = SimpleHttpOperator(
        task_id='spread_pnl',
        http_conn_id='open_faas',
        endpoint='spread-pnl',
        method='GET',
    )
    cash_pnl = SimpleHttpOperator(
        task_id='cash_pnl',
        http_conn_id='open_faas',
        endpoint='cash-pnl',
        method='GET',
    )
    gross_reduction = SimpleHttpOperator(
        task_id='gross_reduction',
        http_conn_id='open_faas',
        endpoint='gross-reduction',
        method='GET',
    )
    per_book_cash_pnl = SimpleHttpOperator(
        task_id='per_book_cash_pnl',
        http_conn_id='open_faas',
        endpoint='per-book-cash-pnl',
        method='GET',
    )
    delta_pnl = SimpleHttpOperator(
        task_id='delta_pnl',
        http_conn_id='open_faas',
        endpoint='delta-pnl',
        method='GET',
    )
    spread_pnl >> cash_pnl >> per_book_cash_pnl >> delta_pnl
    cash_pnl >> gross_reduction