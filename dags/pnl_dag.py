from datetime import datetime

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from requests import JSONDecodeError


def is_response_ok(response) -> bool:
    try:
        errors = response.json()["errors"]
    except (JSONDecodeError, AttributeError, KeyError) as exc:
        ok = True
    else:
        ok = not any(errors)
    finally:
        return ok


with DAG(
    dag_id="pnl_dag",
    description="PNL DAG",
    schedule_interval="0 0 1 1 1",
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    spread_pnl = SimpleHttpOperator(
        task_id="spread_pnl",
        http_conn_id="open_faas",
        endpoint="spread-pnl",
        method="GET",
        response_check=is_response_ok,
    )
    cash_pnl = SimpleHttpOperator(
        task_id="cash_pnl",
        http_conn_id="open_faas",
        endpoint="cash-pnl",
        method="GET",
        response_check=is_response_ok,
    )
    per_book_cash_pnl = SimpleHttpOperator(
        task_id="per_book_cash_pnl",
        http_conn_id="open_faas",
        endpoint="per-book-cash-pnl",
        method="GET",
        response_check=is_response_ok,
    )
    delta_pnl = SimpleHttpOperator(
        task_id="delta_pnl",
        http_conn_id="open_faas",
        endpoint="delta-pnl",
        method="GET",
        response_check=is_response_ok,
    )
    pnl_status_notification = SimpleHttpOperator(
        task_id="pnl_status_notification",
        http_conn_id="open_faas",
        endpoint="pnl-status-notification",
        method="GET",
        response_check=is_response_ok,
    )
    spread_pnl >> cash_pnl >> per_book_cash_pnl >> delta_pnl
    pnl_status_notification
