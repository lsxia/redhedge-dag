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
    dag_id="update_blotters",
    description="Updates the start of day sourcing DAG with the latest data ( every 10 minutes)",
    schedule_interval="*/10 8-17 * * *",
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    update_positions = SimpleHttpOperator(
        task_id="start_of_day_import",
        http_conn_id="open_faas",
        endpoint="start-of-day-import",
        method="GET",
        response_check=is_response_ok,
    )
    update_intraday = SimpleHttpOperator(
        task_id="intraday_import",
        http_conn_id="open_faas",
        endpoint="intraday-import",
        method="GET",
        response_check=is_response_ok,
    )

    (
        update_positions
        >> update_intraday
    )
