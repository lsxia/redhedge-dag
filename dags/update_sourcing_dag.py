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
    dag_id="update_sourcing",
    description="Updates the start of day sourcing DAG with the latest data ( every hour)",
    schedule_interval="* 8-17 * * *",
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:

    compute_security_positions = SimpleHttpOperator(
        task_id="compute_security_positions",
        http_conn_id="open_faas",
        endpoint="compute-security-positions",
        method="GET",
        response_check=is_response_ok,
    )
    update_sourced_risk_data = SimpleHttpOperator(
        task_id="update_sourced_risk_data",
        http_conn_id="open_faas",
        endpoint="update-sourced-risk-data",
        method="GET",
        response_check=is_response_ok,
    )
    compute_risk_data = SimpleHttpOperator(
        task_id="compute_risk_data",
        http_conn_id="open_faas",
        endpoint="compute-risk-data",
        method="GET",
        response_check=is_response_ok,
    )
    (
        update_sourced_risk_data
        >> compute_risk_data
    )
