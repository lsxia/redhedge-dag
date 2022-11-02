from datetime import datetime

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

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
    dag_id="start_of_day_sourcing",
    description="Start of day sourcing DAG",
    schedule_interval="15 8 * * *",
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
    update_bond_static = SimpleHttpOperator(
        task_id="update_bond_static",
        http_conn_id="open_faas",
        endpoint="update-bond-static",
        method="GET",
        response_check=is_response_ok,
    )
    update_main_equivalent = SimpleHttpOperator(
        task_id="update_main_equivalent",
        http_conn_id="open_faas",
        endpoint="update-main-equivalent-ratio",
        method="GET",
        response_check=is_response_ok,
    )
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
    update_benchmark_data = SimpleHttpOperator(
        task_id="update_benchmark_data",
        http_conn_id="open_faas",
        endpoint="update-benchmark-data",
        method="GET",
        response_check=is_response_ok,
    )
    (
        update_positions
        >> update_intraday
        >> update_bond_static
        >> update_main_equivalent
        >> compute_security_positions
        >> update_sourced_risk_data
        >> update_benchmark_data
    )
