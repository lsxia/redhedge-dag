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
    dag_id="regression_dag",
    description="Regression DAG",
    schedule_interval="0 0 * * *",
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    

