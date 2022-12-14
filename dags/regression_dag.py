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
    schedule_interval="0 9,14 * * *",
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    daily_data = SimpleHttpOperator(
        task_id="daily_data",
        http_conn_id="open_faas",
        endpoint="regression-daily-data",
        method="GET",
        response_check=is_response_ok,
    )
    engineer_fields = SimpleHttpOperator(
        task_id="engineer_fields",
        http_conn_id="open_faas",
        endpoint="regression-engineer-daily-fields",
        method="GET",
        response_check=is_response_ok,
    )
    info_watched_items = SimpleHttpOperator(
        task_id="info_watched_items",
        http_conn_id="open_faas",
        endpoint="regression-info-watched-items",
        method="GET",
        response_check=is_response_ok,
    )
    historical_data = SimpleHttpOperator(
        task_id="historical_data",
        http_conn_id="open_faas",
        endpoint="regression-historical-data",
        method="GET",
        response_check=is_response_ok,
    )
    historical_benchmarks = SimpleHttpOperator(
        task_id="historical_benchmarks",
        http_conn_id="open_faas",
        endpoint="regression-historical-benchmarks",
        method="GET",
        response_check=is_response_ok,
    )
    bucketing_time_series = SimpleHttpOperator(
        task_id="bucketing_time_series",
        http_conn_id="open_faas",
        endpoint="regression-bucketing-time-series",
        method="GET",
        response_check=is_response_ok,
    )
    merge_timeseries = SimpleHttpOperator(
        task_id="merge_timeseries",
        http_conn_id="open_faas",
        endpoint="regression-merge-timeseries",
        method="GET",
        response_check=is_response_ok,
    )
    scatter_plot_compute = SimpleHttpOperator(
        task_id="scatter_plot_compute",
        http_conn_id="open_faas",
        endpoint="regression-scatter-plot-compute",
        method="GET",
        response_check=is_response_ok,
    )
    summary = SimpleHttpOperator(
        task_id="summary",
        http_conn_id="open_faas",
        endpoint="regression-summary",
        method="GET",
        response_check=is_response_ok,
    )
    (
        daily_data
        >> engineer_fields
        >> info_watched_items
        >> historical_data
        >> historical_benchmarks
        >> bucketing_time_series
        >> merge_timeseries
        >> scatter_plot_compute
        >> summary
    )
