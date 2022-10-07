from airflow import DAG
from datetime import datetime
from airflow.operators.http_operator import SimpleHttpOperator

# start of day sourcing functions: 
# - start of day import
# - update bond static
# - update main equivalent
# - compute security positions
# - update sourced risk data
# - update benchmark data
# recurent sourcing functions:
# - update intra day 
# - compute risk data
with DAG(
    dag_id='start_of_day_sourcing',
    description='Start of day sourcing DAG',
    schedule_interval='0 7 * * *',
    start_date=datetime(2019, 1, 1),
    catchup=False,
) as dag:
    # good
    update_positions = SimpleHttpOperator(
        task_id='start-of-day-import',
        http_conn_id='open_faas',
        endpoint='start-of-day-import',
        method='GET',
    )
    # good
    update_bond_static = SimpleHttpOperator(
        task_id='update_bond_static',
        http_conn_id='open_faas',
        endpoint='update-bond-static',
        method='GET',
    )
    # good
    update_main_equivalent = SimpleHttpOperator(
        task_id='update_main_equivalent',
        http_conn_id='open_faas',
        endpoint='update-main-equivalent-ratio',
        method='GET',
    )
    # good
    compute_security_positions = SimpleHttpOperator(
        task_id='compute_security_positions',
        http_conn_id='open_faas',
        endpoint='compute-security-positions',
        method='GET',
    )
    # good
    update_sourced_risk_data = SimpleHttpOperator(
        task_id='update_sourced_risk_data',
        http_conn_id='open_faas',
        endpoint='update-sourced-risk-data',
        method='GET',
    )
    # good
    update_benchmark_data = SimpleHttpOperator(
        task_id='update_benchmark_data',
        http_conn_id='open_faas',
        endpoint='update-benchmark-data',
        method='GET',
    )
    update_positions >> update_bond_static >> update_main_equivalent >> compute_security_positions >> update_sourced_risk_data >> update_benchmark_data