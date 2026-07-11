"""
supply_chain_pipeline
======================
Orchestrates the DataCo supply chain pipeline end to end:

    load_raw.py  -->  dbt run  -->  dbt test

Each stage is a separate task so failures are isolated and visible in the
Airflow UI (e.g. you'll immediately see "dbt_test failed" rather than a
single opaque "pipeline failed").

Requires two Airflow Variables to be set (Admin -> Variables), so paths
aren't hardcoded to one machine:
    PROJECT_ROOT   e.g. /opt/airflow/project      (repo root, contains load_raw.py)
    DBT_PROJECT_DIR e.g. /opt/airflow/project/dbt/dataco_analytics
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Variable.get("PROJECT_ROOT", default_var="/opt/airflow/project")
DBT_PROJECT_DIR = Variable.get(
    "DBT_PROJECT_DIR", default_var="/opt/airflow/project/dbt/dataco_analytics"
)
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/home/airflow/.dbt")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # flip to True + set 'email' key once you wire up alerting
}

with DAG(
    dag_id="supply_chain_pipeline",
    description="Load raw DataCo CSV -> dbt run -> dbt test",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataco", "warehouse", "dbt"],
) as dag:

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/load_raw.py",
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/validate_raw.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    load_raw >> validate_raw >> dbt_run >> dbt_test
