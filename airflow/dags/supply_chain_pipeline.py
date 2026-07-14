"""
supply_chain_pipeline
======================
Orchestrates the DataCo supply chain pipeline end to end:

    load_raw.py  -->  validate_raw  -->  dbt run  -->  dbt test  -->  predict  -->  publish_to_neon

Each stage is a separate task so failures are isolated and visible in the
Airflow UI (e.g. you'll immediately see "dbt_test failed" rather than a
single opaque "pipeline failed").

Requires two Airflow Variables to be set (Admin -> Variables), so paths
aren't hardcoded to one machine:
    PROJECT_ROOT   e.g. /opt/airflow/project      (repo root, contains load_raw.py)
    DBT_PROJECT_DIR e.g. /opt/airflow/project/dbt/dataco_analytics
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Variable.get("PROJECT_ROOT", default_var="/opt/airflow/project")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from notifications.callbacks import on_start_callback, on_success_callback, on_failure_callback

DBT_PROJECT_DIR = Variable.get(
    "DBT_PROJECT_DIR", default_var="/opt/airflow/project/dbt/dataco_analytics"
)
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/opt/airflow/dbt_profiles")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="supply_chain_pipeline",
    description="Load raw DataCo CSV -> validate -> dbt -> predict -> publish to Neon",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dataco", "warehouse", "dbt", "ml", "neon"],
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
) as dag:

    def notify_start_wrapper(**context):
        on_start_callback(context)

    notify_start = PythonOperator(
        task_id="notify_start",
        python_callable=notify_start_wrapper,
    )

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

    predict = BashOperator(
        task_id="predict",
        bash_command=f"cd {PROJECT_ROOT}/ml && python predict.py --all-new",
    )

    publish_to_neon = BashOperator(
        task_id="publish_to_neon",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/publish_to_neon.py",
    )

    notify_start >> load_raw >> validate_raw >> dbt_run >> dbt_test >> predict >> publish_to_neon
