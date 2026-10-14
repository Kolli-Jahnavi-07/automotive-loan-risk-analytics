"""Daily loan risk pipeline.

ingest (Spark silver) -> dbt build (models + tests) -> report

dbt build runs tests after each model, so a failed data test stops downstream marts
from refreshing with bad data. Point PROJECT_DIR at the repo checkout on the worker.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/repos/automotive-loan-risk-analytics"
DBT_DIR = f"{PROJECT_DIR}/dbt/loan_warehouse"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="loan_risk_daily",
    description="Bronze -> silver (Spark) -> dbt warehouse and risk marts",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # after the servicing system's nightly extract lands
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["loan-risk", "spark", "dbt"],
) as dag:
    spark_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"cd {PROJECT_DIR} && PYTHONPATH=src python -m loanrisk.spark_silver",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir . --target {{{{ var.value.get('dbt_target', 'dev') }}}}",
    )

    report = BashOperator(
        task_id="publish_report",
        bash_command=f"cd {PROJECT_DIR} && python scripts/report.py",
    )

    spark_silver >> dbt_build >> report
