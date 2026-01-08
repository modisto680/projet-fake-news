from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Dossier MONTÉ dans le conteneur via docker-compose: ./:/opt/airflow/scripts
PROJECT_ROOT = "/opt/airflow/scripts"

default_args = {
    "owner": "pyboisson",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bluesky_login_and_collect",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bluesky", "mongo"],
) as dag:

    login = BashOperator(
        task_id="login",
        cwd=PROJECT_ROOT,
        bash_command=f'python "{PROJECT_ROOT}/login.py"',
    )

    collect = BashOperator(
        task_id="collect_posts",
        cwd=PROJECT_ROOT,
        bash_command=f'python "{PROJECT_ROOT}/recolte_posts.py"',
    )

    login >> collect
