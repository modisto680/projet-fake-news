from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Dans docker-compose, on monte:
# - ./scripts -> /opt/airflow/scripts
# - ./kedro   -> /opt/airflow/kedro
SCRIPTS_DIR = "/opt/airflow/scripts"
KEDRO_DIR = "/opt/airflow/kedro/facts_check"

default_args = {
    "owner": "thumalien",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="thumalien_collect_and_vectorize",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",  # toutes les 6 heures
    catchup=False,
    max_active_runs=1,
    tags=["bluesky", "mongo", "kedro", "nlp", "tfidf", "credibility"],
) as dag:
    login = BashOperator(
        task_id="login_bluesky",
        cwd=SCRIPTS_DIR,
        bash_command=f'python "{SCRIPTS_DIR}/login.py"',
    )

    collect_raw = BashOperator(
        task_id="collect_raw_posts",
        cwd=SCRIPTS_DIR,
        bash_command=f'python "{SCRIPTS_DIR}/collect_posts.py"',
    )

    preprocess_and_vectorize = BashOperator(
        task_id="kedro_preprocess_and_vectorize",
        cwd=KEDRO_DIR,
        # preprocessing + tfidf (voir pipeline_registry.py)
        bash_command="kedro run --pipeline preprocessing && kedro run --pipeline tfidf",
    )

    score_credibility = BashOperator(
        task_id="score_credibility",
        cwd=KEDRO_DIR,
        # Applique le modèle fake_news sur les posts nettoyés → scored_posts (MongoDB)
        bash_command="kedro run --pipeline credibility_scoring",
    )

    # Flux : login → collecte → nettoyage+vectorisation → scoring de crédibilité
    login >> collect_raw >> preprocess_and_vectorize >> score_credibility

