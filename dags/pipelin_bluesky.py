from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'facts_check',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_bluesky',  # ✅ nom du DAG sans ".py"
    default_args=default_args,
    description='Pipeline automatisée Bluesky - Fake News Detection',
    schedule_interval='@hourly',  # exécution chaque heure
    start_date=datetime(2025, 11, 6),
    catchup=False,
) as dag:

    # === Étape 1 : Connexion / récupération du token ===
    login = BashOperator(
        task_id='login',
        bash_command='python /opt/airflow/scripts/login.py'
    )

    # === Étape 2 : Récolte des posts (multi-endpoints) ===
    recolte = BashOperator(
        task_id='recolte_posts_all',
        bash_command='python /opt/airflow/scripts/recolte_posts_all.py'
    )

    # === Étape 3 : Chargement dans MongoDB ===
    charger = BashOperator(
        task_id='charger_mongoDB',
        bash_command='python /opt/airflow/scripts/charger_mongoDB.py'
    )

    # === Orchestration du pipeline ===
    login >> recolte >> charger
