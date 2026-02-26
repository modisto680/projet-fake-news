FROM apache/airflow:2.9.0

COPY requirements-airflow.txt .

RUN pip install --no-cache-dir -r requirements-airflow.txt