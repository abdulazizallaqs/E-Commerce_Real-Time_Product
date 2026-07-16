from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - allows the file to be imported in lightweight environments
    DAG = None
    PythonOperator = None


def run_ingestion():
    import subprocess
    subprocess.run(["python", "ingestion/kafka_producer.py"], check=False)


def run_lakehouse():
    import subprocess
    subprocess.run(["python", "lakehouse/spark_pipeline.py"], check=False)


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="ecommerce_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=["ecommerce", "capstone"],
    ) as dag:
        ingest_task = PythonOperator(task_id="ingest_products", python_callable=run_ingestion)
        lakehouse_task = PythonOperator(task_id="run_lakehouse", python_callable=run_lakehouse)

        ingest_task >> lakehouse_task
else:
    dag = None
