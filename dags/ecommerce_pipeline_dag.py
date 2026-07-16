from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - allows the file to be imported in lightweight environments
    DAG = None
    PythonOperator = None


def run_ingestion():
    import subprocess
    subprocess.run(["python", "ingestion/kafka_producer.py"], check=True)


def run_lakehouse():
    """Runs the full bronze -> silver -> gold Delta pipeline (delta-rs based)."""
    import subprocess
    subprocess.run(["python", "lakehouse/delta_lakehouse.py"], check=True)


def run_quality_gate():
    import subprocess
    subprocess.run(["python", "-c", "from quality_gates.quality_checks import run_quality_checks; print(run_quality_checks([{'product_id': 'demo', 'name': 'Demo', 'price': 1.0, 'stock_quantity': 1, 'category': 'General'}]))"], check=True)


def run_rag_indexing():
    import subprocess
    subprocess.run(["python", "-c", "from rag_pipeline.vector_store import SimpleVectorStore; from rag_pipeline.chunking import build_chunks; store=SimpleVectorStore(); store.upsert_chunks(build_chunks([{'product_id': 'demo', 'name': 'Demo', 'description': 'demo product', 'category': 'General', 'price': 1.0, 'stock_quantity': 1}]))"], check=True)


if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="ecommerce_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule_interval=timedelta(hours=1),
        catchup=False,
        tags=["ecommerce", "capstone"],
    ) as dag:
        ingest_task = PythonOperator(task_id="ingest_products", python_callable=run_ingestion)
        quality_gate_task = PythonOperator(task_id="quality_gate", python_callable=run_quality_gate)
        lakehouse_task = PythonOperator(task_id="run_lakehouse", python_callable=run_lakehouse)
        rag_index_task = PythonOperator(task_id="update_vector_index", python_callable=run_rag_indexing)

        ingest_task >> quality_gate_task >> lakehouse_task >> rag_index_task
else:
    dag = None