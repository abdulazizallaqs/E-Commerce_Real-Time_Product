# E-Commerce Real-Time Product Pipeline

This repository contains a lightweight end-to-end capstone project for ingesting e-commerce product updates, validating them, writing a lakehouse-style bronze/gold artifact locally, and exposing a simple retrieval workflow for product search.

## Project goals
- Simulate real-time product updates with Kafka-style ingestion
- Validate incoming payloads using JSON schema checks
- Write a local bronze artifact and a downstream gold artifact
- Provide a lightweight RAG-style retrieval layer for product search
- Offer a GitHub-friendly structure that can be reviewed and run locally

## Repository structure
- [ingestion](ingestion): Kafka producer/consumer and product schema validation
- [lakehouse](lakehouse): local bronze/gold pipeline entrypoints
- [rag_pipeline](rag_pipeline): text chunking and hybrid search utilities
- [quality_gates](quality_gates): simple record validation checks
- [dags](dags): Airflow DAG scaffold

## Requirements
Python 3.10+ is recommended.
Install dependencies with:

```bash
pip install -r requirements.txt
```

## Quick start
1. Create or activate the project virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the local demo:
   ```bash
   python run_demo.py
   ```
   or on Windows:
   ```bat
   run_demo.bat
   ```
4. Review generated artifacts:
   - [lakehouse/data/bronze/bronze_records.jsonl](lakehouse/data/bronze/bronze_records.jsonl)
   - [lakehouse/data/gold/gold_records.jsonl](lakehouse/data/gold/gold_records.jsonl)

## Optional Docker-based Kafka setup
If you want to test the Kafka path more fully, start the included services:

```bash
docker compose up -d
```

Then run the producer or consumer scripts under [ingestion](ingestion).

## Notes
- The current implementation is intentionally robust for local execution and fallback mode, which makes it easier to run and review in a classroom or capstone environment.
- Delta and Kafka can be environment-sensitive on Windows, so the repository includes a deterministic local artifact path that works even when those services are unavailable.
- The Airflow DAG is a scaffold and can be expanded for production orchestration later.
