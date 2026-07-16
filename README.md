# E-Commerce Real-Time Product Q&A — Capstone Project

**Author:** abdulazizallaqs (solo capstone project)
**Track:** Modern Data Architectures + Real-Time Pipelines + Vector Databases + Advanced RAG + Data Quality + Governance

A single-pipeline implementation that ingests e-commerce product updates, validates and lands them in a lakehouse-style storage layer, and exposes a retrieval workflow for answering product questions.

> **Honesty note:** This README describes what is actually implemented today, and separately what is scaffolded/simulated for local demo purposes. The goal is an accurate scope statement rather than an inflated one — see [Current Scope vs. Rubric](#current-scope-vs-rubric) below.

---

## Architecture Overview

```
Kafka (producer/consumer) → schema validation → bronze (raw) → silver (cleaned/deduped) → gold (curated)
                                                                                                │
                                                                                                ▼
                                                                                 Retrieval layer (chunking + hybrid search)
                                                                                                │
                                                                                                ▼
                                                                                        Q&A over products
```

Each bronze/silver/gold write emits an **OpenLineage** run event (see [Lineage](#lineage--data-quality)). Orchestrated (partially — see below) via an Airflow DAG scaffold.

## Repository Structure

| Path | Purpose |
|---|---|
| `ingestion/` | Kafka producer/consumer + JSON schema validation for incoming product events |
| `lakehouse/` | Local bronze/silver/gold pipeline entrypoints (JSONL-based, Delta-compatible path pending) |
| `rag_pipeline/` | Text chunking + hybrid search utilities for product Q&A retrieval |
| `quality_gates/` | Record-level validation checks, PII masking, and OpenLineage event emission |
| `dags/` | Airflow DAG scaffold |
| `tests/` | Unit tests |
| `docker-compose.yml` | Optional local Kafka stack |

## Requirements

Python 3.10+

```bash
pip install -r requirements.txt
```

## Quick Start

```bash
pip install -r requirements.txt
python run_demo.py        # or run_demo.bat on Windows
```

Generated artifacts:
- `lakehouse/data/bronze/bronze_records.jsonl`
- `lakehouse/data/silver/silver_records.jsonl`
- `lakehouse/data/gold/gold_records.jsonl`
- `lakehouse/data/lineage.jsonl` (or a live Marquez run, if a lineage backend is configured — see below)

### Optional: full Kafka path

```bash
docker compose up -d
```
Then run the producer/consumer scripts under `ingestion/`.

---

## Lineage & Data Quality

- **PII masking** (`quality_gates/pii.py`): emails and phone numbers in free-text fields (reviews, support tickets) are masked before being written to bronze/silver.
- **Silver zone** (`lakehouse/silver.py`): deduplicates on `product_id` and drops records missing required fields before promotion to gold.
- **Lineage** (`quality_gates/lineage.py`): each pipeline step (`ingest_bronze`, `clean_silver`, `curate_gold`) emits a real `RunEvent` via the `openlineage-client` library. If a lineage backend (e.g. Marquez) is reachable at the configured URL, events land there; otherwise the emitter falls back to a console log so the pipeline never breaks in environments without a backend running.

```python
from quality_gates.lineage import emit_lineage

emit_lineage("bronze_write", job_name="ingest_bronze")
emit_lineage("silver_write", job_name="clean_silver")
emit_lineage("gold_write", job_name="curate_gold")
```

Great Expectations coverage is not yet implemented — see [Roadmap](#roadmap--next-steps).

---

## Current Scope vs. Rubric

This project targets a 5-deliverable rubric (Ingestion, Delta Lakehouse, RAG pipeline, Orchestration, Quality gate). Current status per deliverable:

| Deliverable | Rubric ask | Implemented today | Status |
|---|---|---|---|
| Ingestion | Kafka producer/consumer + schema validation | Kafka producer/consumer + JSON schema validation in `ingestion/` | ✅ Done |
| Delta Lakehouse | bronze/silver/gold zones with MERGE + schema enforcement | bronze + silver + gold zones, written as local JSONL (silver adds dedupe/quality filtering; no Delta Lake MERGE yet) | 🟡 Partial — Delta/Spark path was environment-sensitive on Windows, so a deterministic JSONL fallback was used; silver zone now closes part of the gap |
| RAG pipeline | Chunking, embedding, vector index, hybrid search, reranking | Chunking + hybrid (keyword) search in `rag_pipeline/` | 🟡 Partial — no embedding model or vector database (e.g. Qdrant) wired in yet, no reranking step |
| Orchestration | Airflow DAG connecting all modules end-to-end | DAG scaffold in `dags/` | 🟡 Partial — not yet fully wired to call every module in sequence |
| Quality gate | Great Expectations suite + OpenLineage events | Basic record validation checks + PII masking in `quality_gates/`; real OpenLineage `RunEvent` emission (with console fallback) per pipeline step | 🟡 Partial — OpenLineage integrated via `openlineage-client`; Great Expectations suite still pending |
| Governance (PII) | Customer PII protection | Email/phone masking implemented in `quality_gates/pii.py`, applied before bronze/silver writes | ✅ Done (basic) |

### Why this scope

The current implementation prioritizes something that runs reliably end-to-end on a single machine (including Windows, without Spark/Delta cluster dependencies) over deeper integration with every named tool. This makes it easy to demo and grade locally, at the cost of not yet exercising the full advanced stack (Delta MERGE semantics, a real vector DB, reranking, Great Expectations).

## Roadmap / Next Steps

1. Swap the JSONL bronze/silver/gold writers for the `deltalake` Python package (works without Spark, so it's Windows-friendly) with real `MERGE` upserts.
2. Add sentence-embedding generation and a **Qdrant** instance for true vector search; add a reranking step on top of the current hybrid search.
3. Add a small **Great Expectations** suite (schema, null checks, value ranges) alongside the existing OpenLineage events.
4. Wire the Airflow DAG to call ingestion → lakehouse → quality gate → RAG index refresh as a real end-to-end chain.
5. Populate OpenLineage `inputs`/`outputs` with actual dataset facets (currently emitted empty) for full lineage graphs in Marquez.

## Notes

- The pipeline is intentionally robust for local execution and fallback mode, to make it easy to run and review in a classroom/capstone setting.
- Delta and Kafka can be environment-sensitive on Windows; this repo includes a deterministic local artifact path that works even when those services are unavailable.
- The Airflow DAG is a scaffold, expandable for production-style orchestration.
- To point lineage events at a real backend, set the Marquez URL in `quality_gates/lineage.py` (`_LINEAGE_URL`) or via environment variable and run `pip install openlineage-client`.

## License

MIT
