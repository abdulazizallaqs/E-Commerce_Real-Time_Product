# Architecture Overview

```text
Kafka / Ingestion
    -> Bronze (local JSONL + Delta write attempt)
    -> Quality Gate
    -> Silver (deduped, cleaned Delta)
    -> Gold (upserted Delta table)
    -> Vector Index (JSON-backed local index)
    -> RAG / Hybrid Search
```

## Components
- Kafka ingestion: simulated product updates and validation
- Bronze: initial landing zone for raw records
- Silver: cleaned and de-duplicated representation
- Gold: curated analytics-ready layer
- RAG: keyword + exact-match + vector-style retrieval over product chunks
- Governance: PII masking before logs and search indexing

## PII handling
Sensitive user-facing fields such as customer_name, email, phone, and name are masked with `[REDACTED]` before they reach logs or search payloads.
