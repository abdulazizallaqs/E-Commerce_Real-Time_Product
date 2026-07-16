"""
Schema definitions for the Delta Lakehouse (bronze / silver / gold).

These pyarrow schemas are the enforcement mechanism: any record written
to a layer must conform to its schema, or the write is rejected. This
gives us the "schema enforcement" requirement of Deliverable 2.

Product CDC record shape (as produced by the Debezium/Kafka ingestion
layer):
    product_id     -> primary key (exact match for the RAG hybrid search)
    name            -> product title
    description     -> product description (source text for chunking/RAG)
    category
    price
    stock_qty
    sentiment_score  -> filled in by the quality gate (review/ticket sentiment)
    is_spam          -> filled in by the quality gate
    op               -> CDC operation: "c" create, "u" update, "d" delete
    source_ts_ms     -> Debezium event timestamp (ms since epoch)
    ingested_at      -> when the record hit bronze (ISO8601 string)
"""

import pyarrow as pa

BRONZE_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("stock_qty", pa.int64(), nullable=True),
        pa.field("op", pa.string(), nullable=False),
        pa.field("source_ts_ms", pa.int64(), nullable=False),
        pa.field("ingested_at", pa.string(), nullable=False),
        pa.field("_raw_batch_id", pa.string(), nullable=True),
    ]
)

# Silver adds the quality-gate outputs and drops nothing — this is the
# "cleansed and conformed" layer: deduplicated on product_id, bad
# records quarantined before they arrive here.
SILVER_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("stock_qty", pa.int64(), nullable=False),
        pa.field("sentiment_score", pa.float64(), nullable=True),
        pa.field("is_spam", pa.bool_(), nullable=True),
        pa.field("is_deleted", pa.bool_(), nullable=False),
        pa.field("updated_at", pa.string(), nullable=False),
    ]
)

# Gold is business-ready: aggregated / derived fields that RAG and BI
# consume directly. No raw CDC noise, no deleted rows.
GOLD_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("price_tier", pa.string(), nullable=False),
        pa.field("stock_qty", pa.int64(), nullable=False),
        pa.field("is_in_stock", pa.bool_(), nullable=False),
        pa.field("avg_sentiment", pa.float64(), nullable=True),
        pa.field("updated_at", pa.string(), nullable=False),
    ]
)

# Records that fail bronze schema enforcement land here instead of
# crashing the pipeline — feeds the quality-gate / OpenLineage story.
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("raw_payload", pa.string(), nullable=False),
        pa.field("error", pa.string(), nullable=False),
        pa.field("quarantined_at", pa.string(), nullable=False),
    ]
)
