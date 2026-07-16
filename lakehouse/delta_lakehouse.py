"""
Delta Lakehouse core (Deliverable 2 — 25 pts).

Implements a real bronze / silver / gold Medallion architecture on top
of Delta Lake, using the `deltalake` package (delta-rs Python bindings).

Why delta-rs instead of PySpark + Delta:
    - No JVM / Spark cluster needed -> runs reliably on Windows and in
      classroom environments (this was already a stated pain point in
      the repo's notes).
    - Still gives real ACID transactions, ACTUAL `MERGE INTO`, and
      schema enforcement/evolution — the actual Delta Lake protocol,
      not a JSONL fallback.

Layers:
    bronze  -> append-only raw CDC events, schema-enforced on write
    silver  -> deduplicated + cleaned, upserted via MERGE on product_id
    gold    -> business-ready aggregates, rebuilt via MERGE from silver

Run this file directly for a self-contained demo.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from schemas import (
    BRONZE_SCHEMA,
    GOLD_SCHEMA,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMA,
)

LAKE_ROOT = os.path.join(os.path.dirname(__file__), "data")
BRONZE_PATH = os.path.join(LAKE_ROOT, "bronze")
SILVER_PATH = os.path.join(LAKE_ROOT, "silver")
GOLD_PATH = os.path.join(LAKE_ROOT, "gold")
QUARANTINE_PATH = os.path.join(LAKE_ROOT, "quarantine")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(path: str) -> bool:
    try:
        DeltaTable(path)
        return True
    except TableNotFoundError:
        return False


# --------------------------------------------------------------------------
# BRONZE — append-only, schema-enforced raw ingestion
# --------------------------------------------------------------------------
def write_bronze(records: Iterable[dict], batch_id: str | None = None) -> dict:
    """
    Append raw CDC records to the bronze table.

    Schema enforcement: each record is cast against BRONZE_SCHEMA. Any
    record that doesn't fit (missing required field, wrong type) is
    routed to the quarantine table instead of breaking the run.
    """
    required_fields = [f.name for f in BRONZE_SCHEMA if not f.nullable]

    good, bad = [], []
    for rec in records:
        try:
            rec = dict(rec)
            rec.setdefault("ingested_at", _now_iso())
            rec.setdefault("_raw_batch_id", batch_id)

            missing = [f for f in required_fields if rec.get(f) is None]
            if missing:
                raise ValueError(f"missing required field(s): {missing}")

            pa.Table.from_pylist([rec], schema=BRONZE_SCHEMA)
            good.append(rec)
        except Exception as exc:  # noqa: BLE001 - deliberately broad: quarantine anything invalid
            bad.append(
                {
                    "raw_payload": json.dumps(rec, default=str),
                    "error": str(exc),
                    "quarantined_at": _now_iso(),
                }
            )

    if good:
        table = pa.Table.from_pylist(good, schema=BRONZE_SCHEMA)
        write_deltalake(
            BRONZE_PATH,
            table,
            mode="append",
            schema_mode="merge",  # allows controlled schema evolution over time
        )

    if bad:
        qtable = pa.Table.from_pylist(bad, schema=QUARANTINE_SCHEMA)
        write_deltalake(QUARANTINE_PATH, qtable, mode="append")

    return {"written": len(good), "quarantined": len(bad)}

# --------------------------------------------------------------------------
# SILVER — MERGE INTO: dedupe + clean + apply CDC semantics
# --------------------------------------------------------------------------
def upsert_silver(quality_gate_fn=None) -> dict:
    """
    Read new bronze rows, clean them, and MERGE INTO silver on product_id.

    quality_gate_fn: optional callable(pandas.DataFrame) -> pandas.DataFrame
        Hook for Deliverable 5 (Great Expectations) to attach
        sentiment_score / is_spam columns before the merge. If not
        provided, those columns default to null/False.
    """
    bronze_df = DeltaTable(BRONZE_PATH).to_pandas()
    if bronze_df.empty:
        return {"merged": 0}

    # Keep only the latest event per product_id (CDC "last write wins").
    bronze_df = bronze_df.sort_values("source_ts_ms").drop_duplicates(
        "product_id", keep="last"
    )

    bronze_df.loc[:, "is_deleted"] = bronze_df["op"] == "d"
    bronze_df["updated_at"] = bronze_df["ingested_at"]

    if quality_gate_fn is not None:
        bronze_df = quality_gate_fn(bronze_df)
    else:
        bronze_df["sentiment_score"] = None
        bronze_df["is_spam"] = False

    # Drop spam before it ever reaches silver.
    bronze_df = bronze_df[bronze_df["is_spam"] != True]  # noqa: E712

    silver_cols = [f.name for f in SILVER_SCHEMA]
    source_df = bronze_df[[c for c in silver_cols if c in bronze_df.columns]].copy()
    for col in silver_cols:
        if col not in source_df.columns:
            source_df[col] = None

    if not _table_exists(SILVER_PATH):
        write_deltalake(SILVER_PATH, pa.Table.from_pandas(source_df, schema=SILVER_SCHEMA))
        return {"merged": len(source_df), "mode": "initial_load"}

    target = DeltaTable(SILVER_PATH)
    (
        target.merge(
            source=source_df,
            predicate="target.product_id = source.product_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    return {"merged": len(source_df), "mode": "merge"}


# --------------------------------------------------------------------------
# GOLD — business-ready aggregates, rebuilt via MERGE from silver
# --------------------------------------------------------------------------
def _price_tier(price: float) -> str:
    if price is None:
        return "unknown"
    if price < 25:
        return "budget"
    if price < 100:
        return "mid"
    return "premium"


def build_gold() -> dict:
    """
    Derive the gold layer from silver: drop deleted products, compute
    price_tier / is_in_stock, and MERGE the result so gold always
    reflects the latest silver state without a full table rewrite.
    """
    silver_df = DeltaTable(SILVER_PATH).to_pandas()
    silver_df = silver_df[silver_df["is_deleted"] != True]  # noqa: E712
    if silver_df.empty:
        return {"merged": 0}

    silver_df["price_tier"] = silver_df["price"].apply(_price_tier)
    silver_df["is_in_stock"] = silver_df["stock_qty"] > 0
    silver_df["avg_sentiment"] = silver_df["sentiment_score"]

    gold_cols = [f.name for f in GOLD_SCHEMA]
    source_df = silver_df[[c for c in gold_cols if c in silver_df.columns]].copy()
    for col in gold_cols:
        if col not in source_df.columns:
            source_df[col] = None

    if not _table_exists(GOLD_PATH):
        write_deltalake(GOLD_PATH, pa.Table.from_pandas(source_df, schema=GOLD_SCHEMA))
        return {"merged": len(source_df), "mode": "initial_load"}

    target = DeltaTable(GOLD_PATH)
    (
        target.merge(
            source=source_df,
            predicate="target.product_id = source.product_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    return {"merged": len(source_df), "mode": "merge"}


def run_pipeline(records: Iterable[dict], batch_id: str | None = None) -> dict:
    """Convenience entrypoint used by the Airflow DAG / demo script."""
    b = write_bronze(records, batch_id=batch_id)
    s = upsert_silver()
    g = build_gold()
    return {"bronze": b, "silver": s, "gold": g}


if __name__ == "__main__":
    # --- Self-contained demo -------------------------------------------------
    batch_1 = [
        {
            "product_id": "P-1001",
            "name": "Wireless Mouse",
            "description": "Ergonomic wireless mouse with USB-C charging.",
            "category": "Electronics",
            "price": 19.99,
            "stock_qty": 120,
            "op": "c",
            "source_ts_ms": 1,
        },
        {
            "product_id": "P-1002",
            "name": "Standing Desk",
            "description": "Electric height-adjustable standing desk.",
            "category": "Furniture",
            "price": 349.0,
            "stock_qty": 8,
            "op": "c",
            "source_ts_ms": 1,
        },
        {
            # Missing required 'op' on purpose -> should be quarantined
            "product_id": "P-BAD",
            "name": "Broken Record",
            "source_ts_ms": 1,
        },
    ]
    print("Batch 1:", run_pipeline(batch_1, batch_id="batch-1"))

    # CDC update: price change + stock depletion for P-1001, arrives later
    batch_2 = [
        {
            "product_id": "P-1001",
            "name": "Wireless Mouse",
            "description": "Ergonomic wireless mouse with USB-C charging.",
            "category": "Electronics",
            "price": 15.99,  # price drop
            "stock_qty": 0,  # now out of stock
            "op": "u",
            "source_ts_ms": 2,
        },
    ]
    print("Batch 2 (MERGE/update):", run_pipeline(batch_2, batch_id="batch-2"))

    print("\nFinal GOLD table:")
    print(DeltaTable(GOLD_PATH).to_pandas())

    print("\nQuarantine table:")
    if _table_exists(QUARANTINE_PATH):
        print(DeltaTable(QUARANTINE_PATH).to_pandas())

    print("\nSilver table history (Delta time travel):")
    for entry in DeltaTable(SILVER_PATH).history():
        print(entry)

























# """
# Delta Lakehouse core (Deliverable 2 — 25 pts).

# Implements a real bronze / silver / gold Medallion architecture on top
# of Delta Lake, using the `deltalake` package (delta-rs Python bindings).

# Why delta-rs instead of PySpark + Delta:
#     - No JVM / Spark cluster needed -> runs reliably on Windows and in
#       classroom environments (this was already a stated pain point in
#       the repo's notes).
#     - Still gives real ACID transactions, ACTUAL `MERGE INTO`, and
#       schema enforcement/evolution — the actual Delta Lake protocol,
#       not a JSONL fallback.

# Layers:
#     bronze  -> append-only raw CDC events, schema-enforced on write
#     silver  -> deduplicated + cleaned, upserted via MERGE on product_id
#     gold    -> business-ready aggregates, rebuilt via MERGE from silver

# Run this file directly for a self-contained demo.
# """

# from __future__ import annotations

# import json
# import os
# from datetime import datetime, timezone
# from typing import Iterable

# import pandas as pd
# import pyarrow as pa
# from deltalake import DeltaTable, write_deltalake
# from deltalake.exceptions import TableNotFoundError

# from schemas import (
#     BRONZE_SCHEMA,
#     GOLD_SCHEMA,
#     QUARANTINE_SCHEMA,
#     SILVER_SCHEMA,
# )

# LAKE_ROOT = os.path.join(os.path.dirname(__file__), "data")
# BRONZE_PATH = os.path.join(LAKE_ROOT, "bronze")
# SILVER_PATH = os.path.join(LAKE_ROOT, "silver")
# GOLD_PATH = os.path.join(LAKE_ROOT, "gold")
# QUARANTINE_PATH = os.path.join(LAKE_ROOT, "quarantine")


# def _now_iso() -> str:
#     return datetime.now(timezone.utc).isoformat()


# def _table_exists(path: str) -> bool:
#     try:
#         DeltaTable(path)
#         return True
#     except TableNotFoundError:
#         return False


# # --------------------------------------------------------------------------
# # BRONZE — append-only, schema-enforced raw ingestion
# # --------------------------------------------------------------------------
# def write_bronze(records: Iterable[dict], batch_id: str | None = None) -> dict:
#     """
#     Append raw CDC records to the bronze table.

#     Schema enforcement: each record is cast against BRONZE_SCHEMA. Any
#     record that doesn't fit (missing required field, wrong type) is
#     routed to the quarantine table instead of breaking the run.
#     """
#     required_fields = [f.name for f in BRONZE_SCHEMA if not f.nullable]

#     good, bad = [], []
#     for rec in records:
#         try:
#             rec = dict(rec)
#             rec.setdefault("ingested_at", _now_iso())
#             rec.setdefault("_raw_batch_id", batch_id)

#             missing = [f for f in required_fields if rec.get(f) is None]
#             if missing:
#                 raise ValueError(f"missing required field(s): {missing}")

#             pa.Table.from_pylist([rec], schema=BRONZE_SCHEMA)
#             good.append(rec)
#         except Exception as exc:  # noqa: BLE001 - deliberately broad: quarantine anything invalid
#             bad.append(
#                 {
#                     "raw_payload": json.dumps(rec, default=str),
#                     "error": str(exc),
#                     "quarantined_at": _now_iso(),
#                 }
#             )

#     if good:
#         table = pa.Table.from_pylist(good, schema=BRONZE_SCHEMA)
#         write_deltalake(
#             BRONZE_PATH,
#             table,
#             mode="append",
#             schema_mode="merge",  # allows controlled schema evolution over time
#         )

#     if bad:
#         qtable = pa.Table.from_pylist(bad, schema=QUARANTINE_SCHEMA)
#         write_deltalake(QUARANTINE_PATH, qtable, mode="append")

#     return {"written": len(good), "quarantined": len(bad)}

# # --------------------------------------------------------------------------
# # SILVER — MERGE INTO: dedupe + clean + apply CDC semantics
# # --------------------------------------------------------------------------
# def upsert_silver(quality_gate_fn=None) -> dict:
#     """
#     Read new bronze rows, clean them, and MERGE INTO silver on product_id.

#     quality_gate_fn: optional callable(pandas.DataFrame) -> pandas.DataFrame
#         Hook for Deliverable 5 (Great Expectations) to attach
#         sentiment_score / is_spam columns before the merge. If not
#         provided, those columns default to null/False.
#     """
#     bronze_df = DeltaTable(BRONZE_PATH).to_pandas()
#     if bronze_df.empty:
#         return {"merged": 0}

#     # Keep only the latest event per product_id (CDC "last write wins").
#     bronze_df = bronze_df.sort_values("source_ts_ms").drop_duplicates(
#         "product_id", keep="last"
#     )

#     bronze_df.loc[:, "is_deleted"] = bronze_df["op"] == "d"
#     bronze_df["updated_at"] = bronze_df["ingested_at"]

#     if quality_gate_fn is not None:
#         bronze_df = quality_gate_fn(bronze_df)
#     else:
#         bronze_df["sentiment_score"] = None
#         bronze_df["is_spam"] = False

#     # Drop spam before it ever reaches silver.
#     bronze_df = bronze_df[bronze_df["is_spam"] != True]  # noqa: E712

#     silver_cols = [f.name for f in SILVER_SCHEMA]
#     source_df = bronze_df[[c for c in silver_cols if c in bronze_df.columns]].copy()
#     for col in silver_cols:
#         if col not in source_df.columns:
#             source_df[col] = None

#     if not _table_exists(SILVER_PATH):
#         write_deltalake(SILVER_PATH, pa.Table.from_pandas(source_df, schema=SILVER_SCHEMA))
#         return {"merged": len(source_df), "mode": "initial_load"}

#     target = DeltaTable(SILVER_PATH)
#     (
#         target.merge(
#             source=source_df,
#             predicate="target.product_id = source.product_id",
#             source_alias="source",
#             target_alias="target",
#         )
#         .when_matched_update_all()
#         .when_not_matched_insert_all()
#         .execute()
#     )
#     return {"merged": len(source_df), "mode": "merge"}


# # --------------------------------------------------------------------------
# # GOLD — business-ready aggregates, rebuilt via MERGE from silver
# # --------------------------------------------------------------------------
# def _price_tier(price: float) -> str:
#     if price is None:
#         return "unknown"
#     if price < 25:
#         return "budget"
#     if price < 100:
#         return "mid"
#     return "premium"


# def build_gold() -> dict:
#     """
#     Derive the gold layer from silver: drop deleted products, compute
#     price_tier / is_in_stock, and MERGE the result so gold always
#     reflects the latest silver state without a full table rewrite.
#     """
#     silver_df = DeltaTable(SILVER_PATH).to_pandas()
#     silver_df = silver_df[silver_df["is_deleted"] != True]  # noqa: E712
#     if silver_df.empty:
#         return {"merged": 0}

#     silver_df["price_tier"] = silver_df["price"].apply(_price_tier)
#     silver_df["is_in_stock"] = silver_df["stock_qty"] > 0
#     silver_df["avg_sentiment"] = silver_df["sentiment_score"]

#     gold_cols = [f.name for f in GOLD_SCHEMA]
#     source_df = silver_df[[c for c in gold_cols if c in silver_df.columns]].copy()
#     for col in gold_cols:
#         if col not in source_df.columns:
#             source_df[col] = None

#     if not _table_exists(GOLD_PATH):
#         write_deltalake(GOLD_PATH, pa.Table.from_pandas(source_df, schema=GOLD_SCHEMA))
#         return {"merged": len(source_df), "mode": "initial_load"}

#     target = DeltaTable(GOLD_PATH)
#     (
#         target.merge(
#             source=source_df,
#             predicate="target.product_id = source.product_id",
#             source_alias="source",
#             target_alias="target",
#         )
#         .when_matched_update_all()
#         .when_not_matched_insert_all()
#         .execute()
#     )
#     return {"merged": len(source_df), "mode": "merge"}


# def run_pipeline(records: Iterable[dict], batch_id: str | None = None) -> dict:
#     """Convenience entrypoint used by the Airflow DAG / demo script."""
#     b = write_bronze(records, batch_id=batch_id)
#     s = upsert_silver()
#     g = build_gold()
#     return {"bronze": b, "silver": s, "gold": g}


# if __name__ == "__main__":
#     # --- Self-contained demo -------------------------------------------------
#     batch_1 = [
#         {
#             "product_id": "P-1001",
#             "name": "Wireless Mouse",
#             "description": "Ergonomic wireless mouse with USB-C charging.",
#             "category": "Electronics",
#             "price": 19.99,
#             "stock_qty": 120,
#             "op": "c",
#             "source_ts_ms": 1,
#         },
#         {
#             "product_id": "P-1002",
#             "name": "Standing Desk",
#             "description": "Electric height-adjustable standing desk.",
#             "category": "Furniture",
#             "price": 349.0,
#             "stock_qty": 8,
#             "op": "c",
#             "source_ts_ms": 1,
#         },
#         {
#             # Missing required 'op' on purpose -> should be quarantined
#             "product_id": "P-BAD",
#             "name": "Broken Record",
#             "source_ts_ms": 1,
#         },
#     ]
#     print("Batch 1:", run_pipeline(batch_1, batch_id="batch-1"))

#     # CDC update: price change + stock depletion for P-1001, arrives later
#     batch_2 = [
#         {
#             "product_id": "P-1001",
#             "name": "Wireless Mouse",
#             "description": "Ergonomic wireless mouse with USB-C charging.",
#             "category": "Electronics",
#             "price": 15.99,  # price drop
#             "stock_qty": 0,  # now out of stock
#             "op": "u",
#             "source_ts_ms": 2,
#         },
#     ]
#     print("Batch 2 (MERGE/update):", run_pipeline(batch_2, batch_id="batch-2"))

#     print("\nFinal GOLD table:")
#     print(DeltaTable(GOLD_PATH).to_pandas())

#     print("\nQuarantine table:")
#     if _table_exists(QUARANTINE_PATH):
#         print(DeltaTable(QUARANTINE_PATH).to_pandas())

#     print("\nSilver table history (Delta time travel):")
#     for entry in DeltaTable(SILVER_PATH).history():
#         print(entry)
