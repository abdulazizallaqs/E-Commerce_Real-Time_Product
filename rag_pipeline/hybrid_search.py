from typing import Any, Dict, List

try:
    from .chunking import build_chunks
    from .vector_store import SimpleVectorStore
    from .reranker import rerank
except ImportError:
    from rag_pipeline.chunking import build_chunks
    from rag_pipeline.vector_store import SimpleVectorStore
    from rag_pipeline.reranker import rerank


def hybrid_search(query: str, products: List[Dict[str, Any]], top_k: int = 3) -> List[Dict[str, Any]]:
    chunks = build_chunks(products)

    # Layer 1: exact product-code match — highest priority, short-circuits the rest
    exact_matches = [c for c in chunks if str(c.get("source_id", "")).lower() == query.strip().lower()]
    if exact_matches:
        return [{**c, "score": 1.0, "match_type": "exact"} for c in exact_matches[:top_k]]

    # Layer 2: semantic (embedding-based) search over the chunk index
    store = SimpleVectorStore()
    store.upsert_chunks(chunks)
    candidates = store.semantic_search(query, top_k=max(top_k * 4, 10))
    for c in candidates:
        c["match_type"] = "semantic"

    if not candidates:
        return []

    # Layer 3: cross-encoder reranking over the semantic shortlist
    reranked = rerank(query, candidates, top_k=top_k)
    return reranked