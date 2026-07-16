from typing import Any, Dict, List

from sentence_transformers import CrossEncoder

_RERANKER = None


def get_reranker() -> CrossEncoder:
    global _RERANKER
    if _RERANKER is None:
        _RERANKER = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
    return _RERANKER


def rerank(query: str, candidates: List[Dict[str, Any]], top_k: int = 3) -> List[Dict[str, Any]]:
    """Re-score a candidate list with a cross-encoder for higher-precision ranking."""
    if not candidates:
        return []

    reranker = get_reranker()
    pairs = [(query, c.get("text", "")) for c in candidates]
    scores = reranker.predict(pairs)

    for candidate, score in zip(candidates, scores):
        candidate["rerank_score"] = float(score)

    candidates.sort(key=lambda c: c["rerank_score"], reverse=True)
    return candidates[:top_k]