import re
from typing import Any, Dict, List

try:
    from .chunking import build_chunks
except ImportError:
    from rag_pipeline.chunking import build_chunks


def tokenize(text: str) -> List[str]:
    return re.findall(r"\w+", (text or "").lower())


def hybrid_search(query: str, products: List[Dict[str, Any]], top_k: int = 3) -> List[Dict[str, Any]]:
    query_tokens = set(tokenize(query))
    chunks = build_chunks(products)
    ranked: List[Dict[str, Any]] = []
    for chunk in chunks:
        document_tokens = set(tokenize(chunk["text"]))
        overlap = len(query_tokens & document_tokens)
        phrase_bonus = 2 if query.lower() in chunk["text"].lower() else 0
        score = overlap + phrase_bonus
        if score > 0:
            ranked.append({**chunk, "score": score})

    ranked.sort(key=lambda item: item["score"], reverse=True)
    return ranked[:top_k]
