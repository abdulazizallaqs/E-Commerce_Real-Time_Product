import json
import numpy as np
from pathlib import Path
from typing import Any, Dict, List

from sentence_transformers import SentenceTransformer

_MODEL = None


def get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    return _MODEL


class SimpleVectorStore:
    """A local JSON-backed vector store using real sentence embeddings
    and cosine similarity for semantic search."""

    def __init__(self, path: str = "lakehouse/data/vector_index.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def upsert_chunks(self, chunks: List[Dict[str, Any]]) -> None:
        if not chunks:
            self.path.write_text(json.dumps([]), encoding="utf-8")
            return

        model = get_model()
        texts = [c["text"] for c in chunks]
        embeddings = model.encode(texts, normalize_embeddings=True)

        payload = []
        for chunk, vector in zip(chunks, embeddings):
            payload.append({
                "id": chunk.get("id"),
                "source_id": chunk.get("source_id"),
                "text": chunk.get("text"),
                "metadata": chunk.get("metadata", {}),
                "vector": vector.tolist(),
            })
        self.path.write_text(json.dumps(payload), encoding="utf-8")

    def semantic_search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []

        records = json.loads(self.path.read_text(encoding="utf-8"))
        if not records:
            return []

        model = get_model()
        query_vec = model.encode([query], normalize_embeddings=True)[0]

        scored = []
        for record in records:
            doc_vec = np.array(record["vector"])
            similarity = float(np.dot(query_vec, doc_vec))
            scored.append({**record, "score": similarity})

        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:top_k]