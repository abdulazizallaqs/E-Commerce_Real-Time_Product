import json
from pathlib import Path
from typing import Any, Dict, List


class SimpleVectorStore:
    def __init__(self, path: str = "lakehouse/data/vector_index.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def upsert_chunks(self, chunks: List[Dict[str, Any]]) -> None:
        payload = []
        for chunk in chunks:
            payload.append({
                "id": chunk.get("id"),
                "source_id": chunk.get("source_id"),
                "text": chunk.get("text"),
                "metadata": chunk.get("metadata", {}),
            })
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def semantic_search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        records = json.loads(self.path.read_text(encoding="utf-8"))
        scored = []
        for record in records:
            text = str(record.get("text", ""))
            score = 0
            if query.lower() in text.lower():
                score += 2
            if query.lower() in str(record.get("source_id", "")).lower():
                score += 1000
            if score > 0:
                scored.append({**record, "score": score})
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:top_k]
