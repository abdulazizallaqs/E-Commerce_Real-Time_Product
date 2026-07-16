import re
from typing import Any, Dict, List


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def chunk_text(text: str, chunk_size: int = 250, overlap: int = 50) -> List[str]:
    normalized = normalize_text(text)
    if not normalized:
        return []

    words = normalized.split()
    if len(words) <= chunk_size:
        return [normalized]

    chunks: List[str] = []
    start = 0
    while start < len(words):
        end = min(len(words), start + chunk_size)
        chunk = " ".join(words[start:end])
        if chunk:
            chunks.append(chunk)
        if end == len(words):
            break
        start = max(0, end - overlap)
    return chunks


def build_chunks(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    for index, product in enumerate(products):
        source_text = " ".join(
            [
                str(product.get("name", "")),
                str(product.get("description", "")),
                str(product.get("category", "")),
                str(product.get("price", "")),
                str(product.get("stock_quantity", "")),
            ]
        )
        for chunk_index, chunk in enumerate(chunk_text(source_text)):
            chunks.append(
                {
                    "id": f"{index}-{chunk_index}",
                    "source_id": product.get("product_id", f"doc-{index}"),
                    "text": chunk,
                    "metadata": {
                        "name": product.get("name", ""),
                        "category": product.get("category", ""),
                    },
                }
            )
    return chunks
