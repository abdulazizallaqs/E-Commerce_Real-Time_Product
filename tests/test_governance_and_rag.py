from quality_gates.quality_checks import run_quality_checks
from rag_pipeline.hybrid_search import hybrid_search
from quality_gates.governance import mask_pii


def test_mask_pii_masks_sensitive_fields():
    record = {
        "product_id": "p1",
        "name": "Wireless Mouse",
        "customer_name": "Alice",
        "email": "alice@example.com",
        "phone": "0500000000",
    }
    masked = mask_pii(record)
    assert masked["customer_name"] == "[REDACTED]"
    assert masked["email"] == "[REDACTED]"
    assert masked["phone"] == "[REDACTED]"


def test_hybrid_search_prefers_exact_product_id_matches():
    products = [
        {"product_id": "x100", "name": "Keyboard", "description": "Mechanical keyboard", "category": "Electronics", "price": 199.0, "stock_quantity": 10},
        {"product_id": "p1", "name": "Wireless Mouse", "description": "Ergonomic wireless mouse", "category": "Electronics", "price": 39.99, "stock_quantity": 12},
    ]
    results = hybrid_search("p1", products, top_k=3)
    assert results[0]["source_id"] == "p1"
    assert results[0]["score"] >= 100
