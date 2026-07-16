from rag_pipeline.hybrid_search import hybrid_search


def test_hybrid_search_returns_results_for_matching_query():
    products = [
        {
            "product_id": "p1",
            "name": "Wireless Mouse",
            "description": "Ergonomic wireless mouse",
            "category": "Electronics",
            "price": 39.99,
            "stock_quantity": 12,
        }
    ]
    results = hybrid_search("wireless mouse", products, top_k=3)
    assert len(results) >= 1
    assert results[0]["source_id"] == "p1"
