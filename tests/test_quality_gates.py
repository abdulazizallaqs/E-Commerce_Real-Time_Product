from quality_gates.quality_checks import run_quality_checks


def test_quality_checks_pass_for_valid_record():
    record = {
        "product_id": "p1",
        "name": "Wireless Mouse",
        "price": 39.99,
        "stock_quantity": 12,
        "category": "Electronics",
    }
    result = run_quality_checks([record])
    assert result["passed"] is True
    assert result["record_count"] == 1
    assert result["issues"] == []


def test_quality_checks_fail_for_invalid_record():
    record = {
        "product_id": "p2",
        "name": "",
        "price": -1,
        "stock_quantity": -2,
        "category": "Electronics",
    }
    result = run_quality_checks([record])
    assert result["passed"] is False
    assert len(result["issues"]) >= 3
