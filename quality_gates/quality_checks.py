from typing import Any, Dict, List

REQUIRED_FIELDS = ["product_id", "name", "price", "stock_quantity", "category"]


def validate_record(record: Dict[str, Any], index: int) -> List[str]:
    issues: List[str] = []
    for field in REQUIRED_FIELDS:
        if field not in record:
            issues.append(f"record[{index}] missing '{field}'")

    if "price" in record and isinstance(record["price"], (int, float)) and record["price"] <= 0:
        issues.append(f"record[{index}] price must be positive")

    if "stock_quantity" in record and isinstance(record["stock_quantity"], int) and record["stock_quantity"] < 0:
        issues.append(f"record[{index}] stock_quantity cannot be negative")

    if "name" in record and not str(record["name"]).strip():
        issues.append(f"record[{index}] name cannot be empty")

    return issues


def run_quality_checks(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    issues: List[str] = []
    for index, record in enumerate(records):
        issues.extend(validate_record(record, index))

    return {
        "passed": len(issues) == 0,
        "record_count": len(records),
        "issues": issues,
    }
