from typing import Any, Dict

PII_FIELDS = {"customer_name", "email", "phone", "name"}


def mask_pii(record: Dict[str, Any]) -> Dict[str, Any]:
    masked = dict(record)
    for field in PII_FIELDS:
        if field in masked:
            masked[field] = "[REDACTED]"
    return masked
