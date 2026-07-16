from typing import Any, Dict

# حقول خاصة بالعميل/الدعم فقط — لا تشمل حقول المنتج مثل "name"
PII_FIELDS = {"customer_name", "email", "phone", "customer_id", "address", "card_number"}


def mask_pii(record: Dict[str, Any]) -> Dict[str, Any]:
    """Redact known PII fields without touching product-level fields."""
    masked = dict(record)
    for field in PII_FIELDS:
        if field in masked:
            masked[field] = "[REDACTED]"
    return masked