from typing import Any, Dict


def emit_lineage(event_name: str, details: Dict[str, Any] | None = None) -> None:
    payload = {"event": event_name, "details": details or {}}
    print(f"[OpenLineage] {payload}")
