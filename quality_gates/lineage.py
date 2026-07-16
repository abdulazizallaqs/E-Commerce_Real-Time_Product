from datetime import datetime, timezone
from typing import Any, Dict, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.run import Run, RunEvent, RunState, Job
from openlineage.client.uuid import generate_new_uuid

_LINEAGE_URL = "http://localhost:5000"  # عدّلها لعنوان Marquez أو أي backend عندك
_client = OpenLineageClient(url=_LINEAGE_URL)


def emit_lineage(event_name: str, job_name: str = "ecommerce_pipeline",
                  details: Optional[Dict[str, Any]] = None) -> None:
    """Emit a real OpenLineage RunEvent. Falls back to a console log if no
    lineage backend (e.g. Marquez) is reachable."""
    run_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="ecommerce", name=job_name),
        producer="https://github.com/abdulazizallaqs/E-Commerce_Real-Time_Product",
        inputs=[],
        outputs=[],
    )
    try:
        _client.emit(run_event)
        print(f"[OpenLineage] emitted event for job={job_name}: {event_name}")
    except Exception as exc:
        print(f"[OpenLineage] backend unreachable ({exc}); event={event_name} details={details or {}}")