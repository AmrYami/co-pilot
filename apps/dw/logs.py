import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict

logger = logging.getLogger("dw")
_LOG_SQL = os.getenv("DW_LOG_SQL", "1") == "1"


def new_ctx(route: str, inquiry_id: Any | None = None) -> Dict[str, Any]:
    return {"trace_id": str(uuid.uuid4()), "route": route, "inquiry_id": inquiry_id}


def _scrub(val: Any) -> Any:
    if isinstance(val, str):
        if "@" in val:
            user, _, domain = val.partition("@")
            return (user[:2] + "***@" + domain) if user else "***@" + domain
        if len(val) > 64:
            return val[:61] + "..."
    return val


def scrub_binds(binds: Dict[str, Any] | None) -> Dict[str, Any]:
    try:
        return {key: _scrub(value) for key, value in (binds or {}).items()}
    except Exception:
        return {"_error": "failed_to_scrub"}


def jlog(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, default=str, ensure_ascii=False))


@contextmanager
def timed(event: str, **fields: Any):
    start = time.perf_counter()
    try:
        yield
    finally:
        duration_ms = int((time.perf_counter() - start) * 1000)
        jlog(event, duration_ms=duration_ms, **fields)
