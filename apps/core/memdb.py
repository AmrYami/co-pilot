"""Helpers for working with the shared Postgres-backed memory database."""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)
_MEM_ENG: Engine | None = None


def _load_mem_settings() -> Dict[str, Any]:
    """Best-effort loader for memory database settings."""

    try:
        from apps.settings import get_setting  # type: ignore

        setting = get_setting("MEMORY_DB_URL", default="", scope="global", namespace="global")
        if setting:
            return {"MEMORY_DB_URL": str(setting)}
    except Exception:  # pragma: no cover - optional settings backend
        pass
    return {}


def _resolve_memory_url() -> str:
    """Resolve the memory database connection URL."""

    env_url = os.getenv("MEMORY_DB_URL") or os.getenv("MEM_DB_URL")
    if env_url:
        return env_url

    settings = _load_mem_settings()
    if settings.get("MEMORY_DB_URL"):
        return str(settings["MEMORY_DB_URL"])

    return "postgresql+psycopg2://postgres:123456789@localhost/copilot_mem_dev"


def _hide_pw(url: str) -> str:
    try:
        scheme, rest = url.split("://", 1)
        creds, tail = rest.split("@", 1)
        if ":" in creds:
            user = creds.split(":", 1)[0]
        else:
            user = creds
        return f"{scheme}://{user}:***@{tail}"
    except Exception:  # pragma: no cover - defensive mask
        return "masked"


def get_memory_engine(*, force_refresh: bool = False) -> Engine:
    """Return a singleton SQLAlchemy engine for the memory database."""

    global _MEM_ENG
    if _MEM_ENG is not None and not force_refresh:
        return _MEM_ENG

    url = _resolve_memory_url()
    echo_flag = str(os.getenv("MEM_SQL_ECHO", "false")).lower() in {"1", "true", "yes"}

    engine = create_engine(url, pool_pre_ping=True, future=True, echo=echo_flag)
    log.info("memdb.init", extra={"url": _hide_pw(url), "echo": echo_flag})

    _MEM_ENG = engine
    return engine


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def persist_feedback(engine: Engine, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Persist rate feedback metadata into the memory database."""

    if not payload.get("inquiry_id"):
        return {"ok": False, "error": "missing_inquiry_id"}

    intent_json = json.dumps(
        payload.get("intent") or {}, ensure_ascii=False, default=_json_default
    )
    binds_json = json.dumps(
        payload.get("binds") or {}, ensure_ascii=False, default=_json_default
    )

    sql = text(
        """
        INSERT INTO dw_feedback (
          inquiry_id, auth_email, rating, comment,
          intent_json, resolved_sql, binds_json, status
        )
        VALUES (
          :inquiry_id, :auth_email, :rating, :comment,
          CAST(:intent_json AS JSONB), :resolved_sql, CAST(:binds_json AS JSONB), 'pending'
        )
        ON CONFLICT (inquiry_id) DO UPDATE SET
          rating       = EXCLUDED.rating,
          comment      = EXCLUDED.comment,
          intent_json  = EXCLUDED.intent_json,
          resolved_sql = EXCLUDED.resolved_sql,
          binds_json   = EXCLUDED.binds_json,
          updated_at   = NOW()
        RETURNING id
        """
    )

    params = {
        "inquiry_id": payload["inquiry_id"],
        "auth_email": payload.get("auth_email", "") or "",
        "rating": int(payload.get("rating", 0)),
        "comment": payload.get("comment", "") or "",
        "intent_json": intent_json,
        "resolved_sql": payload.get("resolved_sql") or "",
        "binds_json": binds_json,
    }

    with engine.begin() as conn:
        row = conn.execute(sql, params).fetchone()

    feedback_id = int(row[0]) if row and row[0] is not None else None
    return {"ok": True, "msg": "ok", "feedback_id": feedback_id}


__all__ = ["get_memory_engine", "persist_feedback"]
