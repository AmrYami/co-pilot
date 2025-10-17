# -*- coding: utf-8 -*-
"""Lightweight feedback store for /dw/rate overrides.

This module bootstraps a portable schema for `dw_feedback` (and `dw_patches`)
and persists feedback rows with an UPSERT keyed by (inquiry_id).

PostgreSQL:
  - JSON fields use JSONB
  - timestamps use NOW()/TIMESTAMPTZ
  - we also create a trigger to maintain updated_at

SQLite (fallback):
  - JSON stored as TEXT
  - timestamps rely on CURRENT_TIMESTAMP
  - updated_at handled in the UPSERT clause
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from sqlalchemy import create_engine, text

from apps.dw.feedback_repo import upsert_feedback
from apps.dw.memory_db import ensure_feedback_schema

def _get_env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _db_url() -> str:
    return _get_env("MEMORY_DB_URL") or "sqlite:////tmp/copilot_mem_dev.sqlite"


def _is_postgres(dialect_name: str) -> bool:
    return dialect_name.startswith("postgres")


def ensure_schema() -> None:
    """Create/upgrade feedback tables to the fields required by ADR‑0004."""
    engine = create_engine(_db_url(), future=True)
    ensure_feedback_schema(engine)
    dialect = engine.dialect.name
    pg = _is_postgres(dialect)
    with engine.begin() as cx:
        if pg:
            # Ensure updated_at trigger (idempotent)
            cx.execute(text("""
              CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
              BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;
            """))
            cx.execute(text("""
              DO $$
              BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='dw_feedback_set_updated_at') THEN
                  CREATE TRIGGER dw_feedback_set_updated_at
                    BEFORE UPDATE ON dw_feedback
                    FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
                END IF;
              END$$;
            """))
            # Patches table (JSONB)
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_patches(
                  id SERIAL PRIMARY KEY,
                  inquiry_id BIGINT,
                  kind VARCHAR(50),
                  payload_json JSONB,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  approved BOOLEAN DEFAULT FALSE
                );
            """))
        else:
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_patches(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  inquiry_id INTEGER,
                  kind TEXT,
                  payload_json TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  approved INTEGER DEFAULT 0
                );
            """))


def save_feedback(
    inquiry_id: int,
    rating: int,
    comment: str | None,
    hints_payload: Dict[str, Any] | None = None,
    *,
    auth_email: str | None = None,
    binds_payload: Dict[str, Any] | None = None,
    resolved_sql: str | None = None,
    status: str = "pending",
) -> None:
    """Persist a feedback row for later governance (UPSERT by inquiry_id).

    Backward compatibility: the old positional `hints_payload` is treated as
    the new `intent_payload`.
    """
    ensure_schema()
    engine = create_engine(_db_url(), future=True)
    payload = {
        "inquiry_id": inquiry_id,
        "auth_email": auth_email or "",
        "rating": int(rating or 0),
        "comment": comment or "",
        "intent_json": json.dumps(hints_payload or {}, ensure_ascii=False),
        "resolved_sql": resolved_sql or "",
        "binds_json": json.dumps(binds_payload or {}, ensure_ascii=False),
        "status": status or "pending",
    }

    upsert_feedback(engine, **payload)


__all__ = ["ensure_schema", "save_feedback"]
