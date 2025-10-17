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
from typing import Any, Dict, List, Set

from sqlalchemy import create_engine, text


def _get_env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _db_url() -> str:
    return _get_env("MEMORY_DB_URL") or "sqlite:////tmp/copilot_mem_dev.sqlite"


def _is_postgres(dialect_name: str) -> bool:
    return dialect_name.startswith("postgres")


def _sqlite_columns(cx) -> Set[str]:
    """Return existing column names for dw_feedback under SQLite."""
    rows = cx.execute(text("PRAGMA table_info('dw_feedback')")).fetchall()
    return {r[1] for r in rows}  # type: ignore[index]


def ensure_schema() -> None:
    """Create/upgrade feedback tables to the fields required by ADR‑0004."""
    engine = create_engine(_db_url(), future=True)
    dialect = engine.dialect.name
    pg = _is_postgres(dialect)
    with engine.begin() as cx:
        if pg:
            # PostgreSQL — JSONB + TIMESTAMPTZ, robust defaults
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_feedback(
                  id SERIAL PRIMARY KEY,
                  inquiry_id BIGINT UNIQUE NOT NULL,
                  auth_email TEXT,
                  rating INT NOT NULL,
                  comment TEXT,
                  intent_json JSONB,
                  binds_json JSONB,
                  resolved_sql TEXT,
                  status TEXT NOT NULL DEFAULT 'pending',
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """))
            cx.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id
                ON dw_feedback(inquiry_id);
            """))
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
            # SQLite — JSON as TEXT; CURRENT_TIMESTAMP for dates
            cx.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_feedback(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  inquiry_id INTEGER UNIQUE NOT NULL,
                  auth_email TEXT,
                  rating INTEGER NOT NULL,
                  comment TEXT,
                  intent_json TEXT,
                  binds_json TEXT,
                  resolved_sql TEXT,
                  status TEXT NOT NULL DEFAULT 'pending',
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """))
            cx.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id
                ON dw_feedback(inquiry_id);
            """))
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
            # If an older table existed, try to add missing columns (best‑effort).
            cols = _sqlite_columns(cx)
            add_cols: List[str] = []
            if "auth_email" not in cols:
                add_cols.append("ALTER TABLE dw_feedback ADD COLUMN auth_email TEXT")
            if "intent_json" not in cols:
                add_cols.append("ALTER TABLE dw_feedback ADD COLUMN intent_json TEXT")
            if "binds_json" not in cols:
                add_cols.append("ALTER TABLE dw_feedback ADD COLUMN binds_json TEXT")
            if "resolved_sql" not in cols:
                add_cols.append("ALTER TABLE dw_feedback ADD COLUMN resolved_sql TEXT")
            if "status" not in cols:
                add_cols.append(
                    "ALTER TABLE dw_feedback ADD COLUMN status TEXT DEFAULT 'pending'"
                )
            if "updated_at" not in cols:
                add_cols.append(
                    "ALTER TABLE dw_feedback ADD COLUMN updated_at TEXT DEFAULT CURRENT_TIMESTAMP"
                )
            for stmt in add_cols:
                cx.execute(text(stmt))


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
    dialect = engine.dialect.name
    pg = _is_postgres(dialect)

    intent_json = json.dumps(hints_payload or {}, ensure_ascii=False)
    binds_json = json.dumps(binds_payload or {}, ensure_ascii=False)

    with engine.begin() as cx:
        if pg:
            cx.execute(
                text("""
                    INSERT INTO dw_feedback(
                      inquiry_id, auth_email, rating, comment,
                      intent_json, binds_json, resolved_sql, status,
                      created_at, updated_at
                    )
                    VALUES(
                      :iid, :email, :rating, :comment,
                      CAST(:intent AS JSONB), CAST(:binds AS JSONB), :sql, :status,
                      NOW(), NOW()
                    )
                    ON CONFLICT (inquiry_id) DO UPDATE SET
                      auth_email   = EXCLUDED.auth_email,
                      rating       = EXCLUDED.rating,
                      comment      = EXCLUDED.comment,
                      intent_json  = EXCLUDED.intent_json,
                      binds_json   = EXCLUDED.binds_json,
                      resolved_sql = EXCLUDED.resolved_sql,
                      status       = EXCLUDED.status,
                      updated_at   = NOW();
                """),
                {
                    "iid": inquiry_id,
                    "email": auth_email,
                    "rating": rating,
                    "comment": comment,
                    "intent": intent_json,
                    "binds": binds_json,
                    "sql": resolved_sql,
                    "status": status,
                },
            )
        else:
            # SQLite — JSON as TEXT; updated_at via UPSERT expression
            cx.execute(
                text("""
                    INSERT INTO dw_feedback(
                      inquiry_id, auth_email, rating, comment,
                      intent_json, binds_json, resolved_sql, status,
                      created_at, updated_at
                    )
                    VALUES(
                      :iid, :email, :rating, :comment,
                      :intent, :binds, :sql, :status,
                      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(inquiry_id) DO UPDATE SET
                      auth_email   = excluded.auth_email,
                      rating       = excluded.rating,
                      comment      = excluded.comment,
                      intent_json  = excluded.intent_json,
                      binds_json   = excluded.binds_json,
                      resolved_sql = excluded.resolved_sql,
                      status       = excluded.status,
                      updated_at   = CURRENT_TIMESTAMP;
                """),
                {
                    "iid": inquiry_id,
                    "email": auth_email,
                    "rating": rating,
                    "comment": comment,
                    "intent": intent_json,
                    "binds": binds_json,
                    "sql": resolved_sql,
                    "status": status,
                },
            )


__all__ = ["ensure_schema", "save_feedback"]
