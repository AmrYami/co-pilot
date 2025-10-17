"""Memory database connection helpers."""

from __future__ import annotations

from typing import Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from apps.core.memdb import get_memory_engine

_SCHEMA_INITIALIZED: Set[int] = set()


def get_mem_engine(*args, **kwargs) -> Engine:
    """Return the shared Postgres-backed memory engine."""

    return get_memory_engine(*args, **kwargs)


def ensure_feedback_schema(engine: Engine | None) -> None:
    """Ensure the ``dw_feedback`` table exists with the expected shape."""

    if engine is None:
        return

    key = id(engine)
    if key in _SCHEMA_INITIALIZED:
        return

    dialect = getattr(engine, "dialect", None)
    name = getattr(dialect, "name", "") if dialect is not None else ""

    with engine.begin() as cn:
        if name.startswith("sqlite"):
            cn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS dw_feedback (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      inquiry_id INTEGER UNIQUE NOT NULL,
                      auth_email TEXT,
                      rating INTEGER,
                      comment TEXT,
                      intent_json TEXT,
                      binds_json TEXT,
                      resolved_sql TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            cn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id
                      ON dw_feedback(inquiry_id)
                    """
                )
            )
            rows = cn.execute(text("PRAGMA table_info('dw_feedback')")).fetchall()
            cols = {row[1] for row in rows}
            alterations = []
            if "auth_email" not in cols:
                alterations.append("ALTER TABLE dw_feedback ADD COLUMN auth_email TEXT")
            if "intent_json" not in cols:
                alterations.append("ALTER TABLE dw_feedback ADD COLUMN intent_json TEXT")
            if "binds_json" not in cols:
                alterations.append("ALTER TABLE dw_feedback ADD COLUMN binds_json TEXT")
            if "resolved_sql" not in cols:
                alterations.append("ALTER TABLE dw_feedback ADD COLUMN resolved_sql TEXT")
            if "status" not in cols:
                alterations.append(
                    "ALTER TABLE dw_feedback ADD COLUMN status TEXT DEFAULT 'pending'"
                )
            if "created_at" not in cols:
                alterations.append(
                    "ALTER TABLE dw_feedback ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP"
                )
            if "updated_at" not in cols:
                alterations.append(
                    "ALTER TABLE dw_feedback ADD COLUMN updated_at TEXT DEFAULT CURRENT_TIMESTAMP"
                )
            for stmt in alterations:
                cn.execute(text(stmt))
        else:
            cn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS dw_feedback (
                      id SERIAL PRIMARY KEY,
                      inquiry_id BIGINT UNIQUE NOT NULL,
                      auth_email TEXT,
                      rating INT,
                      comment TEXT,
                      intent_json JSONB,
                      binds_json JSONB,
                      resolved_sql TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            cn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id
                      ON dw_feedback(inquiry_id)
                    """
                )
            )

    _SCHEMA_INITIALIZED.add(key)


__all__ = ["get_mem_engine", "ensure_feedback_schema"]
