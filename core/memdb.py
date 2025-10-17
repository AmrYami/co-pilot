"""Helpers for working with the Postgres-backed memory database."""

from __future__ import annotations

import os

from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_mem_engine(app: Flask) -> Engine | None:
    """Return (and cache) the SQLAlchemy engine for ``MEMORY_DB_URL``."""

    engine = app.config.get("MEM_ENGINE")
    if engine is not None:
        return engine

    url = app.config.get("MEMORY_DB_URL") or os.getenv("MEMORY_DB_URL")
    if not url:
        return None

    engine = create_engine(url, pool_pre_ping=True, future=True)
    app.config["MEM_ENGINE"] = engine
    return engine


def ensure_dw_feedback_schema(engine: Engine | None) -> None:
    """Ensure the ``dw_feedback`` schema matches ADR-0004 expectations."""

    if engine is None:
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dw_feedback (
                  id SERIAL PRIMARY KEY,
                  inquiry_id INTEGER UNIQUE NOT NULL,
                  auth_email TEXT,
                  rating INTEGER NOT NULL,
                  comment TEXT,
                  intent_json JSONB,
                  binds_json JSONB,
                  resolved_sql TEXT,
                  status TEXT NOT NULL DEFAULT 'pending',
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_feedback_inquiry_id
                ON dw_feedback(inquiry_id);
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
                BEGIN
                  NEW.updated_at = NOW();
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger WHERE tgname = 'dw_feedback_set_updated_at'
                  ) THEN
                    CREATE TRIGGER dw_feedback_set_updated_at
                    BEFORE UPDATE ON dw_feedback
                    FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
                  END IF;
                END$$;
                """
            )
        )


__all__ = ["ensure_dw_feedback_schema", "get_mem_engine"]
