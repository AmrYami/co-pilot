# -*- coding: utf-8 -*-
"""Lightweight feedback store for /dw/rate overrides."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from sqlalchemy import create_engine, text


def _get_env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _db_url() -> str:
    return _get_env("MEMORY_DB_URL") or "sqlite:////tmp/copilot_mem_dev.sqlite"


def ensure_schema() -> None:
    """Create feedback tables when they do not exist."""
    engine = create_engine(_db_url())
    with engine.begin() as cx:
        cx.execute(
            text(
                """
        CREATE TABLE IF NOT EXISTS dw_feedback (
            id SERIAL PRIMARY KEY,
            inquiry_id BIGINT,
            rating INT,
            comment TEXT,
            hints_json TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
            )
        )
        cx.execute(
            text(
                """
        CREATE TABLE IF NOT EXISTS dw_patches (
            id SERIAL PRIMARY KEY,
            inquiry_id BIGINT,
            kind VARCHAR(50),
            payload_json TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            approved BOOLEAN DEFAULT FALSE
        )
        """
            )
        )


def save_feedback(inquiry_id: int, rating: int, comment: str, hints_payload: Dict[str, Any]) -> None:
    """Persist a feedback row for later governance."""
    ensure_schema()
    engine = create_engine(_db_url())
    payload = json.dumps(hints_payload, ensure_ascii=False)
    with engine.begin() as cx:
        cx.execute(
            text(
                """
        INSERT INTO dw_feedback (inquiry_id, rating, comment, hints_json)
        VALUES (:iid, :rating, :comment, :payload)
        """
            ),
            {
                "iid": inquiry_id,
                "rating": rating,
                "comment": comment,
                "payload": payload,
            },
        )


__all__ = ["ensure_schema", "save_feedback"]
