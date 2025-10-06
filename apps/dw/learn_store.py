"""Lightweight learning store for DW rate feedback."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:  # pragma: no cover - optional dependency in some environments
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except Exception:  # pragma: no cover - degrade gracefully when SQLAlchemy missing
    create_engine = None  # type: ignore[assignment]
    text = None  # type: ignore[assignment]
    Engine = None  # type: ignore[misc, assignment]


LOGGER = logging.getLogger("dw.learn_store")


class LearningStore:
    def __init__(self) -> None:
        url = os.getenv("MEMORY_DB_URL") or ""
        self.enabled = bool(url) and create_engine is not None and text is not None
        self.engine: Optional[Engine] = None
        if self.enabled:
            try:
                self.engine = create_engine(url, pool_pre_ping=True, future=True)
                self._bootstrap()
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.warning("dw.learn_store disabled: %s", exc)
                self.enabled = False
                self.engine = None

    def _bootstrap(self) -> None:
        if not self.engine or text is None:
            return
        with self.engine.begin() as cx:
            cx.execute(
                text(
                    """
            CREATE TABLE IF NOT EXISTS dw_examples(
                id SERIAL PRIMARY KEY,
                inquiry_id BIGINT,
                question TEXT,
                sql TEXT,
                rule_version INTEGER DEFAULT 1,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
            );
            """
                )
            )
            cx.execute(
                text(
                    """
            CREATE TABLE IF NOT EXISTS dw_patches(
                id SERIAL PRIMARY KEY,
                inquiry_id BIGINT,
                comment TEXT,
                produced_sql TEXT,
                patch_version INTEGER DEFAULT 1,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
                status TEXT DEFAULT 'pending'
            );
            """
                )
            )

    def save_example(self, ex: "ExampleRecord") -> None:
        if not self.enabled or not self.engine or text is None:
            return
        try:
            with self.engine.begin() as cx:
                cx.execute(
                    text(
                        """
                INSERT INTO dw_examples(inquiry_id, question, sql, rule_version, created_at)
                VALUES (:inq, :q, :s, :rv, :ts)
                """
                    ),
                    {
                        "inq": ex.inquiry_id,
                        "q": ex.question,
                        "s": ex.sql,
                        "rv": ex.rule_version,
                        "ts": ex.created_at,
                    },
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("dw.learn_store example save failed: %s", exc)

    def save_patch(self, p: "PatchRecord") -> None:
        if not self.enabled or not self.engine or text is None:
            return
        try:
            with self.engine.begin() as cx:
                cx.execute(
                    text(
                        """
                INSERT INTO dw_patches(inquiry_id, comment, produced_sql, patch_version, created_at)
                VALUES (:inq, :c, :s, :pv, :ts)
                """
                    ),
                    {
                        "inq": p.inquiry_id,
                        "c": p.comment,
                        "s": p.produced_sql,
                        "pv": p.patch_version,
                        "ts": p.created_at,
                    },
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("dw.learn_store patch save failed: %s", exc)


@dataclass
class ExampleRecord:
    inquiry_id: int
    question: str
    sql: str
    created_at: datetime
    rule_version: int = 1


@dataclass
class PatchRecord:
    inquiry_id: int
    comment: str
    produced_sql: str
    created_at: datetime
    patch_version: int = 1
