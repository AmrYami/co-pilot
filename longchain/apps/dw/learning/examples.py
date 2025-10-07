"""Persistent example store for DW lightweight RAG hints."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional dependency for tests
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - allow running without SQLAlchemy
    create_engine = None  # type: ignore[assignment]

    def text(sql: str):  # type: ignore
        return sql


class ExampleStore:
    """Store and retrieve Q⇄SQL examples for lightweight planning hints."""

    def __init__(self, url: Optional[str] = None) -> None:
        self.url = url or os.getenv("MEMORY_DB_URL")
        self.engine = create_engine(self.url) if (self.url and create_engine) else None
        self._ensure_schema()

    # ------------------------------------------------------------------
    def _ensure_schema(self) -> None:
        if not self.engine:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS dw_examples (
                          id SERIAL PRIMARY KEY,
                          namespace TEXT,
                          q_norm TEXT,
                          q_raw  TEXT,
                          sql    TEXT,
                          weight FLOAT DEFAULT 1.0,
                          success_count INT DEFAULT 0,
                          created_at TIMESTAMPTZ DEFAULT now(),
                          last_used_at TIMESTAMPTZ
                        )
                        """
                    )
                )
        except Exception:
            # Swallow errors silently; the blueprint should still operate.
            pass

    # ------------------------------------------------------------------
    @staticmethod
    def normalize(text_value: str) -> str:
        return " ".join((text_value or "").lower().split())

    # ------------------------------------------------------------------
    def add_success(self, namespace: str, question: str, sql: str) -> None:
        if not self.engine:
            return
        q_norm = self.normalize(question)
        if not q_norm:
            return
        payload = {
            "ns": namespace,
            "q_norm": q_norm,
            "q_raw": question,
            "sql": sql,
        }
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO dw_examples(namespace, q_norm, q_raw, sql, weight, success_count)
                        VALUES(:ns, :q_norm, :q_raw, :sql, 1.0, 1)
                        """
                    ),
                    payload,
                )
        except Exception:
            # Never block the feedback flow due to DB issues.
            pass

    # ------------------------------------------------------------------
    def _fallback_similarity(self, namespace: str, q_norm: str, top_k: int) -> List[Dict[str, Any]]:
        if not self.engine:
            return []
        try:
            with self.engine.begin() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT id, namespace, q_norm, q_raw, sql, weight, success_count, created_at
                            FROM dw_examples
                            WHERE namespace = :ns
                            ORDER BY ABS(LENGTH(q_norm) - LENGTH(:q_norm)) ASC
                            LIMIT :k
                            """
                        ),
                        {"ns": namespace, "q_norm": q_norm, "k": int(top_k)},
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            return []
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    def find_similar(
        self,
        namespace: str,
        question: str,
        top_k: int = 3,
    ) -> List[Dict[str, Any]]:
        if not self.engine:
            return []
        q_norm = self.normalize(question)
        if not q_norm:
            return []
        params = {"ns": namespace, "q_norm": q_norm, "k": int(top_k)}
        try:
            with self.engine.begin() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT id, namespace, q_norm, q_raw, sql, weight, success_count, created_at
                            FROM dw_examples
                            WHERE namespace = :ns
                            ORDER BY similarity(q_norm, :q_norm) DESC NULLS LAST
                            LIMIT :k
                            """
                        ),
                        params,
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            return self._fallback_similarity(namespace, q_norm, top_k)
        return [dict(row) for row in rows]


__all__ = ["ExampleStore"]
