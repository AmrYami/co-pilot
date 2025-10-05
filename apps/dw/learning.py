"""Persistence helpers for /dw/rate online learning signals."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text


_DDL_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS dw_rules (
      id SERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      question_norm TEXT NOT NULL,
      rule_kind TEXT NOT NULL,
      rule_payload JSONB NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      scope TEXT NOT NULL DEFAULT 'namespace'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dw_rules_enabled
        ON dw_rules (enabled)
    """,
    """
    CREATE TABLE IF NOT EXISTS dw_patches (
      id SERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      inquiry_id BIGINT,
      question_norm TEXT NOT NULL,
      rating INT NOT NULL,
      comment TEXT,
      patch_payload JSONB,
      status TEXT NOT NULL DEFAULT 'pending'
    )
    """,
)

_INITIALIZED_ENGINES: set[int] = set()


def _ensure_tables(engine) -> None:
    if engine is None:
        return
    key = id(engine)
    if key in _INITIALIZED_ENGINES:
        return
    with engine.begin() as cx:
        for stmt in _DDL_STATEMENTS:
            cx.execute(text(stmt))
    _INITIALIZED_ENGINES.add(key)


def _norm_question(question: str) -> str:
    return " ".join((question or "").strip().lower().split())


def _as_json(payload: Any) -> str:
    return json.dumps(payload or {})


def save_positive_rule(engine, question: str, applied_hints: Dict[str, Any]) -> None:
    """Persist positive feedback (rating >= 4) into ``dw_rules``."""

    if engine is None or not applied_hints:
        return
    _ensure_tables(engine)
    rows: list[tuple[str, Dict[str, Any]]] = []

    group_by = applied_hints.get("group_by")
    if group_by:
        rows.append(
            (
                "group_by",
                {
                    "group_by": group_by,
                    "gross": bool(applied_hints.get("gross")),
                },
            )
        )

    tokens = applied_hints.get("fts_tokens") or []
    if tokens:
        rows.append(
            (
                "fts",
                {
                    "tokens": tokens,
                    "operator": applied_hints.get("fts_operator", "OR"),
                    "columns": applied_hints.get("fts_columns", []),
                },
            )
        )

    eq_filters = applied_hints.get("eq_filters") or []
    if eq_filters:
        rows.append(("eq", {"eq_filters": eq_filters}))

    sort_by = applied_hints.get("sort_by")
    sort_desc = applied_hints.get("sort_desc")
    if sort_by or sort_desc is not None:
        rows.append(
            (
                "order_by",
                {
                    "sort_by": sort_by,
                    "sort_desc": bool(sort_desc) if sort_desc is not None else None,
                },
            )
        )

    if not rows:
        return

    with engine.begin() as cx:
        for kind, payload in rows:
            cx.execute(
                text(
                    """
                    INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled)
                    VALUES (:q, :k, :p, TRUE)
                    """
                ),
                {"q": _norm_question(question), "k": kind, "p": _as_json(payload)},
            )


def save_patch(
    engine,
    inquiry_id: Optional[int],
    question: str,
    rating: int,
    comment: str,
    parsed_hints: Dict[str, Any],
) -> None:
    """Persist a corrective patch for low-rating feedback (rating <= 2)."""

    if engine is None:
        return
    _ensure_tables(engine)
    with engine.begin() as cx:
        cx.execute(
            text(
                """
                INSERT INTO dw_patches (inquiry_id, question_norm, rating, comment, patch_payload, status)
                VALUES (:iid, :q, :r, :c, :p, 'pending')
                """
            ),
            {
                "iid": inquiry_id,
                "q": _norm_question(question),
                "r": int(rating),
                "c": comment or "",
                "p": _as_json(parsed_hints or {}),
            },
        )


def load_rules_for_question(engine, question: str) -> Dict[str, Any]:
    """Load merged rule hints for a question from ``dw_rules``."""

    if engine is None:
        return {}
    _ensure_tables(engine)
    merged: Dict[str, Any] = {}
    with engine.connect() as cx:
        rows = cx.execute(
            text(
                """
                SELECT rule_kind, rule_payload
                  FROM dw_rules
                 WHERE enabled = TRUE
                 ORDER BY id DESC
                 LIMIT 20
                """
            )
        )
        for row in rows:
            kind = row[0] if isinstance(row, tuple) else row["rule_kind"]
            payload = row[1] if isinstance(row, tuple) else row["rule_payload"]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            if not isinstance(payload, dict):
                continue
            if kind == "group_by":
                if payload.get("group_by"):
                    merged["group_by"] = payload.get("group_by")
                if payload.get("gross") is not None:
                    merged["gross"] = payload.get("gross")
            elif kind == "fts":
                if payload.get("tokens"):
                    merged["fts_tokens"] = payload.get("tokens")
                    merged["fts_operator"] = payload.get("operator", "OR")
                    if payload.get("columns"):
                        merged["fts_columns"] = payload.get("columns")
            elif kind == "eq":
                eq_payload = payload.get("eq_filters") or []
                if eq_payload:
                    existing = merged.setdefault("eq_filters", [])
                    for item in eq_payload:
                        if item not in existing:
                            existing.append(item)
            elif kind == "order_by":
                if payload.get("sort_by"):
                    merged["sort_by"] = payload.get("sort_by")
                if payload.get("sort_desc") is not None:
                    merged["sort_desc"] = bool(payload.get("sort_desc"))
    if merged:
        merged.setdefault("full_text_search", bool(merged.get("fts_tokens")))
    return merged


__all__ = [
    "load_rules_for_question",
    "save_patch",
    "save_positive_rule",
]

