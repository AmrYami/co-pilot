"""Persistence helpers for /dw/rate online learning signals."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, List

from sqlalchemy import text
import sqlalchemy as sa

from apps.dw.settings import get_setting

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
      status TEXT NOT NULL DEFAULT 'proposed'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dw_feedback (
      id SERIAL PRIMARY KEY,
      inquiry_id BIGINT,
      rating INT,
      comment TEXT,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
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
    "record_feedback",
    "to_patch_from_comment",
]



_ENGINE: Optional[sa.Engine] = None


def _engine() -> Optional[sa.Engine]:
    global _ENGINE
    if _ENGINE is None:
        url = get_setting("MEMORY_DB_URL", scope="global")
        if not url:
            return None
        _ENGINE = sa.create_engine(url, pool_pre_ping=True, future=True)
        _ensure_tables(_ENGINE)
    return _ENGINE


def record_feedback(inquiry_id: int, rating: int, comment: str) -> None:
    eng = _engine()
    if not eng:
        return
    with eng.begin() as cx:
        cx.execute(
            text("INSERT INTO dw_feedback(inquiry_id, rating, comment) VALUES(:iid, :rating, :comment)"),
            {"iid": inquiry_id, "rating": int(rating) if rating is not None else None, "comment": comment},
        )


_RE_EQ = re.compile(r"\beq:\s*([A-Za-z0-9_ ]+)\s*=\s*([^\;]+)", re.I)
_RE_FTS = re.compile(r"\bfts:\s*([^\;]+)", re.I)
_RE_GB = re.compile(r"\bgroup_by:\s*([A-Za-z0-9_ ]+)", re.I)
_RE_GROSS = re.compile(r"\bgross:\s*(true|false)\b", re.I)
_RE_ORDER = re.compile(r"\border_by:\s*([A-Za-z0-9_ ]+)\s*(asc|desc)?", re.I)
_RE_TOP = re.compile(r"\btop\s+(\d+)\b", re.I)
_RE_BOTTOM = re.compile(r"\bbottom\s+(\d+)\b", re.I)


def to_patch_from_comment(comment: str) -> Dict[str, Any]:
    c = comment or ""
    eq_filters: List[Dict[str, Any]] = []
    for m in _RE_EQ.finditer(c):
        col = m.group(1).strip().replace(" ", "_").upper()
        val = m.group(2).strip().strip("'\"")
        eq_filters.append({"col": col, "val": val, "ci": True, "trim": True})
    fts_tokens: Optional[List[str]] = None
    m = _RE_FTS.search(c)
    if m:
        raw = m.group(1)
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        if parts:
            fts_tokens = parts
    gb = None
    m = _RE_GB.search(c)
    if m:
        gb = m.group(1).strip().replace(" ", "_").upper()
    gross = None
    m = _RE_GROSS.search(c)
    if m:
        gross = (m.group(1).lower() == "true")
    sort_by = None
    sort_desc = True
    m = _RE_ORDER.search(c)
    if m:
        sort_by = m.group(1).strip().upper()
        if m.group(2):
            sort_desc = (m.group(2).lower() == "desc")
    top_n = None
    m = _RE_TOP.search(c)
    if m:
        top_n = int(m.group(1))
        sort_desc = False
    m = _RE_BOTTOM.search(c)
    if m:
        top_n = int(m.group(1))
        sort_desc = True
    return {
        "eq_filters": eq_filters,
        "fts_tokens": fts_tokens,
        "fts_operator": "OR",
        "group_by": gb,
        "gross": gross,
        "sort_by": sort_by,
        "sort_desc": sort_desc,
        "top_n": top_n,
    }

