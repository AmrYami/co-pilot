"""Persistence helpers for /dw/rate online learning signals."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional, List
import hashlib

from sqlalchemy import text
import sqlalchemy as sa

from apps.dw.memory_db import get_mem_engine
from apps.dw.lib.intent_sig import build_intent_signature

log = logging.getLogger("dw")

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

_MIGRATIONS = (
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS question_norm TEXT",
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'namespace'",
    "CREATE INDEX IF NOT EXISTS idx_dw_rules_enabled ON dw_rules (enabled)",
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS rule_signature TEXT",
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS intent_sig JSONB",
    "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS intent_sha TEXT",
)


def _ensure_tables(engine) -> None:
    if engine is None:
        return
    key = id(engine)
    if key in _INITIALIZED_ENGINES:
        return
    with engine.begin() as cx:
        for stmt in _DDL_STATEMENTS:
            cx.execute(text(stmt))
        cx.execute(text("ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS question_norm TEXT"))
        cx.execute(
            text(
                "ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE"
            )
        )
        cx.execute(text("ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS rule_kind TEXT"))
        cx.execute(text("ALTER TABLE dw_rules ADD COLUMN IF NOT EXISTS rule_payload JSONB"))
        for stmt in _MIGRATIONS:
            try:
                cx.execute(text(stmt))
            except Exception as e:  # defensive
                log.warning("rules.migration.skip", extra={"err": str(e)})
    _INITIALIZED_ENGINES.add(key)


def _norm_question(question: str) -> str:
    return " ".join((question or "").strip().lower().split())


def _as_json(payload: Any) -> str:
    return json.dumps(payload or {})


def _flatten_fts_groups(hints: dict) -> list[str]:
    groups = []
    if isinstance(hints, dict):
        groups = hints.get("fts_groups") or []
    tokens: list[str] = []
    for group in groups:
        if isinstance(group, (list, tuple)):
            for token in group:
                if not isinstance(token, str):
                    continue
                text_token = token.strip()
                if text_token and text_token not in tokens:
                    tokens.append(text_token)
        elif isinstance(group, str):
            text_token = group.strip()
            if text_token and text_token not in tokens:
                tokens.append(text_token)
    return tokens


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj or {}, separators=(",", ":"), sort_keys=True)
    except Exception:
        return json.dumps(obj or {})


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def save_positive_rule(
    engine,
    question: str,
    applied_hints: Dict[str, Any],
    *,
    rule_signature: Optional[str] = None,
    intent_sig: Optional[Dict[str, Any]] = None,
    intent_sha: Optional[str] = None,
    intent: Optional[Dict[str, Any]] = None,
) -> None:
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

    # Accept both legacy 'fts_tokens' and new 'fts_groups'
    tokens = applied_hints.get("fts_tokens") or _flatten_fts_groups(applied_hints)
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

    # Prefer external signature artifacts; else build from provided intent if any
    signature_json: Optional[str] = rule_signature
    if signature_json is None and isinstance(intent, dict) and intent:
        try:
            sig_dict, sig_str, sha = build_intent_signature(intent)
            signature_json = sig_str
            if intent_sig is None:
                intent_sig = sig_dict
            if intent_sha is None:
                intent_sha = sha
        except Exception:
            signature_json = None

    intent_sha: Optional[str] = None
    intent_sig_obj: Optional[Dict[str, Any]] = None
    if signature_json:
        try:
            intent_sha = _sha256(signature_json)
            intent_sig_obj = json.loads(signature_json)
        except Exception:
            intent_sha = None
            intent_sig_obj = None

    with engine.begin() as cx:
        for kind, payload in rows:
            cx.execute(
                text(
                    """
                    INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled, rule_signature, intent_sig, intent_sha)
                    VALUES (:q, :k, CAST(:p AS JSONB), TRUE, :sig, CAST(:sig_json AS JSONB), :sha)
                    """
                ),
                {
                    "q": _norm_question(question),
                    "k": kind,
                    "p": _json_dumps(payload),
                    "sig": signature_json,
                    "sig_json": json.dumps(intent_sig) if intent_sig is not None else None,
                    "sha": intent_sha,
                },
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


def _merge_eq_filters_prefer_question(current_eq, rule_eq):
    """Merge rule eq filters with question eq filters, preferring question values.

    Accepts list-of-lists [["COL", [values...]], ...] or dict items {col/field, val/values}.
    Returns canonical list-of-lists.
    """
    qmap = {str(c).upper(): vs for c, vs in (current_eq or [])}
    out: List[List[Any]] = []

    def _norm(item):
        if isinstance(item, (list, tuple)) and len(item) == 2:
            return str(item[0]).upper(), item[1]
        if isinstance(item, dict):
            col = item.get("col") or item.get("field")
            vals = item.get("val") or item.get("values") or []
            if vals is not None and not isinstance(vals, (list, tuple, set)):
                vals = [vals]
            return str(col).upper(), list(vals)
        return None, None

    for it in (rule_eq or []):
        col, rvals = _norm(it)
        if not col:
            continue
        out.append([col, qmap.get(col, rvals or [])])  # prefer question values if present
    for col, vals in (current_eq or []):
        c = str(col).upper()
        if not any(c == x[0] for x in out):
            out.append([c, vals])
    return out


def load_rules_for_question(
    engine,
    question: str,
    intent: Optional[Dict[str, Any]] = None,
    intent_sig: Optional[Dict[str, Any]] = None,
    intent_sha: Optional[str] = None,
) -> Dict[str, Any]:
    """Load merged rule hints for a question from ``dw_rules``.

    - Prefer signature-first by checking both SHA-1 and SHA-256 variants.
    - Use mapping rows to avoid tuple-only access errors.
    - Merge EQ with question values taking precedence.
    """

    if engine is None:
        return {}
    _ensure_tables(engine)
    merged: Dict[str, Any] = {}
    norm = _norm_question(question)
    eq_from_rules: List[Any] = []

    with engine.connect() as cx:
        # Helper to execute and return mapping rows
        def _exec(sql: str, binds: Dict[str, Any]):
            return (
                cx.execute(text(sql), binds)
                .mappings()
                .all()
            )

        rows: List[Dict[str, Any]] = []

        # 1) Explicit artifacts
        if intent_sha:
            # Accept both SHA-1 and SHA-256 by probing both slots
            rows = _exec(
                """
                SELECT rule_kind AS rule_kind, rule_payload AS rule_payload
                  FROM dw_rules
                 WHERE enabled = TRUE
                   AND intent_sha IN (:sha1, :sha256)
                 ORDER BY id DESC
                 LIMIT 50
                """,
                {"sha1": intent_sha, "sha256": intent_sha},
            )

        # 2) Signature JSON matching
        if not rows and intent_sig:
            rows = _exec(
                """
                SELECT rule_kind AS rule_kind, rule_payload AS rule_payload
                  FROM dw_rules
                 WHERE enabled = TRUE
                   AND rule_signature = :sig
                 ORDER BY id DESC
                 LIMIT 50
                """,
                {"sig": _json_dumps(intent_sig)},
            )

        # 3) Build from provided intent (derive both SHA-1 and SHA-256)
        if not rows and intent:
            try:
                sig_dict, sig_str, sha1 = build_intent_signature(intent)
                sha256 = _sha256(sig_str)
                rows = _exec(
                    """
                    SELECT rule_kind AS rule_kind, rule_payload AS rule_payload
                      FROM dw_rules
                     WHERE enabled = TRUE
                       AND intent_sha IN (:sha1, :sha256)
                     ORDER BY id DESC
                     LIMIT 50
                    """,
                    {"sha1": sha1, "sha256": sha256},
                )
                if not rows:
                    rows = _exec(
                        """
                        SELECT rule_kind AS rule_kind, rule_payload AS rule_payload
                          FROM dw_rules
                         WHERE enabled = TRUE
                           AND rule_signature = :sig
                         ORDER BY id DESC
                         LIMIT 50
                        """,
                        {"sig": sig_str},
                    )
            except Exception:
                rows = []

        # 4) Fallback to question_norm + globals
        if not rows:
            rows = _exec(
                """
                SELECT rule_kind AS rule_kind, rule_payload AS rule_payload
                  FROM dw_rules
                 WHERE enabled = TRUE
                   AND (COALESCE(question_norm, '') = '' OR question_norm = :q)
                 ORDER BY id DESC
                 LIMIT 50
                """,
                {"q": norm},
            )

    for row in rows:
        # RowMapping → dict-like access
        kind = row.get("rule_kind")
        payload = row.get("rule_payload")
        if kind is None and "rule_kind" not in row:
            # Defensive fallback if a different DBAPI returns tuples
            try:
                kind, payload = row[0], row[1]  # type: ignore[index]
            except Exception:
                continue
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            continue
        k = str(kind or "").lower()

        if k == "rate_hint":
            # Back-compat: payload = {"intent": {...}, "binds": {...}, "resolved_sql": "..."}
            inner_intent = (payload or {}).get("intent") if isinstance(payload, dict) else {}
            if isinstance(inner_intent, str):
                try:
                    inner_intent = json.loads(inner_intent)
                except json.JSONDecodeError:
                    inner_intent = {}
            if not isinstance(inner_intent, dict):
                continue

            ft_tokens = _flatten_fts_groups(inner_intent)
            if not ft_tokens:
                tokens_from_intent = inner_intent.get("fts_tokens") or []
                if isinstance(tokens_from_intent, list):
                    ft_tokens = [
                        str(token).strip()
                        for token in tokens_from_intent
                        if isinstance(token, str) and str(token).strip()
                    ]
            if ft_tokens:
                merged["fts_tokens"] = ft_tokens
                merged["fts_operator"] = inner_intent.get("fts_operator", "OR")
                if inner_intent.get("fts_columns"):
                    merged["fts_columns"] = inner_intent.get("fts_columns")

            eq_payload = inner_intent.get("eq_filters")
            if isinstance(eq_payload, list) and eq_payload:
                eq_from_rules.extend(eq_payload)

            sort_by = inner_intent.get("sort_by")
            if isinstance(sort_by, str) and sort_by.strip():
                merged["sort_by"] = sort_by.strip()
            if inner_intent.get("sort_desc") is not None:
                merged["sort_desc"] = bool(inner_intent.get("sort_desc"))

            group_by = inner_intent.get("group_by")
            if group_by:
                merged["group_by"] = group_by
            if inner_intent.get("gross") is not None:
                merged["gross"] = bool(inner_intent.get("gross"))
            continue

        if k == "group_by":
            if payload.get("group_by"):
                merged["group_by"] = payload.get("group_by")
            if payload.get("gross") is not None:
                merged["gross"] = payload.get("gross")
        elif k == "fts":
            if payload.get("tokens"):
                merged["fts_tokens"] = payload.get("tokens")
                merged["fts_operator"] = payload.get("operator", "OR")
                if payload.get("columns"):
                    merged["fts_columns"] = payload.get("columns")
        elif k == "eq":
            eq_payload = payload.get("eq_filters") or []
            if eq_payload:
                eq_from_rules.extend(eq_payload)
        elif k == "order_by":
            if payload.get("sort_by"):
                merged["sort_by"] = payload.get("sort_by")
            if payload.get("sort_desc") is not None:
                merged["sort_desc"] = bool(payload.get("sort_desc"))

        if merged:
            merged.setdefault("full_text_search", bool(merged.get("fts_tokens")))

    # Prefer-question merge for EQ filters
    if eq_from_rules or (isinstance(intent, dict) and intent.get("eq_filters")):
        merged["eq_filters"] = _merge_eq_filters_prefer_question(
            (intent or {}).get("eq_filters"), eq_from_rules
        )
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
        try:
            _ENGINE = get_mem_engine()
        except Exception:
            return None
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
