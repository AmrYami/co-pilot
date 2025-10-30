"""Persistence helpers for /dw/rate online learning signals."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, Optional, List
import hashlib

from sqlalchemy import text
import sqlalchemy as sa

from apps.dw.memory_db import get_mem_engine
from apps.dw.sql_shared import eq_alias_columns
from apps.dw.lib.intent_sig import build_intent_signature
try:  # prefer the canonical, value-agnostic signature from learning_store
    from apps.dw.learning_store import _canon_signature_from_intent as _canon_sig  # type: ignore
except Exception:  # fallback stub; will recompute locally if needed
    _canon_sig = None  # type: ignore

log = logging.getLogger("dw")

_EMAIL_RE = re.compile(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$")
_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?$")

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
    # Performance indexes for signature-first lookups
    "CREATE INDEX IF NOT EXISTS idx_dw_rules_intent_sha ON dw_rules (intent_sha)",
    "CREATE INDEX IF NOT EXISTS idx_dw_rules_rule_signature ON dw_rules (rule_signature)",
    "CREATE INDEX IF NOT EXISTS idx_dw_rules_question_norm ON dw_rules (question_norm)",
    # Ensure ON CONFLICT (intent_sha, rule_kind) is valid via named unique index
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_dw_rules_intent ON dw_rules (intent_sha, rule_kind)",
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


def _val_type(value: Any) -> str:
    text = str(value or "").strip()
    if _EMAIL_RE.match(text):
        return "EMAIL"
    if _NUMBER_RE.match(text):
        return "NUMBER"
    return "TEXT"


def _normalize_value_list(values: List[Any]) -> List[Any]:
    out: List[Any] = []
    seen: set[str] = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            text = v.strip()
            if not text:
                continue
            key = text.upper()
            value = text
        else:
            value = v
            key = str(v)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _normalize_eq_filters_list(raw: Any) -> List[List[Any]]:
    if not isinstance(raw, list):
        return []
    normalized: List[List[Any]] = []
    for item in raw:
        col = ""
        vals: List[Any] = []
        if isinstance(item, (list, tuple)) and len(item) == 2:
            col = str(item[0] or "").strip().upper()
            candidate = item[1]
        elif isinstance(item, dict):
            col = str((item.get("col") or item.get("field") or "")).strip().upper()
            candidate = (
                item.get("val")
                if item.get("val") is not None
                else item.get("values")
            )
        else:
            continue
        if not col:
            continue
        if isinstance(candidate, (list, tuple, set)):
            vals = list(candidate)
        elif candidate is not None:
            vals = [candidate]
        else:
            vals = []
        clean_vals = _normalize_value_list(vals)
        if not clean_vals:
            continue
        normalized.append([col, clean_vals])
    return normalized


def _build_question_value_map(intent: Optional[Dict[str, Any]]) -> Dict[str, List[Any]]:
    eq_filters = _normalize_eq_filters_list((intent or {}).get("eq_filters") or [])
    value_map: Dict[str, List[Any]] = {}
    for col, values in eq_filters:
        key = str(col or "").strip().upper()
        if not key:
            continue
        bucket = value_map.setdefault(key, [])
        for val in values:
            bucket.append(val)
        value_map[key] = _normalize_value_list(bucket)
    return value_map


def _apply_eq_shape(
    shape_items: List[Dict[str, Any]],
    question_values: Dict[str, List[Any]],
) -> List[List[Any]]:
    eq_filters: List[List[Any]] = []
    for item in shape_items:
        logical = str(item.get("logical") or "").strip().upper()
        column = str(item.get("column") or "").strip().upper()
        targets = item.get("columns") or item.get("targets") or []
        if isinstance(targets, list):
            target_list = [str(t or "").strip().upper() for t in targets if str(t or "").strip()]
        else:
            target_list = []
        if logical:
            values = question_values.get(logical)
            if not values and target_list:
                collected: List[Any] = []
                for target in target_list:
                    collected.extend(question_values.get(target, []))
                values = _normalize_value_list(collected)
            if not values:
                continue
            eq_filters.append([logical, list(values)])
        elif column:
            values = question_values.get(column)
            if not values:
                continue
            eq_filters.append([column, list(values)])
        else:
            continue
    return eq_filters


def _value_policy() -> str:
    try:
        policy = (os.getenv("DW_EQ_VALUE_POLICY") or "question_only").strip().lower()
    except Exception:
        policy = "question_only"
    if policy not in {"question_only", "prefer_question"}:
        policy = "question_only"
    return policy


def _normalize_token_str(token: Any) -> Optional[str]:
    if token is None:
        return None
    text = str(token).strip().lower()
    if not text:
        return None
    base = text
    if base.endswith("'s") and len(base) > 2:
        base = base[:-2]
    elif base.endswith("ies") and len(base) > 3:
        base = base[:-3] + "y"
    elif base.endswith(("xes", "zes", "ches", "shes", "sses", "ees")) and len(base) > 3:
        base = base[:-2]
    elif base.endswith("s") and len(base) > 3 and not base.endswith("ss"):
        base = base[:-1]
    return base


def _normalize_token_list(tokens: Any) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()

    def _add(value: Any) -> None:
        normalized = _normalize_token_str(value)
        if not normalized:
            return
        if normalized in seen:
            return
        seen.add(normalized)
        result.append(normalized)

    if isinstance(tokens, (list, tuple, set)):
        for item in tokens:
            if isinstance(item, (list, tuple, set)):
                for sub in item:
                    _add(sub)
            else:
                _add(item)
    elif tokens is not None:
        _add(tokens)
    return result


def _normalize_learning_hints(hints: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    src = hints or {}
    normalized: Dict[str, Any] = {}

    eq_filters = _normalize_eq_filters_list(src.get("eq_filters") or [])
    if eq_filters:
        normalized["eq_filters"] = eq_filters
    if isinstance(src.get("or_groups"), list):
        normalized["or_groups"] = [grp for grp in src["or_groups"] if isinstance(grp, list) and grp]

    tokens = _normalize_token_list(src.get("fts_tokens"))
    if tokens:
        normalized["fts_tokens"] = tokens
        if src.get("fts_operator"):
            normalized["fts_operator"] = src.get("fts_operator")
        if isinstance(src.get("fts_columns"), list):
            columns = [
                str(col or "").strip().upper()
                for col in src.get("fts_columns")
                if isinstance(col, str) and str(col or "").strip()
            ]
            if columns:
                normalized["fts_columns"] = columns

    if src.get("group_by"):
        normalized["group_by"] = src.get("group_by")
    if src.get("gross") is not None:
        normalized["gross"] = bool(src.get("gross"))

    sort_by = src.get("sort_by")
    if isinstance(sort_by, str) and sort_by.strip():
        normalized["sort_by"] = sort_by.strip().upper()
    if src.get("sort_desc") is not None:
        normalized["sort_desc"] = bool(src.get("sort_desc"))

    return normalized


def _normalize_learning_intent(intent: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    src = intent or {}
    normalized: Dict[str, Any] = {}

    eq_filters = _normalize_eq_filters_list(src.get("eq_filters") or [])
    if eq_filters:
        normalized["eq_filters"] = eq_filters

    if isinstance(src.get("or_groups"), list):
        normalized["or_groups"] = [grp for grp in src["or_groups"] if isinstance(grp, list) and grp]

    tokens = _normalize_token_list(src.get("fts_tokens"))
    if not tokens and isinstance(src.get("fts_groups"), list):
        tokens = _normalize_token_list(src.get("fts_groups"))
    if tokens:
        normalized["fts_tokens"] = tokens
        if src.get("fts_operator"):
            normalized["fts_operator"] = src.get("fts_operator")
        if isinstance(src.get("fts_columns"), list):
            columns = [
                str(col or "").strip().upper()
                for col in src.get("fts_columns")
                if isinstance(col, str) and str(col or "").strip()
            ]
            if columns:
                normalized["fts_columns"] = columns

    if src.get("group_by"):
        normalized["group_by"] = src.get("group_by")
    if src.get("gross") is not None:
        normalized["gross"] = bool(src.get("gross"))

    sort_by = src.get("sort_by") or src.get("order", {}).get("col")
    if isinstance(sort_by, str) and sort_by.strip():
        normalized["sort_by"] = sort_by.strip().upper()
    sort_desc: Optional[bool] = None
    if src.get("sort_desc") is not None:
        sort_desc = bool(src.get("sort_desc"))
    elif isinstance(src.get("order"), dict) and src.get("order", {}).get("desc") is not None:
        sort_desc = bool(src["order"]["desc"])
    if sort_desc is not None:
        normalized["sort_desc"] = sort_desc

    if normalized.get("sort_by") and not isinstance(normalized.get("order"), dict):
        normalized["order"] = {
            "col": normalized.get("sort_by"),
            "desc": bool(normalized.get("sort_desc", True)),
        }

    return normalized


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
    applied_hints = _normalize_learning_hints(applied_hints)
    intent = _normalize_learning_intent(intent)
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

    # Persist EQ with optional cross-column OR groups when aliases were used
    raw_eq = applied_hints.get("eq_filters") or []
    if raw_eq:
        alias_map: Dict[str, List[str]] = {}
        try:
            alias_map = eq_alias_columns()
        except Exception:
            alias_map = {}

        shape_items: List[Dict[str, Any]] = []

        def _norm(it) -> tuple[str, List[Any]] | None:
            if isinstance(it, (list, tuple)) and len(it) == 2:
                col = str(it[0] or "").upper().strip()
                vals = it[1]
                return col, list(vals) if isinstance(vals, (list, tuple, set)) else [vals]
            if isinstance(it, dict):
                col = str(it.get("col") or it.get("field") or "").upper().strip()
                val = it.get("val") if it.get("val") is not None else it.get("value")
                if val is None:
                    vals = []
                elif isinstance(val, (list, tuple, set)):
                    vals = list(val)
                else:
                    vals = [val]
                return col, vals
            return None

        for it in raw_eq:
            norm = _norm(it)
            if not norm:
                continue
            col, vals = norm
            if not col:
                continue
            clean_vals = _normalize_value_list(vals)
            if not clean_vals:
                continue
            entry: Dict[str, Any] = {
                "op": "in" if len(clean_vals) > 1 else "eq",
                "types": sorted({_val_type(v) for v in clean_vals}),
                "ci": True,
                "trim": True,
            }
            targets = alias_map.get(col)
            if targets:
                entry["logical"] = col
                entry["columns"] = targets
            else:
                entry["column"] = col
            shape_items.append(entry)

        if shape_items:
            rows.append(("eq_shape", {"items": shape_items}))

        # Optionally persist eq_like shards (alias-aware) as a tolerant backup
        try:
            import os
            want_like = str(os.getenv("DW_EQ_LIKE_RULES", "1")).lower() in {"1", "true", "yes"}
        except Exception:
            want_like = True
        if want_like and alias_map:
            # Build alias->tokens mapping from raw_eq values
            like_fragments: Dict[str, List[str]] = {}
            min_len = 4
            try:
                raw_min = os.getenv("DW_EQ_TOKEN_MIN_LEN")
                if raw_min is not None:
                    min_len = int(raw_min)
            except Exception:
                min_len = 4

            import re as _re
            def _tokens(v: Any) -> List[str]:
                parts = [tok for tok in _re.split(r"[^A-Z0-9]+", str(v or "").upper()) if tok]
                out: List[str] = []
                seen: set[str] = set()
                for t in parts:
                    if len(t) >= min_len and t not in seen:
                        seen.add(t)
                        out.append(t)
                return out[:3]  # cap

            for it in raw_eq:
                norm = _norm(it)
                if not norm:
                    continue
                col, vals = norm
                if not col or not vals:
                    continue
                if col not in alias_map:
                    continue
                toks: List[str] = []
                for v in vals:
                    toks.extend(_tokens(v))
                # de-dup
                uniq: List[str] = []
                seen_local: set[str] = set()
                for t in toks:
                    if t in seen_local:
                        continue
                    seen_local.add(t)
                    uniq.append(t)
                if uniq:
                    like_fragments[col] = uniq
            if like_fragments:
                rows.append(("eq_like", {"fragments": like_fragments, "min_len": min_len}))

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
    # Prefer value-agnostic signature shape if available
    if signature_json is None and isinstance(intent, dict) and intent:
        try:
            if _canon_sig:
                sha256, sha1, sig_text = _canon_sig(intent)  # type: ignore[misc]
                signature_json = sig_text
                if intent_sig is None:
                    try:
                        intent_sig = json.loads(sig_text)
                    except Exception:
                        intent_sig = None
                if intent_sha is None:
                    intent_sha = sha256
            else:
                # Fallback to legacy value-based signature
                sig_dict, sig_str, sha = build_intent_signature(intent)
                signature_json = sig_str
                if intent_sig is None:
                    intent_sig = sig_dict
                if intent_sha is None:
                    intent_sha = _sha256(sig_str)
        except Exception:
            signature_json = None

    # Normalize intent_sig object if still a JSON string
    intent_sig_obj: Optional[Dict[str, Any]] = None
    if isinstance(intent_sig, dict):
        intent_sig_obj = intent_sig
    elif signature_json:
        try:
            intent_sig_obj = json.loads(signature_json)
        except Exception:
            intent_sig_obj = None

    with engine.begin() as cx:
        for kind, payload in rows:
            cx.execute(
                text(
                    """
                    INSERT INTO dw_rules (question_norm, rule_kind, rule_payload, enabled, rule_signature, intent_sig, intent_sha)
                    VALUES (:q, :k, CAST(:p AS JSONB), TRUE, :sig, CAST(:sig_json AS JSONB), :sha)
                    ON CONFLICT (intent_sha, rule_kind) DO UPDATE SET
                        question_norm = EXCLUDED.question_norm,
                        rule_payload  = EXCLUDED.rule_payload,
                        rule_signature = EXCLUDED.rule_signature,
                        intent_sig    = EXCLUDED.intent_sig,
                        enabled       = TRUE
                    """
                ),
                {
                    "q": _norm_question(question),
                    "k": kind,
                    "p": _json_dumps(payload),
                    "sig": signature_json,
                    "sig_json": json.dumps(intent_sig_obj) if intent_sig_obj is not None else None,
                    "sha": intent_sha,
                },
            )
    try:
        log.info(
            {
                "event": "rules.save",
                "question_norm": _norm_question(question),
                "kinds": [k for k, _ in rows],
                "has_sig": bool(signature_json),
                "sig_len": len(signature_json or ""),
            }
        )
    except Exception:
        pass


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
    shape_rules: List[Dict[str, Any]] = []

    try:
        alias_map = eq_alias_columns()
    except Exception:
        alias_map = {}

    intent_norm: Dict[str, Any] = {}
    if isinstance(intent, dict):
        intent_norm = _normalize_learning_intent(intent)

    with engine.connect() as cx:
        try:
            sig_for_log = None
            if intent_norm:
                if _canon_sig:
                    sha256, sha1, sig_text = _canon_sig(intent_norm)  # type: ignore[misc]
                    try:
                        log.info({"event": "rules.intent.normalized", "intent": intent_norm})
                        log.info({"event": "rules.intent.signature", "sig_text": sig_text})
                    except Exception:
                        pass
                else:
                    sig_dict, sig_text, sha1 = build_intent_signature(intent_norm)
                    sha256 = _sha256(sig_text)
                sig_for_log = {"sha1": sha1, "sha256": sha256, "sig_len": len(sig_text or "")}
            log.info({"event": "rules.signature.compute", "qnorm": norm, **(sig_for_log or {})})
        except Exception:
            pass
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

        # 3) Build from provided intent (derive both SHA-1 and SHA-256) using value-agnostic shape
        if not rows and intent_norm:
            try:
                if _canon_sig:
                    sha256, sha1, sig_str = _canon_sig(intent_norm)  # type: ignore[misc]
                else:
                    # Fallback to legacy: use value-based signature then convert to both hashes
                    sig_dict, sig_str, sha1 = build_intent_signature(intent_norm)
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
        try:
            log.info(
                {
                    "event": "rules.load.summary",
                    "by_sha": bool(intent_sha),
                    "rows": len(rows or []),
                }
            )
        except Exception:
            pass

    kinds_found: List[str] = []
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
        kinds_found.append(k)

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
        elif k == "eq_shape":
            items = payload.get("items")
            if isinstance(items, list) and items:
                for entry in items:
                    if isinstance(entry, dict):
                        shape_rules.append(entry)
        elif k == "eq":
            eq_payload = payload.get("eq_filters") or []
            if eq_payload:
                eq_from_rules.extend(eq_payload)
            og = payload.get("or_groups") or []
            if isinstance(og, list) and og:
                try:
                    existing_og = merged.setdefault("or_groups", [])
                    for grp in og:
                        if isinstance(grp, list) and grp:
                            existing_og.append(grp)
                except Exception:
                    pass
        elif k == "order_by":
            if payload.get("sort_by"):
                merged["sort_by"] = payload.get("sort_by")
            if payload.get("sort_desc") is not None:
                merged["sort_desc"] = bool(payload.get("sort_desc"))
        elif k == "eq_like":
            # Merge persisted alias->LIKE fragments as a tolerant backup overlay.
            # Expected payload shape: { "fragments": { ALIAS: [TOK, ...], ... }, "min_len": int }
            try:
                fr = payload.get("fragments") if isinstance(payload, dict) else None
            except Exception:
                fr = None
            if isinstance(fr, dict) and fr:
                target = merged.setdefault("eq_like", {})
                for alias, toks in fr.items():
                    key = str(alias or "").strip().upper()
                    if not key:
                        continue
                    # normalize tokens -> uppercase unique list
                    vals = []
                    seen = set()
                    for t in (toks or []):
                        s = str(t or "").strip().upper()
                        if not s or s in seen:
                            continue
                        seen.add(s)
                        vals.append(s)
                    if not vals:
                        continue
                    if key in target and isinstance(target.get(key), list):
                        # extend de-duplicating
                        combined = list(target.get(key) or [])
                        for v in vals:
                            if v not in combined:
                                combined.append(v)
                        target[key] = combined
                    else:
                        target[key] = vals

        if merged:
            merged.setdefault("full_text_search", bool(merged.get("fts_tokens")))

    # Prefer-question merge for EQ filters
    question_eq_normalized = _normalize_eq_filters_list(intent_norm.get("eq_filters") or [])
    question_values = _build_question_value_map(intent_norm)
    eq_from_shape: List[List[Any]] = []
    if shape_rules:
        eq_from_shape = _apply_eq_shape(shape_rules, question_values)

    policy = _value_policy()
    eq_final: List[List[Any]] = []
    seen_cols: set[str] = set()

    for col, values in eq_from_shape:
        col_key = str(col or "").strip().upper()
        if not col_key:
            continue
        clean_vals = _normalize_value_list(values)
        if not clean_vals:
            continue
        eq_final.append([col_key, clean_vals])
        seen_cols.add(col_key)

    for col, values in question_eq_normalized:
        col_key = str(col or "").strip().upper()
        if not col_key or col_key in seen_cols:
            continue
        eq_final.append([col_key, values])
        seen_cols.add(col_key)

    if not eq_final:
        if eq_from_rules and policy != "question_only":
            eq_final = _merge_eq_filters_prefer_question(
                question_eq_normalized,
                eq_from_rules,
            )
        elif question_eq_normalized:
            eq_final = question_eq_normalized

    if eq_final:
        merged["eq_filters"] = eq_final
    try:
        log.info(
            {
                "event": "rules.merged",
                "kinds": sorted(set(kinds_found)),
                "has_eq": bool(merged.get("eq_filters")),
                "has_order": bool(merged.get("sort_by") or merged.get("order")),
                "has_fts": bool(merged.get("fts_tokens")),
            }
        )
    except Exception:
        pass
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
        
