"""DocuWare DW blueprint backed by a deterministic contract planner."""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - allow unit tests without Flask dependency
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - lightweight stub for tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def register_blueprint(self, *args, **kwargs):
            return None

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def get(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):  # pragma: no cover - stub
        return {}

    class _StubRequest:  # pragma: no cover - stub
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency during tests
    from sqlalchemy import text
except Exception:  # pragma: no cover - fallback for tests
    def text(sql: str):  # type: ignore
        return sql

from core.inquiries import create_or_update_inquiry

from apps.dw.rate_grammar import parse_rate_comment_strict
from apps.dw.lib.eq_ops import build_eq_where as build_eq_where_v2, parse_eq_from_text
from apps.dw.lib.fts_ops import build_fts_where as build_fts_where_v2, detect_fts_groups
from apps.dw.lib.sql_utils import direction_from_words, merge_where as merge_where_v2, order_by_safe
from apps.dw.common.debug_groups import build_boolean_debug
from apps.dw.rate_hints import (
    append_where,
    apply_rate_hints,
    parse_rate_hints,
    replace_or_add_order_by,
)
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.settings_utils import load_explicit_filter_columns
from apps.dw.tables.contracts import build_contract_sql
from apps.mem.kv import get_settings_for_namespace
from apps.dw.online_learning import load_recent_hints
from apps.dw.learning import load_rules_for_question
from apps.dw.builder import _where_from_eq_filters
from apps.dw.learning_store import (
    DWExample,
    DWPatch,
    DWRule,
    SessionLocal,
    get_similar_examples,
    init_db,
    list_metrics_summary,
    record_run,
)
from apps.dw.explain import build_explain
from .contracts.fts import extract_fts_terms, build_fts_where_groups
from .contracts.filters import parse_explicit_filters
from .contracts.contract_planner import plan_contract_query
from .rating import rate_bp

LOGGER = logging.getLogger("dw.app")


dw_bp = Blueprint("dw", __name__)
init_db()
dw_bp.register_blueprint(rate_bp, url_prefix="")


def _ns() -> str:
    return "dw::common"


def _coerce_debug_columns(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, (set, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, dict):
        collected: List[str] = []
        for candidate in value.values():
            collected.extend(_coerce_debug_columns(candidate))
        return collected
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _first_non_empty_text(values: List[Any]) -> str:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
        elif isinstance(value, (list, tuple)):
            nested = _first_non_empty_text(list(value))
            if nested:
                return nested
    return ""


def _extract_question_for_debug(payload: Dict[str, Any], response: Dict[str, Any]) -> str:
    candidates: List[Any] = []
    debug_section = response.get("debug")
    if isinstance(debug_section, dict):
        intent_section = debug_section.get("intent")
        if isinstance(intent_section, dict):
            candidates.extend(
                [
                    intent_section.get("question"),
                    intent_section.get("raw_question"),
                ]
            )
            notes_section = intent_section.get("notes")
            if isinstance(notes_section, dict):
                candidates.extend(
                    [
                        notes_section.get("q"),
                        notes_section.get("question"),
                        notes_section.get("raw_question"),
                    ]
                )
    meta_section = response.get("meta")
    if isinstance(meta_section, dict):
        candidates.extend([meta_section.get("question"), meta_section.get("raw_question")])
        clarifier = meta_section.get("clarifier_intent")
        if isinstance(clarifier, dict):
            candidates.extend(
                [
                    clarifier.get("question"),
                    clarifier.get("raw_question"),
                ]
            )
            clarifier_notes = clarifier.get("notes")
            if isinstance(clarifier_notes, dict):
                candidates.extend(
                    [
                        clarifier_notes.get("q"),
                        clarifier_notes.get("question"),
                    ]
                )
    if isinstance(payload, dict):
        candidates.extend([payload.get("question"), payload.get("q")])
    return _first_non_empty_text(candidates)


def _extract_fts_columns_for_debug(response: Dict[str, Any]) -> List[str]:
    candidates: List[Any] = []
    debug_section = response.get("debug")
    if isinstance(debug_section, dict):
        fts_section = debug_section.get("fts")
        if isinstance(fts_section, dict):
            candidates.append(fts_section.get("columns"))
        intent_section = debug_section.get("intent")
        if isinstance(intent_section, dict):
            intent_fts = intent_section.get("fts")
            if isinstance(intent_fts, dict):
                candidates.append(intent_fts.get("columns"))
    meta_section = response.get("meta")
    if isinstance(meta_section, dict):
        meta_fts = meta_section.get("fts")
        if isinstance(meta_fts, dict):
            candidates.append(meta_fts.get("columns"))
        clarifier = meta_section.get("clarifier_intent")
        if isinstance(clarifier, dict):
            candidates.append(clarifier.get("fts_columns"))
            clarifier_fts = clarifier.get("fts")
            if isinstance(clarifier_fts, dict):
                candidates.append(clarifier_fts.get("columns"))
    for candidate in candidates:
        columns = _coerce_debug_columns(candidate)
        if columns:
            return columns
    return []


def _respond(payload: Dict[str, Any], response: Dict[str, Any]):
    if not isinstance(response, dict):
        return jsonify(response)

    meta = response.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        response["meta"] = meta

    try:
        response["explain"] = build_explain(meta)
    except Exception:
        pass

    try:
        rows_field = response.get("rows")
        if isinstance(rows_field, list):
            rows_count = len(rows_field)
        else:
            rows_count = int(meta.get("rows") or rows_field or 0)
    except Exception:
        rows_count = 0

    try:
        duration = int(meta.get("duration_ms") or 0)
    except Exception:
        duration = 0

    try:
        record_run(
            namespace=_ns(),
            user_email=payload.get("auth_email"),
            question=payload.get("question"),
            sql=str(response.get("sql") or ""),
            ok=bool(response.get("ok")),
            duration_ms=duration,
            rows=rows_count,
            strategy=str(meta.get("strategy") or ""),
            explain=str(response.get("explain") or ""),
            meta=meta,
        )
    except Exception:
        pass

    debug_section = response.setdefault("debug", {}) if isinstance(response, dict) else {}
    if isinstance(debug_section, dict):
        fts_debug = debug_section.get("fts")
        if not isinstance(fts_debug, dict):
            fts_debug = {}
            debug_section["fts"] = fts_debug

        engine_value: Optional[str] = None
        raw_engine: Any = None
        try:
            settings_obj = get_settings()
        except Exception:  # pragma: no cover - defensive fallback
            settings_obj = None
        if isinstance(settings_obj, dict):
            raw_engine = settings_obj.get("DW_FTS_ENGINE")
        else:
            getter = getattr(settings_obj, "get", None)
            if callable(getter):
                try:
                    raw_engine = getter("DW_FTS_ENGINE")
                except TypeError:
                    raw_engine = getter("DW_FTS_ENGINE", None)
                except Exception:  # pragma: no cover - defensive fallback
                    raw_engine = None
            else:
                raw_engine = None
        if raw_engine:
            try:
                engine_value = str(raw_engine).strip() or None
            except Exception:  # pragma: no cover - defensive fallback
                engine_value = None
        if not engine_value:
            engine_value = fts_engine()
        if engine_value:
            fts_debug["engine"] = engine_value
        fts_debug.pop("error", None)

        intent_tokens: List[str] = []
        intent_section = debug_section.get("intent")
        if isinstance(intent_section, dict):
            raw_tokens: Any = intent_section.get("fts_tokens")
            if not raw_tokens:
                fts_section = intent_section.get("fts")
                if isinstance(fts_section, dict):
                    raw_tokens = fts_section.get("tokens")
            if isinstance(raw_tokens, (list, tuple, set)):
                intent_tokens = [
                    str(token).strip()
                    for token in raw_tokens
                    if str(token or "").strip()
                ]
            elif isinstance(raw_tokens, str) and raw_tokens.strip():
                intent_tokens = [raw_tokens.strip()]
        try:
            question_text = _extract_question_for_debug(payload if isinstance(payload, dict) else {}, response)
            fts_columns = _extract_fts_columns_for_debug(response)
            plan = build_boolean_debug(question_text, fts_columns)
            blocks = plan.get("blocks", [])
            summary_text = plan.get("summary", "") or ""
            debug_section["boolean_groups"] = blocks
            debug_section["boolean_groups_text"] = summary_text
            if blocks and isinstance(blocks[0], dict):
                first_block = blocks[0]
                block_fts = first_block.get("fts") if isinstance(first_block.get("fts"), list) else []
                if not block_fts and intent_tokens:
                    first_block["fts"] = intent_tokens
                    fts_text = "FTS(" + " OR ".join(intent_tokens) + ")"
                    if summary_text.startswith("(") and len(summary_text) > 1:
                        debug_section["boolean_groups_text"] = "(" + f"{fts_text} AND " + summary_text[1:]
                    else:
                        debug_section["boolean_groups_text"] = f"({fts_text})"
        except Exception as exc:  # pragma: no cover - debug best-effort
            debug_section["boolean_groups_error"] = str(exc)

    return jsonify(response)


def _ensure_engine():
    app = current_app
    if app is None:
        return None
    config = getattr(app, "config", {})
    getter = getattr(config, "get", None)
    engine = getter("DW_ENGINE") if callable(getter) else config.get("DW_ENGINE") if isinstance(config, dict) else None
    if engine is not None:
        return engine
    pipeline = None
    if callable(getter):
        pipeline = getter("PIPELINE") or getter("pipeline")
    elif isinstance(config, dict):
        pipeline = config.get("PIPELINE") or config.get("pipeline")
    if pipeline is None:
        return None
    try:
        return pipeline.ds.engine(None)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive fallback
        return getattr(pipeline, "app_engine", None)


def _ensure_date(val: Any) -> Any:
    """Return a datetime.date if the input looks like an ISO date; otherwise return as-is."""
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        text = val.strip()
        # Quick ISO-8601 check: 'YYYY-MM-DD'
        if len(text) == 10 and text[4] == '-' and text[7] == '-':
            try:
                return datetime.strptime(text, "%Y-%m-%d").date()
            except ValueError:
                # Not parseable as ISO date
                return val
    return val



def _coerce_oracle_binds(binds: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    b: Dict[str, Any] = dict(binds or {})

    def _to_date(v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            s = v.strip()
            # Fast path: YYYY-MM-DD
            try:
                return date.fromisoformat(s)
            except Exception:
                pass
            # Common fallbacks
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(s, fmt).date()
                except Exception:
                    continue
        return None

    for k in ("date_start", "date_end"):
        dv = _to_date(b.get(k))
        if dv is not None:
            b[k] = dv

    if "top_n" in b:
        try:
            b["top_n"] = int(b["top_n"])
        except Exception:
            b["top_n"] = 10
    return b


def _coerce_bind_dates(binds: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce date-like bind values to datetime.date objects."""
    out: Dict[str, Any] = {}
    for k, v in (binds or {}).items():
        if isinstance(v, (date, datetime)):
            out[k] = v.date() if isinstance(v, datetime) else v
        elif isinstance(v, str):
            out[k] = _ensure_date(v)
        else:
            out[k] = v
    return out

def _execute_oracle(sql: str, binds: Dict[str, Any]):
    engine = _ensure_engine()
    if engine is None:
        return [], [], {"rows": 0}
    # Normalize bind types first (prevents ORA-01861 and removes malformed try/except)
    safe_binds = _coerce_bind_dates(_coerce_oracle_binds(binds or {}))
    with engine.connect() as cx:  # type: ignore[union-attr]
        rs = cx.execute(text(sql), safe_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols, {"rows": len(rows)}


def _coerce_prefixes(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(p) for p in raw if p is not None]
    return [str(raw)]


def _dates_to_iso(explicit: Optional[Tuple[date, date]]) -> Optional[Dict[str, str]]:
    if not explicit:
        return None
    start, end = explicit
    return {"start": start.isoformat(), "end": end.isoformat()}


def _append_order_by(sql: str, column: str, *, descending: bool = True) -> str:
    """Ensure ``ORDER BY`` appears once, replacing an existing clause if needed."""

    if not column:
        return sql
    clause = f"ORDER BY {column} {'DESC' if descending else 'ASC'}"
    return replace_or_add_order_by(sql, clause)


def _resolve_contract_table(settings: Any, namespace: str, default: str = "Contract") -> str:
    table_name = default
    getter = getattr(settings, "get", None) if settings is not None else None
    if callable(getter):
        try:
            configured = getter("DW_CONTRACT_TABLE", scope="namespace", namespace=namespace)
        except TypeError:
            configured = getter("DW_CONTRACT_TABLE")
        if configured:
            table_name = str(configured)
    return table_name


def _json_safe_binds(binds: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {}
    for key, value in (binds or {}).items():
        if isinstance(value, (date, datetime)):
            safe[key] = value.isoformat()
        else:
            safe[key] = value
    return safe


_GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)


def _coalesce_rate_intent(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize hints coming from /dw/rate regardless of nesting."""

    if not isinstance(raw, dict):
        return {}

    sources: List[Dict[str, Any]] = []

    def _collect(obj: Optional[Dict[str, Any]]) -> None:
        if not isinstance(obj, dict):
            return
        sources.append(obj)
        for key in ("intent", "rate_hints"):
            candidate = obj.get(key)
            if isinstance(candidate, dict):
                _collect(candidate)

    _collect(raw)

    def _has_value(val: Any) -> bool:
        if val is None:
            return False
        if isinstance(val, bool):
            return True
        if isinstance(val, (int, float)):
            return True
        if isinstance(val, str):
            return bool(val.strip())
        if isinstance(val, (list, tuple, set, dict)):
            return bool(val)
        return True

    intent: Dict[str, Any] = {}
    merged_eq: List[Dict[str, Any]] = []

    for source in sources:
        if "eq_filters" in source and isinstance(source.get("eq_filters"), list):
            for entry in source["eq_filters"]:
                if isinstance(entry, dict):
                    merged_eq.append(dict(entry))
        for key in (
            "namespace",
            "full_text_search",
            "fts_tokens",
            "fts_columns",
            "fts_operator",
            "sort_by",
            "sort_desc",
            "group_by",
            "gross",
        ):
            if key not in source:
                continue
            value = source[key]
            if key in {"sort_desc", "gross", "full_text_search"}:
                if key not in intent and value is not None:
                    intent[key] = value
                elif key == "full_text_search" and bool(value):
                    intent[key] = True
                continue
            existing = intent.get(key)
            if not _has_value(existing) and _has_value(value):
                intent[key] = value
            elif key not in intent:
                intent[key] = value

    if merged_eq:
        intent["eq_filters"] = merged_eq

    for source in sources:
        fts = source.get("fts")
        if not isinstance(fts, dict):
            continue
        if not _has_value(intent.get("fts_tokens")) and _has_value(fts.get("tokens")):
            intent["fts_tokens"] = fts.get("tokens")
        if not _has_value(intent.get("fts_columns")) and _has_value(fts.get("columns")):
            intent["fts_columns"] = fts.get("columns")
        if "operator" in fts and not intent.get("fts_operator"):
            intent["fts_operator"] = fts.get("operator")
        if fts.get("enabled") and not intent.get("full_text_search"):
            intent["full_text_search"] = True

    tokens = intent.get("fts_tokens")
    if tokens and not intent.get("full_text_search"):
        intent["full_text_search"] = True

    if "sort_desc" in intent:
        intent["sort_desc"] = _coerce_bool_flag(intent.get("sort_desc"), default=True)
    if "gross" in intent:
        intent["gross"] = _coerce_bool_flag(intent.get("gross"))
    if "full_text_search" in intent:
        intent["full_text_search"] = _coerce_bool_flag(intent.get("full_text_search"), default=False)

    return intent


def _normalize_columns(columns: Sequence[Any]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for col in columns:
        if not isinstance(col, str):
            continue
        text = col.strip().strip('"')
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        seen.add(upper)
        normalized.append(upper)
    return normalized


def _normalize_token_groups(raw_tokens: Any) -> List[List[str]]:
    groups: List[List[str]] = []
    if raw_tokens is None:
        return groups
    tokens = raw_tokens if isinstance(raw_tokens, list) else [raw_tokens]
    for token in tokens:
        if isinstance(token, (list, tuple, set)):
            group = [str(t).strip() for t in token if str(t).strip()]
            if group:
                groups.append(group)
        else:
            text = str(token).strip()
            if text:
                groups.append([text])
    return groups


def _quote_column(col: str) -> str:
    cleaned = col.strip()
    if cleaned.startswith('"') and cleaned.endswith('"'):
        return cleaned
    if re.fullmatch(r"[A-Z0-9_]+", cleaned):
        return f'"{cleaned}"'
    return cleaned


def _build_rate_fts_where(
    columns: Sequence[str],
    token_groups: List[List[str]],
    *,
    operator: str,
    bind_prefix: str = "ol_fts",
) -> Tuple[str, Dict[str, Any]]:
    if not columns or not token_groups:
        return "", {}

    binds: Dict[str, Any] = {}
    pieces: List[str] = []
    bind_idx = 0

    def _column_predicate(col: str, bind_name: str) -> str:
        quoted = _quote_column(col)
        return f"UPPER(TRIM(NVL({quoted},''))) LIKE UPPER(:{bind_name})"

    for group in token_groups:
        if not group:
            continue
        group_parts: List[str] = []
        for token in group:
            bind_name = f"{bind_prefix}_{bind_idx}"
            bind_idx += 1
            binds[bind_name] = f"%{token}%"
            group_parts.append(
                "(" + " OR ".join(_column_predicate(col, bind_name) for col in columns) + ")"
            )
        if group_parts:
            pieces.append("(" + " AND ".join(group_parts) + ")")

    if not pieces:
        return "", {}

    top_op = "AND" if operator == "AND" else "OR"
    where_sql = "(" + f" {top_op} ".join(pieces) + ")"
    return where_sql, binds


def _strip_trailing_order_by(sql: str) -> str:
    return re.sub(r"\s+ORDER\s+BY[\s\S]*$", "", sql, flags=re.IGNORECASE).rstrip()


def _coerce_bool_flag(value: Any, *, default: Optional[bool] = None) -> Optional[bool]:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y", "on", "desc"}:
            return True
        if lowered in {"0", "false", "f", "no", "n", "off", "asc"}:
            return False
    return default


def _apply_online_rate_hints(
    sql: str,
    binds: Dict[str, Any],
    intent_patch: Dict[str, Any],
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    if not intent_patch:
        return sql, binds, meta

    patch: Dict[str, Any] = dict(intent_patch)
    intent = _coalesce_rate_intent(patch)
    combined_binds: Dict[str, Any] = {}
    where_clauses: List[str] = []

    eq_filters_raw = intent.get("eq_filters") or []
    deduped_filters: List[Dict[str, Any]] = []
    seen_filters: set[Tuple[Any, ...]] = set()
    for entry in eq_filters_raw:
        if not isinstance(entry, dict):
            continue
        normalized = dict(entry)
        key = (
            (normalized.get("col") or normalized.get("column") or "").strip().upper(),
            json.dumps(normalized.get("synonyms"), sort_keys=True)
            if isinstance(normalized.get("synonyms"), dict)
            else None,
            normalized.get("val"),
            normalized.get("op"),
            bool(normalized.get("ci")),
            bool(normalized.get("trim")),
        )
        if key in seen_filters:
            continue
        seen_filters.add(key)
        deduped_filters.append(normalized)

    eq_applied = False
    if deduped_filters:
        eq_temp_binds: Dict[str, Any] = {}
        eq_clause = _where_from_eq_filters(deduped_filters, eq_temp_binds)
        if eq_clause:
            rename_map: Dict[str, str] = {}
            for key in eq_temp_binds.keys():
                base = f"ol_{key}"
                new_key = base
                suffix = 1
                while new_key in binds or new_key in combined_binds or new_key in rename_map.values():
                    new_key = f"{base}_{suffix}"
                    suffix += 1
                rename_map[key] = new_key
            for old, new in rename_map.items():
                eq_clause = eq_clause.replace(f":{old}", f":{new}")
            renamed_binds = {rename_map[k]: v for k, v in eq_temp_binds.items()}
            combined_binds.update(renamed_binds)
            where_clauses.append(f"({eq_clause})")
            eq_applied = True

    namespace_hint = intent.get("namespace") or patch.get("namespace")
    namespace = namespace_hint if isinstance(namespace_hint, str) and namespace_hint.strip() else "dw::common"

    tokens_groups = _normalize_token_groups(intent.get("fts_tokens") or [])
    operator_raw = intent.get("fts_operator") or intent.get("fts_op") or "OR"
    operator = str(operator_raw).upper() if isinstance(operator_raw, str) else "OR"
    if operator not in {"AND", "OR"}:
        operator = "OR"

    columns = _normalize_columns(intent.get("fts_columns") or [])
    if (intent.get("full_text_search") or tokens_groups) and not columns:
        settings_obj = _get_settings()
        fts_map = _extract_fts_map(settings_obj, namespace)
        fallback = _resolve_fts_columns_from_map(fts_map, "Contract")
        columns = _normalize_columns(fallback)

    fts_error: str | None = None
    fts_meta_tokens = tokens_groups if tokens_groups else intent.get("fts_tokens") or []
    if (intent.get("full_text_search") or tokens_groups) and (not tokens_groups or not columns):
        fts_error = "missing_tokens" if not tokens_groups else "missing_columns"
    fts_clause = ""
    fts_temp_binds: Dict[str, Any] = {}
    if not fts_error and tokens_groups and columns:
        fts_clause, fts_temp_binds = _build_rate_fts_where(
            columns,
            tokens_groups,
            operator=operator,
            bind_prefix="ol_fts",
        )

    fts_meta = {
        "enabled": bool(fts_clause),
        "tokens": fts_meta_tokens,
        "columns": columns,
        "operator": operator,
        "binds": [],
        "error": fts_error,
    }

    if fts_clause:
        rename_map: Dict[str, str] = {}
        for key in fts_temp_binds.keys():
            new_key = key
            suffix = 1
            while new_key in binds or new_key in combined_binds or new_key in rename_map.values():
                new_key = f"{key}_{suffix}"
                suffix += 1
            rename_map[key] = new_key
        for old, new in rename_map.items():
            fts_clause = fts_clause.replace(f":{old}", f":{new}")
        renamed = {rename_map.get(k, k): v for k, v in fts_temp_binds.items()}
        combined_binds.update(renamed)
        where_clauses.append(fts_clause)
        fts_meta["enabled"] = True
        fts_meta["binds"] = list(renamed.keys())
        fts_meta["error"] = None

    if where_clauses:
        sql = append_where(sql, " AND ".join(where_clauses))

    if combined_binds:
        binds.update(combined_binds)

    group_by_raw = intent.get("group_by")
    group_by_clause = ""
    if isinstance(group_by_raw, (list, tuple, set)):
        group_items = [str(item).strip() for item in group_by_raw if str(item).strip()]
        group_by_clause = ", ".join(group_items)
    elif isinstance(group_by_raw, str) and group_by_raw.strip():
        group_by_clause = group_by_raw.strip()

    gross_flag = intent.get("gross")
    if group_by_clause:
        sql = _strip_trailing_order_by(sql)
        inner = sql.strip()
        measure_sql = "COUNT(*) AS CNT"
        if gross_flag is True:
            measure_sql = f"SUM({_GROSS_EXPR}) AS TOTAL_GROSS"
        sql = (
            "SELECT "
            + group_by_clause
            + (", " if measure_sql else "")
            + measure_sql
            + "\nFROM (\n"
            + inner
            + "\n) RATE_WRAP\nGROUP BY "
            + group_by_clause
        )
        meta["group_by"] = group_by_clause
        if gross_flag is not None:
            meta["gross"] = bool(gross_flag)

    sort_by = intent.get("sort_by")
    if isinstance(sort_by, str) and sort_by.strip():
        sort_desc = _coerce_bool_flag(intent.get("sort_desc"), default=True)
        clause = f"ORDER BY {sort_by.strip()} {'DESC' if sort_desc else 'ASC'}"
        sql = replace_or_add_order_by(sql, clause)
        meta["order_by"] = clause

    meta["eq_filters"] = eq_applied
    meta["fts"] = fts_meta
    return sql, binds, meta


def _plan_contract_sql(
    question: str,
    namespace: str,
    *,
    today: date | None = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    settings = get_settings_for_namespace(namespace)
    sql, binds, meta = build_contract_sql(
        question,
        settings or {},
        today=today,
        overrides=overrides or {},
    )
    return sql, binds, meta


def derive_sql_for_test(
    question: str,
    namespace: str = "dw::common",
    test_binds: dict | None = None,
    rate_comment: str | None = None,
):
    """Produce SQL (without execution) for a natural-language question.
    Used by golden tests; merges deterministic planner binds with optional overrides."""
    sql: str = ""
    binds: Dict[str, Any] = {}
    try:
        sql, base_binds, _ = _plan_contract_sql(question, namespace, today=date.today())
        binds.update(base_binds or {})
    except Exception:  # pragma: no cover - defensive fallback for optional planner
        sql = ""

    if not sql:
        explicit_dates = _resolve_window(question)
        top_n = _extract_top_n(question)
        fts_columns = _get_fts_columns(table="Contract", namespace=namespace)
        sql, planner_binds, _, _ = plan_contract_query(
            question,
            explicit_dates=explicit_dates,
            top_n=top_n,
            payload={"full_text_search": False},
            settings={"DW_FTS_COLUMNS": {}},
            fts_columns=fts_columns,
        )
        binds.update(planner_binds or {})

    if sql and ":top_n" in sql and "top_n" not in binds:
        binds["top_n"] = 10

    if test_binds:
        binds.update(test_binds)

    if sql and rate_comment and rate_comment.strip():
        settings_obj = _get_settings()
        getter = None
        if settings_obj is not None:
            getter = getattr(settings_obj, "get_json", None) or getattr(settings_obj, "get", None)
        allowed_cols = load_explicit_filter_columns(
            getter, namespace, DEFAULT_EXPLICIT_FILTER_COLUMNS
        )
        strict_hints = parse_rate_comment_strict(rate_comment)
        if strict_hints.filters:
            allowed_map = {col.upper(): col.upper() for col in allowed_cols}
            extra_where: List[str] = []
            for idx, filt in enumerate(strict_hints.filters):
                canonical = allowed_map.get(filt.col.upper())
                if not canonical:
                    continue
                safe_col = re.sub(r"[^A-Z0-9]+", "_", canonical)
                bind_name = f"rh_eq_{safe_col}_{idx}"
                value = filt.value.strip() if filt.trim and isinstance(filt.value, str) else filt.value
                binds[bind_name] = value
                lhs = canonical
                if filt.trim:
                    lhs = f"TRIM({lhs})"
                if filt.ci:
                    lhs = f"UPPER({lhs})"
                rhs = f":{bind_name}"
                if filt.ci:
                    rhs = f"UPPER({rhs})"
                if filt.trim:
                    rhs = f"TRIM({rhs})"
                extra_where.append(f"{lhs} = {rhs}")
            if extra_where:
                sql = append_where(sql, " AND ".join(extra_where))

        hints = parse_rate_hints(rate_comment, getter)
        if hints.where_sql:
            sql = append_where(sql, hints.where_sql)
            binds.update(hints.where_binds)
        if hints.order_by_sql:
            sql = replace_or_add_order_by(sql, hints.order_by_sql)
        elif strict_hints.order_by:
            first = strict_hints.order_by[0]
            clause = f"ORDER BY {first.expr} {'DESC' if first.desc else 'ASC'}"
            sql = replace_or_add_order_by(sql, clause)

    return sql, _coerce_bind_dates(binds)


def _ensure_oracle_date(value: Optional[Any]) -> Optional[date]:
    coerced = _ensure_date(value)
    if isinstance(coerced, date) and not isinstance(coerced, datetime):
        return coerced
    return None


def _infer_window_column(question: str) -> str:
    ql = (question or "").lower()
    if any(word in ql for word in ("expire", "expired", "expiring", "termination", "ended")):
        return "END_DATE"
    if "start" in ql and "date" in ql:
        return "START_DATE"
    if "request" in ql:
        return "REQUEST_DATE"
    return "REQUEST_DATE"


_LAST_DAYS_RE = re.compile(r"last\s+(\d+)\s+day", re.IGNORECASE)
_NEXT_DAYS_RE = re.compile(r"(?:next|in)\s+(\d+)\s+day", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(?:in|for|during)\s+(20\d{2})\b")
_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.IGNORECASE)
_TEXTUAL_TOP = {
    "ten": 10,
    "five": 5,
    "three": 3,
    "twenty": 20,
    "twenty five": 25,
    "thirty": 30,
}


def _resolve_window(question: str) -> Optional[Tuple[date, date]]:
    q = (question or "").lower()
    today = date.today()

    if "last month" in q or "previous month" in q:
        first_this = today.replace(day=1)
        last_month_end = first_this - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start, last_month_end

    if "last quarter" in q:
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        prev_start_month = quarter_start_month - 3
        prev_year = today.year
        if prev_start_month < 1:
            prev_start_month += 12
            prev_year -= 1
        start = date(prev_year, prev_start_month, 1)
        next_month = prev_start_month + 3
        next_year = prev_year
        if next_month > 12:
            next_month -= 12
            next_year += 1
        end = date(next_year, next_month, 1) - timedelta(days=1)
        return start, end

    match = _LAST_DAYS_RE.search(q)
    if match:
        days = int(match.group(1))
        if days > 0:
            end = today
            start = today - timedelta(days=days)
            return start, end

    match = _NEXT_DAYS_RE.search(q)
    if match:
        days = int(match.group(1))
        if days > 0:
            start = today
            end = today + timedelta(days=days)
            return start, end

    if "this year" in q:
        start = date(today.year, 1, 1)
        end = date(today.year, 12, 31)
        return start, end

    match = _YEAR_RE.search(q)
    if match:
        year = int(match.group(1))
        if 2000 <= year <= 2100:
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            return start, end

    return None


def _get_pipeline():
    app = current_app
    if app is None:
        return None
    config = getattr(app, "config", {})
    getter = getattr(config, "get", None)
    if callable(getter):
        return getter("PIPELINE") or getter("pipeline")
    if isinstance(config, dict):
        return config.get("PIPELINE") or config.get("pipeline")
    return None


def _get_settings():
    pipeline = _get_pipeline()
    if pipeline is None:
        return None
    return getattr(pipeline, "settings", None)


def _get_namespace_setting(settings_obj: Any, namespace: str, key: str, default: Any = None) -> Any:
    """Fetch a namespaced setting using ``get_json``/``get`` fallbacks."""

    if settings_obj is None:
        return default

    for attr in ("get_json", "get"):
        getter = getattr(settings_obj, attr, None)
        if not callable(getter):
            continue
        try:
            value = getter(key, scope="namespace", namespace=namespace)
        except TypeError:
            value = getter(key)
        except Exception:
            continue
        if value is not None:
            return value
    return default


def _extract_fts_map(settings_obj: Any, namespace: str) -> Dict[str, Any]:
    if settings_obj is None:
        return {}
    
    def _coerce(value: Any) -> Optional[Dict[str, Any]]:
        if isinstance(value, dict):
            return value
        if isinstance(value, (list, tuple, set)):
            return {"*": list(value)}
        return None

    getter = getattr(settings_obj, "get_fts_columns", None)
    if callable(getter):
        try:
            value = getter(namespace)  # type: ignore[arg-type]
            coerced = _coerce(value)
            if coerced is not None:
                return coerced
        except TypeError:
            pass
    json_getter = getattr(settings_obj, "get_json", None)
    if callable(json_getter):
        try:
            value = json_getter("DW_FTS_COLUMNS", scope="namespace", namespace=namespace)
        except TypeError:
            value = json_getter("DW_FTS_COLUMNS")
        coerced = _coerce(value)
        if coerced is not None:
            return coerced
    plain_get = getattr(settings_obj, "get", None)
    if callable(plain_get):
        try:
            value = plain_get("DW_FTS_COLUMNS", scope="namespace", namespace=namespace)
        except TypeError:
            value = plain_get("DW_FTS_COLUMNS")
        coerced = _coerce(value)
        if coerced is not None:
            return coerced
    return {}


def _resolve_fts_columns_from_map(fts_map: Dict[str, Any], table: str) -> List[str]:
    """Return normalized FTS columns for ``table`` with sensible fallbacks."""

    def _normalize(cols: List[str]) -> List[str]:
        seen: set[str] = set()
        normalized: List[str] = []
        for col in cols:
            text = str(col).strip().strip('"')
            if not text:
                continue
            upper = text.upper()
            if upper in seen:
                continue
            seen.add(upper)
            normalized.append(f'"{upper}"')
        return normalized

    if isinstance(fts_map, dict):
        def _coerce(raw: Any) -> List[str]:
            if isinstance(raw, dict):
                for key in ("columns", "values", "cols"):
                    if key in raw:
                        return _coerce(raw.get(key))
                return []
            if isinstance(raw, (list, tuple, set)):
                return [str(item) for item in raw if str(item).strip()]
            if isinstance(raw, str):
                return [part.strip() for part in raw.split(",") if part.strip()]
            return []

        normalized_table = table.strip('"')
        lookup_keys = [
            table,
            normalized_table,
            normalized_table.upper(),
            normalized_table.lower(),
            f'"{normalized_table}"',
            "*",
        ]
        for key in lookup_keys:
            if key not in fts_map:
                continue
            cols = _normalize(_coerce(fts_map.get(key)))
            if cols:
                return cols

    if isinstance(fts_map, (list, tuple, set)):
        return _normalize([str(item) for item in fts_map if str(item).strip()])

    return _normalize(list(DEFAULT_CONTRACT_FTS_COLUMNS))


def _get_fts_columns(*, table: str, namespace: str) -> List[str]:
    settings = _get_settings()
    fts_map = _extract_fts_map(settings, namespace)
    return _resolve_fts_columns_from_map(fts_map, table)


def _extract_top_n(question: str) -> Optional[int]:
    match = _TOP_RE.search(question or "")
    if match:
        try:
            return max(1, min(int(match.group(1)), 500))
        except ValueError:
            pass
    lowered = (question or "").lower()
    for phrase, number in _TEXTUAL_TOP.items():
        if f"top {phrase}" in lowered:
            return number
    return None


def _coerce_datasource(pipeline, body: Dict[str, Any]) -> str:
    if pipeline is None:
        return body.get("datasource") or "default"
    datasource = body.get("datasource")
    if datasource:
        return str(datasource)
    default = None
    settings = getattr(pipeline, "settings", None)
    getter = getattr(settings, "default_datasource", None)
    if callable(getter):
        default = getter("dw::common")
    if not default:
        default = getattr(pipeline, "default_ds", None)
    return default or "default"


def _log_inquiry(
    question: str,
    auth_email: Optional[str],
    *,
    status: str,
    rows: int,
    prefixes: Sequence[str],
    payload: Dict[str, Any],
) -> Optional[int]:
    pipeline = _get_pipeline()
    if pipeline is None:
        return None
    mem_engine = getattr(pipeline, "mem_engine", None)
    if mem_engine is None:
        return None
    try:
        datasource = _coerce_datasource(pipeline, payload)
        inquiry_id = create_or_update_inquiry(
            mem_engine,
            namespace="dw::common",
            prefixes=list(prefixes),
            question=question,
            auth_email=auth_email,
            run_id=None,
            research_enabled=False,
            datasource=datasource,
            status=status,
        )
        if rows >= 0:
            return inquiry_id
        return inquiry_id
    except Exception as exc:  # pragma: no cover - logging guard
        LOGGER.warning("[dw] failed to log inquiry: %s", exc)
        return None


def _attempt_like_eq_fallback(
    *,
    question: str,
    namespace: str,
    table_name: str,
    settings: Any,
    fts_columns: List[str],
    allowed_columns: List[str],
    full_text_search: bool,
    payload: Dict[str, Any],
    prefixes: Sequence[str],
    auth_email: Optional[str],
    t0: float,
    online_hints_applied: int,
    online_intent: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    question_text = question or ""
    lowered = question_text.lower()
    implied_fts = bool(
        re.search(r"\bhas\b", lowered)
        or re.search(r"\bcontain", lowered)
        or re.search(r"\binclude", lowered)
    )

    columns = [str(col).strip().upper() for col in (fts_columns or []) if str(col).strip()]
    allowed = [str(col).strip().upper() for col in (allowed_columns or []) if str(col).strip()]

    engine_value = _get_namespace_setting(settings, namespace, "DW_FTS_ENGINE", "like")
    synonyms_setting = _get_namespace_setting(settings, namespace, "DW_ENUM_SYNONYMS", {})
    settings_bundle: Dict[str, Any] = {
        "DW_FTS_COLUMNS": {"value": {"Contract": columns}},
        "DW_FTS_ENGINE": {"value": str(engine_value or "like")},
        "DW_EXPLICIT_FILTER_COLUMNS": {"value": allowed},
    }
    if synonyms_setting:
        settings_bundle["DW_ENUM_SYNONYMS"] = {"value": synonyms_setting}

    eq_filters_v2 = parse_eq_from_text(question_text, settings_bundle)

    tokens_groups: List[List[str]] = []
    operator_between_groups = "OR"
    if full_text_search or implied_fts:
        tokens_groups, operator_between_groups = detect_fts_groups(question_text)

    if not tokens_groups and not eq_filters_v2:
        return None

    fts_sql = ""
    fts_binds: Dict[str, Any] = {}
    fts_debug: Dict[str, Any] = {"enabled": False, "error": None, "columns": columns, "groups": []}
    if tokens_groups:
        fts_sql, fts_binds, fts_debug = build_fts_where_v2(settings_bundle, tokens_groups, operator_between_groups)
        fts_debug.setdefault("groups", tokens_groups)
        fts_debug["operator"] = operator_between_groups
    else:
        fts_debug["operator"] = operator_between_groups

    eq_sql = ""
    eq_binds: Dict[str, Any] = {}
    if eq_filters_v2:
        eq_sql, eq_binds = build_eq_where_v2(eq_filters_v2, settings_bundle, bind_prefix="eq")

    if not fts_sql and not eq_sql:
        return None

    where_sql = merge_where_v2([fts_sql, eq_sql])

    binds: Dict[str, Any] = {}
    binds.update(fts_binds)
    binds.update(eq_binds)

    base_sql = f'SELECT * FROM "{table_name}"'
    sql = base_sql + ("\n" + where_sql if where_sql else "")
    order_dir = direction_from_words(question_text, "DESC")
    sql = order_by_safe(sql, f"ORDER BY REQUEST_DATE {order_dir}")

    sanitized_patch = {
        key: value
        for key, value in online_intent.items()
        if key not in {"fts_tokens", "fts_columns", "fts_operator", "fts_op", "full_text_search"}
    }
    online_meta: Dict[str, Any] = {}
    if sanitized_patch:
        sql, binds, online_meta = _apply_online_rate_hints(sql, binds, sanitized_patch)

    binds = _coerce_bind_dates(binds)
    rows, cols, exec_meta = _execute_oracle(sql, binds)

    inquiry_id = _log_inquiry(
        question,
        auth_email,
        status="answered",
        rows=len(rows),
        prefixes=prefixes,
        payload=payload,
    )

    duration_ms = int((time.time() - t0) * 1000)
    meta: Dict[str, Any] = {
        "strategy": "fts_like_fallback",
        "binds": _json_safe_binds(binds),
        **exec_meta,
        "duration_ms": duration_ms,
        "online_learning": {"hints": online_hints_applied, **online_meta},
        "fts": fts_debug,
    }
    if eq_filters_v2:
        meta["eq_filters"] = eq_filters_v2

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "rows": rows,
        "columns": cols,
        "sql": sql,
        "meta": meta,
        "debug": {
            "fts": meta["fts"],
            "like_fallback": {
                "tokens": tokens_groups,
                "operator": operator_between_groups,
                "eq_filters": eq_filters_v2,
            },
            "online_learning": meta.get("online_learning"),
        },
    }
    return response


@dw_bp.post("/answer")
def answer():
    t0 = time.time()
    payload = request.get_json(force=True, silent=False) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    settings = _get_settings()
    online_intent: Dict[str, Any] = {}
    online_hints_applied = 0
    pipeline = _get_pipeline()
    mem_engine = getattr(pipeline, "mem_engine", None) if pipeline else None
    learned_hints = {}
    try:
        learned_hints = load_rules_for_question(mem_engine, question)
        if learned_hints:
            apply_rate_hints(online_intent, learned_hints, settings)
            online_hints_applied += 1
    except Exception as exc:
        LOGGER.warning("[dw] failed to load persisted rules: %s", exc)
    try:
        recent_hints = load_recent_hints(question, ttl_seconds=900)
        online_hints_applied += len(recent_hints)
        for hint in recent_hints:
            apply_rate_hints(online_intent, hint, settings)
    except Exception as exc:
        LOGGER.warning("[dw] failed to load online hints: %s", exc)
        online_hints_applied = 1 if learned_hints else 0

    prefixes = _coerce_prefixes(payload.get("prefixes"))
    auth_email = payload.get("auth_email") or None
    full_text_search = bool(payload.get("full_text_search", False))
    overrides = {"full_text_search": full_text_search}

    namespace = (payload.get("namespace") or "dw::common").strip() or "dw::common"
    table_name = _resolve_contract_table(settings, namespace)
    initial_getter = getattr(settings, "get_json", None) or getattr(settings, "get", None)
    allowed_columns_initial = load_explicit_filter_columns(
        initial_getter,
        namespace,
        DEFAULT_EXPLICIT_FILTER_COLUMNS,
    )
    fts_map_initial = _extract_fts_map(settings, namespace)
    fts_columns_initial = _resolve_fts_columns_from_map(fts_map_initial, table_name)
    if full_text_search:
        direct_groups, direct_mode = extract_fts_terms(question, force=False)
        if direct_mode == "explicit" and direct_groups and fts_columns_initial:
            direct_where, direct_binds = build_fts_where_groups(direct_groups, fts_columns_initial)
            if direct_where:
                direct_sql = f'SELECT * FROM "{table_name}"\nWHERE {direct_where}'
            else:
                direct_sql = f'SELECT * FROM "{table_name}"'
            direct_sql = _append_order_by(direct_sql, "REQUEST_DATE", descending=True)

            sanitized_patch = {
                key: value
                for key, value in online_intent.items()
                if key not in {"fts_tokens", "fts_columns", "fts_operator", "fts_op", "full_text_search"}
            }
            binds = dict(direct_binds)
            online_meta: Dict[str, Any] = {}
            if sanitized_patch:
                direct_sql, binds, online_meta = _apply_online_rate_hints(
                    direct_sql, binds, sanitized_patch
                )

            binds = _coerce_bind_dates(binds)
            rows, cols, exec_meta = _execute_oracle(direct_sql, binds)
            inquiry_id = _log_inquiry(
                question,
                auth_email,
                status="answered",
                rows=len(rows),
                prefixes=prefixes,
                payload=payload,
            )
            duration_ms = int((time.time() - t0) * 1000)
            meta = {
                "strategy": "fts_direct",
                "binds": _json_safe_binds(binds),
                **exec_meta,
                "duration_ms": duration_ms,
                "online_learning": {"hints": online_hints_applied, **online_meta},
                "fts": {
                    "enabled": True,
                    "mode": direct_mode,
                    "tokens": direct_groups,
                    "columns": fts_columns_initial,
                    "binds": list(binds.keys()),
                    "error": None,
                },
            }
            response = {
                "ok": True,
                "inquiry_id": inquiry_id,
                "rows": rows,
                "columns": cols,
                "sql": direct_sql,
                "meta": meta,
                "debug": {
                    "fts": meta["fts"],
                    "online_learning": meta.get("online_learning"),
                },
            }
            return _respond(payload, response)

    contract_sql, contract_binds, contract_meta = _plan_contract_sql(
        question,
        namespace,
        today=date.today(),
        overrides=overrides,
    )
    if contract_sql:
        binds = _coerce_bind_dates(dict(contract_binds or {}))
        contract_sql, binds, online_meta = _apply_online_rate_hints(
            contract_sql, binds, online_intent
        )
        if ":top_n" in contract_sql and "top_n" not in binds:
            binds["top_n"] = 10
        rows, cols, exec_meta = _execute_oracle(contract_sql, binds)
        inquiry_id = _log_inquiry(
            question,
            auth_email,
            status="answered",
            rows=len(rows),
            prefixes=prefixes,
            payload=payload,
        )
        duration_ms = int((time.time() - t0) * 1000)
        response = {
            "ok": True,
            "inquiry_id": inquiry_id,
            "rows": rows,
            "columns": cols,
            "sql": contract_sql,
            "meta": {
                "strategy": "contract_deterministic",
                "binds": _json_safe_binds(binds),
                **(contract_meta or {}),
                **exec_meta,
                "duration_ms": duration_ms,
                "online_learning": {
                    "hints": online_hints_applied,
                    **({} if not online_meta else online_meta),
                },
            },
            "explain": (contract_meta or {}).get("explain"),
            "debug": {"contract_planner": True},
        }
        if response["meta"].get("online_learning", {}).get("fts"):
            response["meta"]["fts"] = response["meta"]["online_learning"]["fts"]
        return _respond(payload, response)

    like_response = _attempt_like_eq_fallback(
        question=question,
        namespace=namespace,
        table_name=table_name,
        settings=settings,
        fts_columns=fts_columns_initial,
        allowed_columns=allowed_columns_initial,
        full_text_search=full_text_search,
        payload=payload,
        prefixes=prefixes,
        auth_email=auth_email,
        t0=t0,
        online_hints_applied=online_hints_applied,
        online_intent=online_intent,
    )
    if like_response is not None:
        return _respond(payload, like_response)

    namespace = "dw::common"

    getter = getattr(settings, "get", None) if settings is not None else None
    table_name = _resolve_contract_table(settings, namespace)

    explicit_dates = _resolve_window(question)

    allowed_columns = load_explicit_filter_columns(
        getattr(settings, "get_json", None) or getter,
        namespace,
        DEFAULT_EXPLICIT_FILTER_COLUMNS,
    )

    fts_map = _extract_fts_map(settings, namespace)
    fts_columns = _resolve_fts_columns_from_map(fts_map, table_name)
    fts_groups, fts_mode = extract_fts_terms(question, force=full_text_search)
    fts_where_sql, fts_binds = ("", {})
    if fts_groups and fts_columns:
        fts_where_sql, fts_binds = build_fts_where_groups(fts_groups, fts_columns)

    top_n = payload.get("top_n")
    if top_n is None:
        top_n = _extract_top_n(question)
    elif isinstance(top_n, str) and top_n.isdigit():
        top_n = int(top_n)

    explicit_snips, explicit_binds = parse_explicit_filters(question, allowed_columns)
    if explicit_snips:
        where_clauses = list(explicit_snips)
        binds: Dict[str, Any] = dict(explicit_binds)
        explain_bits = ["Applied explicit column filters from the question (took precedence over defaults)."]

        if explicit_dates:
            ds = _ensure_oracle_date(explicit_dates[0])
            de = _ensure_oracle_date(explicit_dates[1])
            if ds and de:
                date_col = _infer_window_column(question)
                binds["date_start"] = ds
                binds["date_end"] = de
                where_clauses.append(f"{date_col} BETWEEN :date_start AND :date_end")
                explain_bits.append(f"Used date window {ds} .. {de} on {date_col}.")

        if fts_where_sql:
            where_clauses.append(fts_where_sql)
            explain_bits.append(
                "Applied full-text search for tokens "
                + ", ".join([" AND ".join(group) for group in fts_groups])
                + "."
            )
            binds.update(fts_binds)

        sql = f'SELECT * FROM "{table_name}"'
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        sql = _append_order_by(sql, order_by, descending=True)
        if top_n:
            binds["top_n"] = int(top_n)
            sql += " FETCH FIRST :top_n ROWS ONLY"
            explain_bits.append(f"Limited to top {int(top_n)} rows.")

        LOGGER.info("[dw] explicit_filters_sql: %s", {"size": len(sql), "sql": sql})
        binds = _coerce_bind_dates(binds)
        rows, cols, exec_meta = _execute_oracle(sql, binds)

        inquiry_id = _log_inquiry(
            question,
            auth_email,
            status="answered",
            rows=len(rows),
            prefixes=prefixes,
            payload=payload,
        )

        duration_ms = int((time.time() - t0) * 1000)
        meta = {
            "strategy": "explicit_filters",
            "explicit_filters": True,
            "binds": _json_safe_binds(binds),
            **exec_meta,
            "duration_ms": duration_ms,
        }
        meta["fts"] = {
            "enabled": bool(fts_where_sql),
            "mode": fts_mode,
            "tokens": fts_groups if fts_where_sql else None,
            "columns": fts_columns if fts_where_sql else None,
            "binds": list(fts_binds.keys()) if fts_where_sql else None,
            "error": None,
        }
        response = {
            "ok": True,
            "inquiry_id": inquiry_id,
            "rows": rows,
            "columns": cols,
            "sql": sql,
            "meta": meta,
            "explain": " ".join(explain_bits),
            "debug": {
                "explicit_filter_mode": True,
                "intent": {
                    "explicit_dates": _dates_to_iso(explicit_dates),
                    "top_n": top_n,
                    "full_text_search": full_text_search,
                    "fts": {
                        "mode": fts_mode,
                        "tokens": fts_groups,
                        "columns": fts_columns,
                    },
                },
            },
        }
        return _respond(payload, response)

    planner_settings = {"DW_FTS_COLUMNS": fts_map} if isinstance(fts_map, dict) else {}
    sql, binds, meta, explain = plan_contract_query(
        question,
        explicit_dates=explicit_dates,
        top_n=top_n,
        payload=payload,
        settings=planner_settings,
        fts_columns=fts_columns,
    )

    LOGGER.info("[dw] final_sql: %s", {"size": len(sql), "sql": sql})
    sql, binds, online_meta = _apply_online_rate_hints(sql, binds or {}, online_intent)
    binds = _coerce_bind_dates(binds or {})
    rows, cols, exec_meta = _execute_oracle(sql, binds)

    inquiry_id = _log_inquiry(
        question,
        auth_email,
        status="answered",
        rows=len(rows),
        prefixes=prefixes,
        payload=payload,
    )

    duration_ms = int((time.time() - t0) * 1000)
    meta_out: Dict[str, Any] = {
        **(meta or {}),
        **exec_meta,
        "duration_ms": duration_ms,
        "explicit_filters": False,
        "online_learning": {
            "hints": online_hints_applied,
            **({} if not online_meta else online_meta),
        },
    }
    if isinstance(online_meta, dict) and online_meta.get("fts"):
        meta_out["fts"] = online_meta["fts"]
    if "binds" not in meta_out:
        meta_out["binds"] = _json_safe_binds(binds or {})
    meta_fts = (meta_out or {}).get("fts") if isinstance(meta_out, dict) else None
    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "rows": rows,
        "columns": cols,
        "sql": sql,
        "meta": meta_out,
        "explain": explain,
        "debug": {
            "intent": {
                "explicit_dates": _dates_to_iso(explicit_dates),
                "top_n": top_n,
                "full_text_search": full_text_search,
                "fts": {
                    "mode": fts_mode,
                    "tokens": fts_groups,
                    "columns": fts_columns,
                },
            }
        },
    }
    if isinstance(response.get("debug"), dict):
        response["debug"]["fts"] = {
            "enabled": bool(meta_fts.get("enabled")) if isinstance(meta_fts, dict) else False,
            "mode": meta_fts.get("mode") if isinstance(meta_fts, dict) else None,
            "tokens": meta_fts.get("tokens") if isinstance(meta_fts, dict) else [],
            "columns": meta_fts.get("columns") if isinstance(meta_fts, dict) else [],
            "binds": meta_fts.get("binds") if isinstance(meta_fts, dict) else None,
            "error": meta_fts.get("error") if isinstance(meta_fts, dict) else None,
        }
        fts_debug = response["debug"].get("fts")
        if isinstance(fts_debug, dict):
            engine_value = fts_engine()
            if engine_value:
                fts_debug["engine"] = engine_value
            if engine_value == "like" and fts_debug.get("error") == "no_engine":
                fts_debug.pop("error", None)
        response["debug"]["online_learning"] = {
            "hints": online_hints_applied,
            **({} if not online_meta else online_meta),
        }
    return _respond(payload, response)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp

# --- Admin JSON endpoints (MVP) ---


@dw_bp.route("/admin/dw/metrics", methods=["GET"])
def dw_metrics():
    try:
        return jsonify({"ok": True, "metrics_24h": list_metrics_summary(24)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@dw_bp.route("/admin/dw/examples", methods=["GET"])
def dw_examples():
    namespace = request.args.get("namespace") or _ns()
    question = request.args.get("question")
    if question:
        matches = get_similar_examples(namespace, question, limit=10)
        data = [
            {
                "id": match.id,
                "q": match.raw_question,
                "sql": match.sql,
                "tags": match.tags,
                "created_at": match.created_at.isoformat() if match.created_at else None,
                "success_count": match.success_count,
            }
            for match in matches
        ]
        return jsonify({"ok": True, "examples": data, "mode": "similar"})

    with SessionLocal() as session:
        rows = (
            session.query(DWExample)
            .filter_by(namespace=namespace)
            .order_by(DWExample.id.desc())
            .limit(200)
            .all()
        )
        data = [
            {
                "id": row.id,
                "q": row.raw_question,
                "sql": row.sql,
                "tags": row.tags,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "success_count": row.success_count,
            }
            for row in rows
        ]
    return jsonify({"ok": True, "examples": data, "mode": "recent"})


@dw_bp.route("/admin/dw/patches", methods=["GET"])
def dw_patches():
    namespace = request.args.get("namespace") or _ns()
    with SessionLocal() as session:
        rows = (
            session.query(DWPatch)
            .filter_by(namespace=namespace)
            .order_by(DWPatch.id.desc())
            .limit(200)
            .all()
        )
        data = [
            {
                "id": row.id,
                "status": row.status,
                "comment": row.comment,
                "patch_intent": row.patch_intent,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "applied_now": row.applied_now,
                "user_email": row.user_email,
                "inquiry_id": row.inquiry_id,
            }
            for row in rows
        ]
    return jsonify({"ok": True, "patches": data})


@dw_bp.route("/admin/dw/rules", methods=["GET", "POST"])
def dw_rules():
    with SessionLocal() as session:
        if request.method == "POST":
            payload = request.get_json(force=True) or {}
            rule_id = int(payload.get("id") or 0)
            action = (payload.get("action") or "").lower()
            rule = session.query(DWRule).filter_by(id=rule_id).first()
            if not rule:
                return jsonify({"ok": False, "error": "rule_not_found"}), 404
            if action in {"approve", "activate"}:
                rule.status = "active"
                rule.approved_at = datetime.utcnow()
            elif action in {"disable", "reject"}:
                rule.status = "disabled"
            elif action == "canary":
                rule.status = "canary"
                if payload.get("canary_percent") is not None:
                    try:
                        rule.canary_percent = int(payload.get("canary_percent"))
                    except Exception:
                        pass
            session.commit()

        rows = session.query(DWRule).order_by(DWRule.id.desc()).limit(200).all()
        data = [
            {
                "id": row.id,
                "name": row.name,
                "status": row.status,
                "version": row.version,
                "canary_percent": row.canary_percent,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ]
    return jsonify({"ok": True, "rules": data})

# ensure FTS engine check and default
from apps.dw.settings import get_setting, get_settings

def fts_engine():
    eng = (get_setting("DW_FTS_ENGINE", scope="namespace") or "like")
    try:
        eng = eng.lower()
    except Exception:
        eng = "like"
    return "like" if eng not in ("like", "oracle-text") else eng
