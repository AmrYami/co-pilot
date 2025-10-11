from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - allow unit tests without Flask dependency
    from flask import Blueprint, current_app, jsonify, request
except Exception:  # pragma: no cover - lightweight fallback used in tests
    current_app = None  # type: ignore[assignment]

    class _StubBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _jsonify(*args, **kwargs):
        return {}

    class _StubRequest:
        args: Dict[str, str] = {}

        def get_json(self, force: bool = False):
            return {}

    Blueprint = _StubBlueprint  # type: ignore[assignment]
    jsonify = _jsonify  # type: ignore[assignment]
    request = _StubRequest()  # type: ignore[assignment]
try:  # pragma: no cover - optional dependency in tests
    from sqlalchemy import text
except Exception:  # pragma: no cover
    def text(sql: str):  # type: ignore
        return sql

from apps.dw.common.debug_groups import build_boolean_debug, build_boolean_where
from apps.dw.common.sort_utils import normalize_sort

from .attempts import run_attempt
from .online_learning import store_rate_hints
from .rate_feedback import (
    apply_rate_hints_to_intent,
    build_contract_sql,
    parse_rate_comment as parse_rate_comment_legacy,
)
from .learning import save_patch, save_positive_rule
from .utils import env_flag, env_int
from .rate_comment import parse_rate_comment as parse_rate_comment_structured
from .sql_builders import GROSS_EXPR, group_by_sql, select_all_sql
from .settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from .fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from .learn.store import save_feedback
from apps.dw.learning_store import record_example, record_patch


def _default_namespace(ns: Optional[str]) -> str:
    text = (ns or "").strip()
    return text or "dw::common"


def _hash_pct(seed: str) -> int:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


def _should_apply_canary(user_email: Optional[str], question: Optional[str], percent: int) -> bool:
    pct = max(0, min(100, percent))
    key = f"{user_email or ''}|{question or ''}"
    return _hash_pct(key) < pct


def _build_example_tags(hints: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    if not isinstance(hints, dict):
        return tags
    if hints.get("fts_tokens") or hints.get("fts_token_groups"):
        tags.append("fts")
    for entry in hints.get("eq_filters") or []:
        if isinstance(entry, dict):
            col = entry.get("col") or entry.get("column")
            if col:
                tags.append(f"eq:{col}")
    group_by = hints.get("group_by")
    if group_by:
        tags.append(f"group:{group_by}")
    sort_by = hints.get("sort_by") or hints.get("order_by")
    if sort_by:
        tags.append(f"order:{sort_by}")
    return tags
from apps.dw.lib.eq_ops import build_eq_where as build_eq_where_v2
from apps.dw.lib.fts_ops import build_fts_where as build_fts_where_v2
from apps.dw.lib.rate_ops import parse_rate_comment as parse_rate_comment_v2
from apps.dw.lib.sql_utils import gross_expr as gross_expr_v2, merge_where as merge_where_v2, order_by_safe as order_by_safe_v2


def _settings_lookup(settings_obj: Any, key: str, namespace: Optional[str]) -> Any:
    if settings_obj is None:
        return None

    scopes: List[Tuple[Optional[str], Dict[str, Any]]] = []
    ns_value = (namespace or "").strip()
    if ns_value:
        scopes.append(("namespace", {"namespace": ns_value}))
    scopes.append(("global", {}))
    scopes.append((None, {}))

    for attr in ("get_json", "get"):
        getter = getattr(settings_obj, attr, None)
        if not callable(getter):
            continue
        for scope, extra in scopes:
            try:
                if scope is None:
                    value = getter(key)
                else:
                    value = getter(key, scope=scope, **extra)
            except TypeError:
                try:
                    value = getter(key)
                except Exception:
                    continue
            except Exception:
                continue
            if value is not None:
                return value
    return None


def _normalize_columns(raw: Sequence[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for col in raw or []:
        text = str(col or "").strip()
        if not text:
            continue
        upper = text.upper()
        if upper in seen:
            continue
        result.append(upper)
        seen.add(upper)
    return result


def _resolve_fts_columns(settings_obj: Any, namespace: Optional[str]) -> List[str]:
    raw = _settings_lookup(settings_obj, "DW_FTS_COLUMNS", namespace)
    candidates: Sequence[Any]
    if isinstance(raw, dict):
        candidates = raw.get("Contract") or raw.get("*") or []
    elif isinstance(raw, (list, tuple, set)):
        candidates = list(raw)
    else:
        candidates = []
    if not candidates:
        candidates = DEFAULT_CONTRACT_FTS_COLUMNS
    return _normalize_columns(candidates)


def _resolve_eq_columns(settings_obj: Any, namespace: Optional[str]) -> List[str]:
    raw = _settings_lookup(settings_obj, "DW_EXPLICIT_FILTER_COLUMNS", namespace)
    candidates: Sequence[Any]
    if isinstance(raw, dict):
        candidates = raw.get("Contract") or raw.get("*") or []
    elif isinstance(raw, (list, tuple, set)):
        candidates = list(raw)
    elif isinstance(raw, str):
        candidates = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        candidates = []
    if not candidates:
        candidates = DEFAULT_EXPLICIT_FILTER_COLUMNS
    return _normalize_columns(candidates)

rate_bp = Blueprint("dw_rate", __name__)


@rate_bp.post("/rate")
def rate():
    app = current_app
    engine = app.config["MEM_ENGINE"]
    data = request.get_json(force=True) or {}
    user_email_raw = data.get("auth_email") or data.get("user_email")
    user_email = (user_email_raw or "").strip() or None
    inquiry_id = int(data.get("inquiry_id") or 0)
    rating = int(data.get("rating") or 0)
    feedback = (data.get("feedback") or "").strip() or None
    comment = (data.get("comment") or "").strip()
    if not comment and feedback:
        comment = feedback
    structured_hints = parse_rate_comment_structured(comment or "")
    structured_hints["full_text_search"] = bool(structured_hints.get("fts_tokens"))

    def _build_sql_from_v2(hints: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any], Dict[str, Any]]:
        groups = hints.get("fts_tokens") or []
        operator = hints.get("fts_operator") or "OR"
        fts_sql = ""
        fts_binds: Dict[str, Any] = {}
        fts_debug: Dict[str, Any] = {
            "enabled": False,
            "error": None,
            "groups": groups,
            "columns": fts_columns,
        }
        if groups:
            fts_sql, fts_binds, fts_debug = build_fts_where_v2(settings_bundle, groups, operator)
            fts_debug.setdefault("groups", groups)
            fts_debug["operator"] = operator
        else:
            fts_debug["operator"] = operator

        boolean_groups = hints.get("boolean_groups") or []
        first_group = boolean_groups[0] if isinstance(boolean_groups, list) and boolean_groups else None
        where_bg = ""
        bg_binds: Dict[str, Any] = {}
        bg_binds_text = ""
        if isinstance(first_group, dict):
            where_bg, bg_binds, bg_binds_text = build_boolean_where(first_group)

        eq_filters = list(hints.get("eq_filters") or [])
        eq_sql = ""
        eq_binds: Dict[str, Any] = {}
        if eq_filters and not where_bg:
            eq_sql, eq_binds = build_eq_where_v2(eq_filters, settings_bundle, bind_prefix="eq")

        where_parts: List[str] = []
        if fts_sql:
            where_parts.append(fts_sql)
        if where_bg:
            where_parts.append(where_bg)
        elif eq_sql:
            where_parts.append(eq_sql)

        where_sql = merge_where_v2(where_parts)
        binds: Dict[str, Any] = {}
        binds.update(fts_binds)
        if where_bg:
            binds.update(bg_binds)
        else:
            binds.update(eq_binds)

        group_by = hints.get("group_by") or None
        gross_flag = hints.get("gross") if hints.get("gross") is not None else (False if group_by else None)
        select_clause = "*"
        if group_by:
            select_parts = [f"{group_by} AS GROUP_KEY"]
            if gross_flag is True:
                select_parts.append(f"{gross_expr_v2()} AS TOTAL_GROSS")
            else:
                select_parts.append("COUNT(*) AS CNT")
            select_clause = ", ".join(select_parts)

        sql_lines: List[str] = [f'SELECT {select_clause} FROM "Contract"']
        if where_sql:
            sql_lines.append(where_sql)
        if group_by:
            sql_lines.append(f"GROUP BY {group_by}")

        sort_hint = hints.get("order_by")
        sort_col, sort_desc = normalize_sort(sort_hint, default_col="REQUEST_DATE")
        order_dir_hint = (hints.get("order_dir") or "").upper()
        if order_dir_hint in {"ASC", "DESC"}:
            sort_desc = order_dir_hint != "ASC"
        effective_sort_by = sort_col
        if group_by:
            if sort_hint:
                order_clause = f"ORDER BY {sort_col} {'DESC' if sort_desc else 'ASC'}"
            else:
                if gross_flag is True:
                    effective_sort_by = "TOTAL_GROSS"
                    sort_desc = True
                    order_clause = "ORDER BY TOTAL_GROSS DESC"
                else:
                    effective_sort_by = "CNT"
                    sort_desc = True
                    order_clause = "ORDER BY CNT DESC"
        else:
            order_clause = f"ORDER BY {sort_col} {'DESC' if sort_desc else 'ASC'}"

        sql = "\n".join(sql_lines)
        sql = order_by_safe_v2(sql, order_clause)

        intent = {
            "wants_all_columns": not bool(group_by),
            "full_text_search": bool(groups),
            "fts_tokens": [token for group in groups for token in group],
            "fts_groups": groups,
            "fts_operator": operator,
            "fts_columns": fts_columns,
            "eq_filters": eq_filters,
            "boolean_groups": boolean_groups,
            "group_by": group_by,
            "gross": gross_flag,
            "sort_by": effective_sort_by,
            "sort_desc": sort_desc,
        }
        fts_debug.setdefault("columns", fts_columns)
        fts_debug.setdefault("binds", fts_debug.get("binds", {}))

        rate_hints = {
            "comment_present": bool(comment),
            "eq_filters": len(eq_filters),
            "group_by": [group_by] if group_by else None,
            "order_by_applied": bool(order_clause),
            "where_applied": bool(groups or eq_filters or where_bg),
            "gross": bool(gross_flag),
            "gross_expr": gross_expr_v2() if gross_flag else None,
        }
        if isinstance(first_group, dict):
            rate_hints["eq_filters"] = len(first_group.get("fields") or [])
            rate_hints["where_applied"] = True

        debug = {
            "intent": intent,
            "fts": fts_debug,
            "rate_hints": rate_hints,
            "validation": {
                "ok": True,
                "errors": [],
                "binds": list(binds.keys()),
                "bind_names": list(binds.keys()),
            },
        }

        if where_bg:
            debug["where_text"] = where_bg
            if bg_binds_text:
                debug["binds_text"] = bg_binds_text
            if fts_sql:
                def _wrap_clause(expr: str) -> str:
                    text = (expr or "").strip()
                    if not text:
                        return text
                    if text.startswith("(") and text.endswith(")"):
                        return text
                    return f"({text})"

                debug["where_text_full"] = f"{_wrap_clause(fts_sql)} AND {_wrap_clause(where_bg)}"

        debug["final_sql"] = {"size": len(sql), "sql": sql}

        return sql, binds, debug
    if not inquiry_id or rating < 1 or rating > 5:
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    with engine.begin() as cx:
        cx.execute(
            text(
                """
            UPDATE mem_inquiries
               SET rating = :r,
                   feedback_comment = COALESCE(:fb, feedback_comment),
                   satisfied = CASE WHEN :r >= 4 THEN TRUE ELSE NULL END,
                   updated_at = NOW()
             WHERE id = :iid
        """
            ),
            {"r": rating, "fb": feedback, "iid": inquiry_id},
        )

    inquiry_row: Optional[tuple[str, str]] = None
    if rating < 3 or rating >= 4:
        with engine.connect() as cx:
            row = cx.execute(
                text(
                    """
                SELECT namespace, question
                  FROM mem_inquiries
                 WHERE id = :iid
            """
                ),
                {"iid": inquiry_id},
            ).fetchone()
        if row is not None:
            if hasattr(row, "_mapping"):
                ns = row._mapping.get("namespace")
                qtext = row._mapping.get("question")
            else:
                try:
                    ns, qtext = row[0], row[1]
                except (TypeError, IndexError):
                    ns, qtext = None, None
            if ns is not None or qtext is not None:
                inquiry_row = (ns, qtext)

    pipeline_obj = app.config.get("PIPELINE") if app else None
    inquiry_namespace = inquiry_row[0] if inquiry_row else None
    default_namespace = getattr(pipeline_obj, "namespace", None)
    effective_namespace = inquiry_namespace or default_namespace
    namespace_value = _default_namespace(effective_namespace)
    settings_obj = app.config.get("SETTINGS") if app else None
    fts_columns = _resolve_fts_columns(settings_obj, effective_namespace)
    eq_allowed = _resolve_eq_columns(settings_obj, effective_namespace)
    engine_setting = _settings_lookup(settings_obj, "DW_FTS_ENGINE", effective_namespace) or "like"
    synonyms_setting = _settings_lookup(settings_obj, "DW_ENUM_SYNONYMS", effective_namespace) or {}
    settings_bundle: Dict[str, Any] = {
        "DW_FTS_COLUMNS": {"value": {"Contract": fts_columns}},
        "DW_FTS_ENGINE": {"value": str(engine_setting or "like")},
        "DW_EXPLICIT_FILTER_COLUMNS": {"value": eq_allowed},
    }
    if synonyms_setting:
        settings_bundle["DW_ENUM_SYNONYMS"] = {"value": synonyms_setting}

    structured_hints_v2 = parse_rate_comment_v2(comment or "")

    def _flatten_groups(groups: Optional[List[List[str]]]) -> List[str]:
        flattened: List[str] = []
        for group in groups or []:
            for token in group:
                token_str = (token or "").strip()
                if token_str:
                    flattened.append(token_str)
        return flattened

    if structured_hints_v2.get("fts_tokens"):
        structured_hints["fts_tokens"] = _flatten_groups(structured_hints_v2.get("fts_tokens"))
        structured_hints["fts_token_groups"] = structured_hints_v2.get("fts_tokens") or []
        structured_hints["fts_operator"] = structured_hints_v2.get("fts_operator") or structured_hints.get("fts_operator") or "OR"
    else:
        structured_hints.setdefault("fts_token_groups", [])
        structured_hints.setdefault("fts_operator", structured_hints.get("fts_operator") or "OR")

    if structured_hints_v2.get("eq_filters"):
        structured_hints["eq_filters"] = list(structured_hints_v2.get("eq_filters") or [])

    if structured_hints_v2.get("group_by"):
        structured_hints["group_by"] = structured_hints_v2.get("group_by")

    if structured_hints_v2.get("order_by"):
        structured_hints["sort_by"] = structured_hints_v2.get("order_by")
    if structured_hints_v2.get("order_dir"):
        structured_hints["sort_desc"] = (structured_hints_v2.get("order_dir") or "DESC").upper() != "ASC"

    if structured_hints_v2.get("gross") is not None:
        structured_hints["gross"] = structured_hints_v2.get("gross")

    structured_hints["full_text_search"] = bool(structured_hints.get("fts_tokens"))

    rate_sql: Optional[str] = None
    rate_binds: Dict[str, Any] = {}
    rate_debug: Dict[str, Any] = {}
    structured_hint_present_v2 = any(
        (
            structured_hints_v2.get("fts_tokens"),
            structured_hints_v2.get("eq_filters"),
            structured_hints_v2.get("group_by"),
            structured_hints_v2.get("order_by"),
            structured_hints_v2.get("gross") is not None,
        )
    )
    if structured_hint_present_v2:
        try:
            rate_sql, rate_binds, rate_debug = _build_sql_from_v2(structured_hints_v2)
            intent_from_v2 = rate_debug.get("intent") if isinstance(rate_debug, dict) else None
            if isinstance(intent_from_v2, dict):
                if intent_from_v2.get("fts_tokens") is not None:
                    structured_hints["fts_tokens"] = list(intent_from_v2.get("fts_tokens") or [])
                if intent_from_v2.get("fts_groups") is not None:
                    structured_hints["fts_token_groups"] = list(intent_from_v2.get("fts_groups") or [])
                if intent_from_v2.get("fts_operator"):
                    structured_hints["fts_operator"] = intent_from_v2.get("fts_operator")
                if intent_from_v2.get("group_by"):
                    structured_hints["group_by"] = intent_from_v2.get("group_by")
                if intent_from_v2.get("gross") is not None:
                    structured_hints["gross"] = intent_from_v2.get("gross")
                if intent_from_v2.get("sort_by"):
                    structured_hints["sort_by"] = intent_from_v2.get("sort_by")
                if intent_from_v2.get("sort_desc") is not None:
                    structured_hints["sort_desc"] = intent_from_v2.get("sort_desc")
                structured_hints["full_text_search"] = bool(intent_from_v2.get("full_text_search"))
        except Exception as exc:  # pragma: no cover - defensive fallback
            rate_sql = None
            rate_binds = {}
            rate_debug = {"error": str(exc)}

    def _rate_hints_to_dict(hints_obj) -> Dict[str, Any]:
        if not hints_obj:
            return {}
        payload: Dict[str, Any] = {}
        if getattr(hints_obj, "fts_tokens", None):
            payload["fts_tokens"] = list(hints_obj.fts_tokens)
            payload["fts_operator"] = hints_obj.fts_operator
            payload["full_text_search"] = True
        if getattr(hints_obj, "order_by", None):
            payload["order_by"] = hints_obj.order_by
        filters = []
        for f in getattr(hints_obj, "eq_filters", []) or []:
            filters.append(
                {
                    "col": f.col,
                    "val": f.val,
                    "ci": f.ci,
                    "trim": f.trim,
                    "op": f.op,
                }
            )
        if filters:
            payload["eq_filters"] = filters
        if getattr(hints_obj, "group_by", None):
            payload["group_by"] = hints_obj.group_by
        if getattr(hints_obj, "gross", None) is not None:
            payload["gross"] = bool(hints_obj.gross)
        return payload

    hints_dict: Dict[str, Any] = {}
    hints_obj = None
    if comment:
        try:
            hints_obj = parse_rate_comment_legacy(comment)
            hints_dict = _rate_hints_to_dict(hints_obj)
        except Exception:
            hints_obj = None
            hints_dict = {}

    hints_debug: Dict[str, Any] = {}
    intent_snapshot: Dict[str, Any] = {}
    if hints_obj:
        try:
            settings = current_app.config.get("NAMESPACE_SETTINGS", {}) if current_app else {}
        except Exception:
            settings = {}
        try:
            apply_rate_hints_to_intent(intent_snapshot, hints_obj, settings)
            sql, binds = build_contract_sql(intent_snapshot, settings)
            hints_debug = {
                "sql": sql,
                "binds": binds,
                "intent": intent_snapshot,
            }
            plan = intent_snapshot.get("boolean_plan") if isinstance(intent_snapshot, dict) else None
            if isinstance(plan, dict):
                where_text = plan.get("where_text")
                if where_text:
                    hints_debug["where_text"] = where_text
                binds_text = plan.get("binds_text")
                if binds_text:
                    hints_debug["binds_text"] = binds_text
        except Exception as exc:
            hints_debug = {"error": str(exc)}

    structured_hint_present = bool(comment) and any(
        (
            structured_hints.get("fts_tokens"),
            structured_hints.get("fts_token_groups"),
            structured_hints.get("eq_filters"),
            structured_hints.get("group_by"),
            structured_hints.get("sort_by"),
            structured_hints.get("gross") is not None,
        )
    )
    if structured_hint_present and rate_sql is None:
        rate_binds = {}
        try:
            if structured_hints.get("group_by"):
                rate_sql, rate_binds = group_by_sql(
                    group_by=structured_hints.get("group_by") or "",
                    gross=bool(structured_hints.get("gross")),
                    fts_tokens=list(structured_hints.get("fts_tokens") or []),
                    fts_cols=fts_columns,
                    fts_operator=str(structured_hints.get("fts_operator") or "OR"),
                    eq_filters=list(structured_hints.get("eq_filters") or []),
                    allowed_eq_cols=eq_allowed,
                    sort_by=structured_hints.get("sort_by") or None,
                    sort_desc=structured_hints.get("sort_desc"),
                )
            else:
                rate_sql, rate_binds = select_all_sql(
                    fts_tokens=list(structured_hints.get("fts_tokens") or []),
                    fts_cols=fts_columns,
                    fts_operator=str(structured_hints.get("fts_operator") or "OR"),
                    eq_filters=list(structured_hints.get("eq_filters") or []),
                    allowed_eq_cols=eq_allowed,
                    sort_by=structured_hints.get("sort_by") or None,
                    sort_desc=structured_hints.get("sort_desc"),
                )
        except Exception as exc:
            rate_sql = None
            rate_binds = {}
            rate_debug = {"error": str(exc)}

    if rate_sql and not rate_debug:
        fts_tokens = list(structured_hints.get("fts_tokens") or [])
        eq_filters = list(structured_hints.get("eq_filters") or [])
        sort_by_hint = structured_hints.get("sort_by") or None
        sort_desc_hint = (
            structured_hints.get("sort_desc")
            if structured_hints.get("sort_desc") is not None
            else True
        )
        rate_debug = {
            "intent": {
                "wants_all_columns": True,
                "full_text_search": bool(fts_tokens),
                "fts_tokens": fts_tokens,
                "fts_operator": structured_hints.get("fts_operator") or "OR",
                "fts_columns": fts_columns,
                "eq_filters": eq_filters,
                "group_by": structured_hints.get("group_by"),
                "gross": structured_hints.get("gross"),
                "sort_by": sort_by_hint,
                "sort_desc": sort_desc_hint,
            },
            "fts": {
                "enabled": bool(fts_tokens),
                "tokens": fts_tokens,
                "columns": fts_columns,
                "binds": {k: v for k, v in rate_binds.items() if k.startswith("fts_")},
                "error": None,
            },
            "rate_hints": {
                "comment_present": bool(comment),
                "eq_filters": len(eq_filters),
                "group_by": [structured_hints.get("group_by")] if structured_hints.get("group_by") else None,
                "order_by_applied": bool(sort_by_hint),
                "where_applied": bool(fts_tokens or eq_filters),
                "gross": bool(structured_hints.get("gross")),
                "gross_expr": GROSS_EXPR if structured_hints.get("gross") else None,
            },
            "validation": {
                "ok": True,
                "errors": [],
                "binds": list(rate_binds.keys()),
                "bind_names": list(rate_binds.keys()),
            },
        }
        plan = intent_snapshot.get("boolean_plan") if isinstance(intent_snapshot, dict) else None
        if isinstance(plan, dict):
            rate_debug.setdefault("intent", {})["boolean_plan"] = plan
            where_text = plan.get("where_text")
            if where_text:
                rate_debug["where_text"] = where_text
            binds_text = plan.get("binds_text")
            if binds_text:
                rate_debug["binds_text"] = binds_text
            field_count = plan.get("field_count")
            hints_section = rate_debug.get("rate_hints")
            if isinstance(hints_section, dict):
                hints_section["where_applied"] = True
                if isinstance(field_count, int):
                    hints_section["eq_filters"] = field_count
        if hints_debug:
            rate_debug.setdefault("legacy", {})["rate_hints"] = hints_debug
    elif rate_debug:
        if hints_debug:
            rate_debug.setdefault("legacy", {})["rate_hints"] = hints_debug

    if structured_hint_present:
        try:
            save_feedback(
                inquiry_id,
                rating,
                comment or "",
                {
                    "hints": structured_hints,
                    "sql": rate_sql,
                    "binds": rate_binds,
                    "namespace": effective_namespace,
                },
            )
        except Exception:
            pass

    def _augment_response(payload: Dict[str, Any]) -> Dict[str, Any]:
        debug_section = payload.setdefault("debug", {})
        if rate_sql:
            payload["sql"] = rate_sql
            meta = payload.setdefault("meta", {})
            meta.setdefault("attempt_no", 2)
            meta.setdefault("strategy", "rate_overrides")
            meta["binds"] = rate_binds
            intent_debug = rate_debug.get("intent")
            if intent_debug:
                meta["clarifier_intent"] = intent_debug
            payload.setdefault("rows", [])
            payload["retry"] = payload.get("retry") or True
            for key, value in rate_debug.items():
                if value is None:
                    continue
                if (
                    isinstance(value, dict)
                    and isinstance(debug_section.get(key), dict)
                ):
                    debug_section[key].update(value)
                else:
                    debug_section[key] = value
        elif hints_debug:
            debug_section["rate_hints"] = hints_debug

        fts_section = debug_section.get("fts")
        if not isinstance(fts_section, dict):
            fts_section = {}
            debug_section["fts"] = fts_section
        engine_value = str(engine_setting or "like").lower()
        if engine_value == "like":
            fts_section["engine"] = "like"
            if fts_section.get("error") == "no_engine":
                fts_section.pop("error", None)
        elif engine_value:
            fts_section.setdefault("engine", engine_value)

        try:
            base_question = comment or (inquiry_row[1] if inquiry_row and len(inquiry_row) >= 2 else None) or ""
            plan = build_boolean_debug(base_question, fts_columns)
            debug_section["boolean_groups"] = plan.get("blocks", [])
            debug_section["boolean_groups_text"] = plan.get("summary", "")
            where_text = plan.get("where_text")
            if where_text:
                debug_section["where_text"] = where_text
            binds_text = plan.get("binds_text")
            if binds_text:
                debug_section["binds_text"] = binds_text
        except Exception as exc:  # pragma: no cover - debug best-effort
            debug_section["boolean_groups_error"] = str(exc)

        return payload

    question_text = inquiry_row[1] if inquiry_row and len(inquiry_row) >= 2 else None

    if rating >= 4 and question_text:
        if intent_snapshot:
            try:
                save_positive_rule(engine, question_text, intent_snapshot)
            except Exception:
                pass
        if rate_sql:
            try:
                tags = _build_example_tags(structured_hints_v2 or structured_hints or {})
                record_example(
                    namespace_value,
                    user_email,
                    question_text,
                    rate_sql,
                    tags=tags,
                    rating=rating,
                )
            except Exception:
                pass

    if rating <= 2 and comment and question_text and hints_dict:
        store_rate_hints(question_text, hints_dict)
        try:
            save_patch(engine, inquiry_id, question_text, rating, comment, hints_dict)
        except Exception:
            pass
        try:
            canary_percent = int(os.getenv("DW_CANARY_DEFAULT_PERCENT", "15"))
        except Exception:
            canary_percent = 15
        applied_now_flag = bool(rate_sql) and _should_apply_canary(user_email, question_text, canary_percent)
        try:
            patch_payload = {
                "structured": structured_hints_v2,
                "legacy": hints_dict,
            }
            record_patch(
                namespace_value,
                user_email,
                inquiry_id,
                rating,
                comment,
                patch_payload,
                status="shadow",
                applied_now=applied_now_flag,
            )
        except Exception:
            pass

    if rating < 3 and env_int("DW_MAX_RERUNS", 1) > 0:
        alt_strategy = (
            request.args.get("strategy")
            or (env_flag("DW_ACCURACY_FIRST", True) and "det_overlaps_gross")
            or "deterministic"
        )
        if inquiry_row:
            ns, q = inquiry_row[0], inquiry_row[1]
            fts_present = bool(
                (hints_dict.get("fts_tokens") if hints_dict else None)
                or (hints_dict.get("full_text_search") if hints_dict else None)
                or getattr(hints_obj, "fts_tokens", None)
                or getattr(hints_obj, "full_text_search", None)
            )
            alt = run_attempt(
                q,
                ns,
                attempt_no=2,
                strategy=alt_strategy,
                full_text_search=True if fts_present else None,
                rate_comment=comment or None,
            )
            if comment and hints_dict:
                store_rate_hints(q, hints_dict)
            if hints_debug:
                alt.setdefault("debug", {})["rate_hints"] = hints_debug
            with engine.begin() as cx:
                cx.execute(
                    text(
                        """
                    INSERT INTO mem_runs(namespace, input_query, status, context_pack, created_at)
                    VALUES(:ns, :q, 'complete', :ctx, NOW())
                """
                    ),
                    {
                        "ns": ns,
                        "q": q,
                        "ctx": json.dumps(
                            {
                                "inquiry_id": inquiry_id,
                                "attempt_no": 2,
                                "strategy": alt_strategy,
                            }
                        ),
                    },
                )
            response_payload = {"ok": True, "retry": True, "inquiry_id": inquiry_id, **alt}
            response_payload = _augment_response(response_payload)
            return jsonify(response_payload)

        response = {"ok": True, "retry": False, "inquiry_id": inquiry_id}
        response = _augment_response(response)
        return jsonify(response)

    if rating < 3 and env_flag("DW_ESCALATE_ON_LOW_RATING", True):
        with engine.begin() as cx:
            cx.execute(
                text(
                    """
                INSERT INTO mem_alerts(namespace, event_type, recipient, payload, status, created_at)
                VALUES(:ns, 'low_rating', :rcpt, :payload, 'queued', NOW())
            """
                ),
                {
                    "ns": "dw::common",
                    "rcpt": "admin@example.com",
                    "payload": json.dumps(
                        {"inquiry_id": inquiry_id, "rating": rating, "feedback": feedback}
                    ),
                },
            )

    response: Dict[str, Any] = {"ok": True, "retry": False, "inquiry_id": inquiry_id}
    response = _augment_response(response)
    return jsonify(response)
