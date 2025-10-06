from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Sequence

from flask import Blueprint, request, jsonify

from .settings import get_setting
from .sql_builder import build_measure_sql, quote_ident, strip_double_order_by
from .learn_store import LearningStore, ExampleRecord, PatchRecord
from .utils import safe_upper
from .settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from .rate_helpers import (
    build_eq_clause,
    build_fts_like_where,
    choose_fts_columns,
    parse_rate_comment,
)

rate_bp = Blueprint("rate", __name__)

# ---------------------------
# Rate Comment Parsing
# ---------------------------
def _resolve_allowed_column(raw_col: str | None, allowed: Sequence[str]) -> str | None:
    if not raw_col:
        return None
    target = raw_col.strip().upper()
    for candidate in allowed:
        candidate_up = candidate.strip().upper()
        if target == candidate_up:
            return candidate
        if target.replace(" ", "") == candidate_up.replace(" ", ""):
            return candidate
    return None


@rate_bp.route("/dw/rate", methods=["POST"])
def rate():
    payload = request.get_json(force=True, silent=True) or {}
    inquiry_id = payload.get("inquiry_id")
    rating = payload.get("rating")
    comment = payload.get("comment") or ""

    hints_intent = parse_rate_comment(comment)

    measure_sql = build_measure_sql()

    binds: dict = {}
    where_parts: list[str] = []
    order_by: str | None = None
    sort_desc: bool | None = None

    namespace_settings = {
        "DW_FTS_COLUMNS": get_setting("DW_FTS_COLUMNS", scope="namespace") or {},
        "DW_EXPLICIT_FILTER_COLUMNS": get_setting("DW_EXPLICIT_FILTER_COLUMNS", scope="namespace")
        or DEFAULT_EXPLICIT_FILTER_COLUMNS,
    }

    raw_fts_columns = choose_fts_columns(namespace_settings)
    fts_columns = [quote_ident(col) for col in raw_fts_columns]
    fts_tokens = hints_intent.get("fts_tokens") or []
    fts_operator = hints_intent.get("fts_operator") or "OR"
    fts_where, fts_binds = build_fts_like_where(fts_columns, fts_tokens, operator=fts_operator)
    fts_enabled = bool(fts_where)
    if fts_where:
        where_parts.append(fts_where)
        binds.update(fts_binds)

    raw_allowed_eq = namespace_settings.get("DW_EXPLICIT_FILTER_COLUMNS")
    if not isinstance(raw_allowed_eq, Sequence) or isinstance(raw_allowed_eq, (str, bytes)):
        allowed_eq_columns: List[str] = list(DEFAULT_EXPLICIT_FILTER_COLUMNS)
    else:
        allowed_eq_columns = [str(col).strip() for col in raw_allowed_eq if isinstance(col, str) and col.strip()]
        if not allowed_eq_columns:
            allowed_eq_columns = list(DEFAULT_EXPLICIT_FILTER_COLUMNS)

    eq_filters = []
    eq_applied: List[Dict[str, str]] = []
    for filt in hints_intent.get("eq_filters") or []:
        resolved = _resolve_allowed_column(filt.get("col"), allowed_eq_columns)
        if not resolved:
            continue
        eq_filter = dict(filt)
        eq_filter["col"] = quote_ident(resolved)
        eq_filters.append(eq_filter)
        eq_applied.append({**filt, "col": resolved})

    eq_where, eq_binds = build_eq_clause(eq_filters)
    if eq_where:
        where_parts.append(eq_where)
        binds.update(eq_binds)

    group_by_resolved: List[str] = []
    group_by_display: List[str] = []
    for col in hints_intent.get("group_by") or []:
        resolved = _resolve_allowed_column(col, allowed_eq_columns)
        if resolved:
            group_by_resolved.append(quote_ident(resolved))
            group_by_display.append(resolved)

    gross_flag = hints_intent.get("gross")

    order_hint = hints_intent.get("order_by") or {}
    if order_hint:
        resolved = _resolve_allowed_column(order_hint.get("col"), allowed_eq_columns)
        if resolved:
            order_by = quote_ident(resolved)
            sort_desc = (order_hint.get("dir") or "DESC").upper() != "ASC"
        else:
            col_upper = (order_hint.get("col") or "").strip().upper()
            if col_upper in {"MEASURE", "CNT", "COUNT"}:
                order_by = col_upper if col_upper != "COUNT" else "CNT"
                sort_desc = (order_hint.get("dir") or "DESC").upper() != "ASC"
            elif col_upper == "REQUEST_DATE":
                order_by = quote_ident("REQUEST_DATE")
                sort_desc = (order_hint.get("dir") or "DESC").upper() != "ASC"

    table_setting = get_setting("DW_CONTRACT_TABLE", scope="namespace")
    if isinstance(table_setting, str) and table_setting.strip():
        table = table_setting.strip()
    else:
        table = '"Contract"'
    if not table.startswith('"'):
        table = quote_ident(table)

    final_sql: str

    if group_by_resolved:
        select_group = ", ".join(group_by_resolved)
        if gross_flag is True:
            select_cols = f"{select_group}, SUM({measure_sql}) AS MEASURE, COUNT(*) AS CNT"
            default_order_col = "MEASURE"
        elif gross_flag is False:
            select_cols = f"{select_group}, COUNT(*) AS CNT"
            default_order_col = "CNT"
        else:
            select_cols = f"{select_group}, COUNT(*) AS CNT"
            default_order_col = "CNT"
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        order_col = order_by or default_order_col
        order_upper = safe_upper(order_col.strip('"')) if isinstance(order_col, str) else None
        if order_upper == "REQUEST_DATE":
            order_col = default_order_col
        if order_col not in ("MEASURE", "CNT"):
            order_col = quote_ident(str(order_col).strip('"'))
        direction = "DESC" if (sort_desc is True or sort_desc is None) else "ASC"
        final_sql = (
            f"SELECT {select_cols}\n"
            f"FROM {table}{where_sql}\n"
            f"GROUP BY {select_group}\n"
            f"ORDER BY {order_col} {direction}"
        )
    else:
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        if order_by is None:
            order_by = quote_ident("REQUEST_DATE")
            sort_desc = True
        direction = "DESC" if (sort_desc is True or sort_desc is None) else "ASC"
        final_sql = f"SELECT * FROM {table}{where_sql}\nORDER BY {order_by} {direction}"

    final_sql = strip_double_order_by(final_sql)

    debug = {
        "fts": {
            "enabled": bool(fts_enabled),
            "mode": "like" if fts_enabled else None,
            "tokens": fts_tokens or None,
            "columns": raw_fts_columns if fts_enabled else None,
            "binds": list(fts_binds.keys()) if fts_enabled else None,
            "error": None,
        },
        "eq": {
            "applied": eq_applied,
            "binds": list(eq_binds.keys()),
        },
        "intent": {
            "agg": None if not group_by_resolved else ("count" if gross_flag is not True else "sum"),
            "date_column": "OVERLAP",
            "eq_filters": hints_intent.get("eq_filters") or [],
            "group_by": group_by_display,
            "measure_sql": measure_sql,
        },
        "validation": {
            "ok": True,
            "errors": [],
            "binds": list(binds.keys()),
            "bind_names": list(binds.keys()),
        },
    }

    try:
        store = LearningStore()
        if rating is not None:
            if rating >= 4:
                store.save_example(
                    ExampleRecord(
                        inquiry_id=inquiry_id,
                        question=payload.get("question") or "",
                        sql=final_sql,
                        created_at=datetime.utcnow(),
                    )
                )
            elif rating <= 2 and comment:
                store.save_patch(
                    PatchRecord(
                        inquiry_id=inquiry_id,
                        comment=comment,
                        produced_sql=final_sql,
                        created_at=datetime.utcnow(),
                    )
                )
    except Exception as e:  # pragma: no cover - defensive logging path
        debug["learning_store_error"] = str(e)

    return jsonify(
        {
            "ok": True,
            "inquiry_id": inquiry_id,
            "sql": final_sql,
            "meta": {
                "attempt_no": 2,
                "binds": binds,
                "clarifier_intent": debug["intent"],
                "fts": debug["fts"],
                "rate_hints": {
                    "comment_present": bool(comment),
                    "eq_filters": len(eq_applied),
                    "group_by": group_by_display if group_by_display else None,
                    "order_by_applied": True,
                    "where_applied": bool(where_parts),
                },
            },
            "debug": debug,
            "rows": [],
            "retry": True,
        }
    )
