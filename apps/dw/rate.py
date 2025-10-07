from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Sequence

from flask import Blueprint, request, jsonify

from apps.dw.patchlib.order_utils import (
    detect_order_direction as _patch_detect_order_direction,
    detect_top_n as _patch_detect_top_n,
)
from apps.dw.patchlib.rate_parser import parse_rate_comment as parse_rate_comment_patch
from apps.dw.patchlib.settings_util import (
    get_explicit_filter_columns as _patch_get_explicit_filter_columns,
    get_enum_synonyms as _patch_get_enum_synonyms,
    get_fts_columns as _patch_get_fts_columns,
)
from apps.dw.sql_builder import build_eq_where_from_pairs, build_fts_where as build_fts_where_patch
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
LOGGER = logging.getLogger("dw.rate")

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

    patch_hints = parse_rate_comment_patch(comment)
    patch_has_directives = any(
        [
            patch_hints.get("fts"),
            patch_hints.get("eq"),
            patch_hints.get("group_by"),
            patch_hints.get("gross") is not None,
            patch_hints.get("order_by"),
            _patch_detect_top_n(comment),
        ]
    )

    if patch_has_directives:
        try:
            explicit_cols = _patch_get_explicit_filter_columns() or []
            allowed = {
                str(col).strip().upper().replace(" ", "_")
                for col in explicit_cols
                if isinstance(col, str) and col.strip()
            }

            eq_pairs: List[Dict] = []
            for pair in patch_hints.get("eq") or []:
                col = str(pair.get("col") or "").upper()
                if not col:
                    continue
                if allowed and col not in allowed:
                    continue
                eq_pairs.append(pair)

            fts_info = patch_hints.get("fts") or {}
            fts_tokens = fts_info.get("tokens") or []
            fts_mode = fts_info.get("mode") or "OR"
            fts_sql, fts_binds = build_fts_where_patch(fts_tokens, fts_mode)

            eq_sql, eq_binds = build_eq_where_from_pairs(eq_pairs, _patch_get_enum_synonyms())

            where_parts_patch = [part for part in (fts_sql, eq_sql) if part]
            where_sql = (" WHERE " + " AND ".join(where_parts_patch)) if where_parts_patch else ""

            group_by_col = patch_hints.get("group_by")
            gross_flag = patch_hints.get("gross")
            measure_expr = (
                "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
                "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
            )

            if group_by_col:
                select_sql = f"SELECT {group_by_col} AS GROUP_KEY"
                default_order_col = "CNT"
                if gross_flag is True:
                    select_sql += f", SUM({measure_expr}) AS TOTAL_GROSS"
                    default_order_col = "TOTAL_GROSS"
                select_sql += ", COUNT(*) AS CNT"
                final_sql = (
                    f"{select_sql}\n"
                    f"FROM \"Contract\"{where_sql}\n"
                    f"GROUP BY {group_by_col}"
                )
            else:
                final_sql = f'SELECT *\nFROM "Contract"{where_sql}'
                default_order_col = "REQUEST_DATE"

            order_hint = patch_hints.get("order_by")
            if order_hint:
                order_col = order_hint.get("col") or default_order_col
                order_dir = (order_hint.get("dir") or "DESC").upper()
            else:
                order_col = default_order_col
                order_dir = _patch_detect_order_direction(comment, default_desc=True)

            if not order_col:
                order_col = "REQUEST_DATE"
            if not order_dir:
                order_dir = "DESC"

            final_sql += f"\nORDER BY {order_col} {order_dir}"

            top_n = _patch_detect_top_n(comment)
            if top_n:
                final_sql += f"\nFETCH FIRST {top_n} ROWS ONLY"

            binds: Dict[str, object] = {}
            binds.update(fts_binds)
            binds.update(eq_binds)

            debug_fts = {
                "enabled": bool(fts_tokens),
                "mode": (fts_mode or "OR").upper(),
                "tokens": fts_tokens,
                "columns": _patch_get_fts_columns("Contract"),
                "binds": list(fts_binds.keys()),
                "error": None,
            }
            debug_eq = {
                "pairs": eq_pairs,
                "binds": list(eq_binds.keys()),
            }
            debug_payload = {
                "fts": debug_fts,
                "eq": debug_eq,
                "validation": {
                    "ok": True,
                    "errors": [],
                    "binds": list(binds.keys()),
                    "bind_names": list(binds.keys()),
                },
                "rate_patch": True,
            }

            response_payload = {
                "ok": True,
                "inquiry_id": inquiry_id,
                "sql": final_sql,
                "meta": {
                    "attempt_no": 2,
                    "binds": binds,
                    "clarifier_intent": {
                        "fts": fts_tokens,
                        "eq": eq_pairs,
                        "group_by": group_by_col,
                        "gross": gross_flag,
                    },
                    "fts": debug_fts,
                },
                "debug": debug_payload,
                "rows": [],
                "retry": True,
            }

            return jsonify(response_payload)
        except Exception:  # pragma: no cover - defensive fallback
            LOGGER.exception("[dw/rate] patch handler failed; using legacy pipeline")

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
