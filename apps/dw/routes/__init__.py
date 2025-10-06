"""Simplified DW blueprint exposing /dw/answer and /dw/rate."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from flask import Blueprint, jsonify, request

from apps.dw.db import fetch_rows
from apps.dw.intent import derive_intent
from apps.dw.rate_grammar import parse_rate_comment
from apps.dw.sql_builder import build_contract_sql, build_eq_where, build_fts_where
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS

try:  # pragma: no cover - lightweight fallback for tests without settings backend
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default

bp = Blueprint("dw", __name__)


@bp.route("/dw/answer", methods=["POST"])
def answer() -> Any:
    payload = request.get_json(force=True, silent=True) or {}
    intent = derive_intent(payload)
    sql, binds = build_contract_sql(intent)
    rows = fetch_rows(sql, binds)
    return jsonify({"ok": True, "sql": sql, "meta": {"binds": binds}, "rows": rows})


@bp.route("/dw/rate", methods=["POST"])
def rate() -> Any:
    payload = request.get_json(force=True, silent=True) or {}
    comment = (payload.get("comment") or "").strip()

    hints = parse_rate_comment(comment)
    binds: Dict[str, Any] = {}
    where_parts: List[str] = []

    # Resolve FTS columns
    cfg = _get_setting("DW_FTS_COLUMNS", scope="namespace", namespace="dw::common", default={})
    contract_columns: Iterable[str]
    if isinstance(cfg, dict):
        contract_columns = (
            cfg.get("Contract")
            or cfg.get("CONTRACT")
            or cfg.get("*")
            or DEFAULT_CONTRACT_FTS_COLUMNS
        )
    else:
        contract_columns = DEFAULT_CONTRACT_FTS_COLUMNS
    fts_columns = [
        col if (isinstance(col, str) and col.strip().startswith('"'))
        else str(col).strip().upper()
        for col in contract_columns
        if isinstance(col, str) and col.strip()
    ]

    fts_where = build_fts_where(hints.get("fts_tokens") or [], binds, operator=hints.get("fts_operator", "OR"), columns=fts_columns)
    if fts_where:
        where_parts.append(fts_where)

    allowed_eq = _get_setting(
        "DW_EXPLICIT_FILTER_COLUMNS",
        scope="namespace",
        namespace="dw::common",
        default=None,
    )
    if not isinstance(allowed_eq, (list, tuple, set)):
        allowed_eq = DEFAULT_EXPLICIT_FILTER_COLUMNS
    eq_predicates = build_eq_where(hints.get("eq_filters") or [], binds, allowed_columns=allowed_eq)
    where_parts.extend(eq_predicates)

    where_sql = ""
    if where_parts:
        where_sql = " WHERE " + " AND ".join(f"({part})" for part in where_parts)

    group_cols = hints.get("group_by") or []
    sort_by = hints.get("sort_by")
    sort_desc = hints.get("sort_desc")
    if sort_desc is None:
        sort_desc = True

    if group_cols:
        group_cols_fmt = ", ".join(group_cols)
        order_col = sort_by or "CNT"
        order_sql = f" ORDER BY {order_col} {'DESC' if sort_desc else 'ASC'}"
        sql = (
            f'SELECT {group_cols_fmt} AS GROUP_KEY, COUNT(*) AS CNT FROM "Contract"'
            f"{where_sql} GROUP BY {group_cols_fmt}{order_sql}"
        )
    else:
        order_col = sort_by or "REQUEST_DATE"
        order_sql = f" ORDER BY {order_col} {'DESC' if sort_desc else 'ASC'}"
        sql = f'SELECT * FROM "Contract"{where_sql}{order_sql}'

    rows = fetch_rows(sql, binds)

    intent = {
        "full_text_search": bool(hints.get("fts_tokens")),
        "fts_tokens": hints.get("fts_tokens") or [],
        "fts_operator": hints.get("fts_operator") or "OR",
        "eq_filters": hints.get("eq_filters") or [],
        "group_by": group_cols or None,
        "sort_by": sort_by,
        "sort_desc": sort_desc,
        "wants_all_columns": not group_cols,
    }

    debug = {
        "intent": intent,
        "validation": {
            "ok": True,
            "bind_names": list(binds.keys()),
            "binds": list(binds.keys()),
            "errors": [],
        },
        "fts": {
            "enabled": bool(hints.get("fts_tokens")),
            "tokens": hints.get("fts_tokens") or None,
            "columns": fts_columns,
            "binds": {k: v for k, v in binds.items() if k.startswith("fts_")},
            "error": None,
        },
    }

    meta = {
        "attempt_no": 1,
        "binds": binds,
        "clarifier_intent": intent,
        "strategy": "rate_hints_direct",
    }

    return jsonify({
        "ok": True,
        "sql": sql,
        "meta": meta,
        "debug": debug,
        "rows": rows,
        "retry": True,
    })


__all__ = ["bp", "answer", "rate"]
