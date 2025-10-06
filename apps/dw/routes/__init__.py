"""Simplified DW blueprint exposing /dw/answer and /dw/rate."""

from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, jsonify, request

from apps.dw.db import fetch_rows
from apps.dw.intent import derive_intent
from apps.dw.rate_grammar import apply_rate_comment
from apps.dw.sql_builder import build_contract_sql

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

    base_intent: Dict[str, Any]
    if isinstance(payload.get("intent"), dict):
        base_intent = dict(payload["intent"])  # shallow copy
    else:
        base_intent = derive_intent({"question": "", "full_text_search": False})

    patched = apply_rate_comment(base_intent, comment)
    sql, binds = build_contract_sql(patched)
    rows = fetch_rows(sql, binds)

    debug = {
        "intent": {
            key: patched.get(key)
            for key in ("eq_filters", "fts", "group_by", "sort_by", "sort_desc", "gross")
        },
        "validation": {
            "ok": True,
            "bind_names": list(binds.keys()),
            "binds": list(binds.keys()),
            "errors": [],
        },
    }

    return jsonify({
        "ok": True,
        "sql": sql,
        "meta": {"binds": binds},
        "debug": debug,
        "rows": rows,
        "retry": True,
    })


__all__ = ["bp", "answer", "rate"]
