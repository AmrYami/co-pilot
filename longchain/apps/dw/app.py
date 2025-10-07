from __future__ import annotations

import copy
import itertools
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, render_template, request

from . import settings as dw_settings
from .examples import retrieve_examples_for_question, save_example_if_positive
from .explain import build_explain, build_user_explain
from .fts import build_fts_where
from .rules import choose_canary
from .settings import Settings


SETTINGS = Settings()
dw_bp = Blueprint("dw", __name__)


_INQUIRY_COUNTER = itertools.count(1)
_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOTS: Dict[int, Dict[str, Any]] = {}

_EQ_PATTERN = re.compile(r"(?P<col>[A-Za-z0-9_\"\. ]+)\s*=\s*(?P<val>'[^']*'|\"[^\"]*\"|[A-Za-z0-9_./-]+)", re.IGNORECASE)


def _clean_eq_value(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    return text


def _normalize_col(col: str) -> str:
    text = (col or "").strip()
    if text.startswith('"') and text.endswith('"'):
        return text.strip('"')
    return re.sub(r"\s+", "_", text.upper())


def _parse_eq_filters(text: str) -> List[Dict[str, Any]]:
    filters: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    for match in _EQ_PATTERN.finditer(text or ""):
        col_raw = match.group("col")
        val_raw = match.group("val")
        col = _normalize_col(col_raw)
        val = _clean_eq_value(val_raw)
        key = (col, val.upper())
        if key in seen:
            continue
        seen.add(key)
        filters.append({
            "col": col,
            "val": val,
            "ci": True,
            "trim": True,
        })
    return filters


def _build_eq_sql(eq_filters: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    sql_parts: List[str] = []
    binds: Dict[str, Any] = {}
    for idx, spec in enumerate(eq_filters):
        col = _normalize_col(spec.get("col"))
        if not col:
            continue
        bind = f"eq_{idx}"
        sql_parts.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind}))")
        binds[bind] = spec.get("val")
    return sql_parts, binds


def _default_fts_debug(columns: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "enabled": False,
        "mode": "explicit",
        "operator": None,
        "columns": columns or dw_settings.get_fts_columns("Contract"),
        "tokens": [],
        "binds": {},
        "error": None,
        "engine": dw_settings.get_fts_engine("like"),
    }


def _prepare_fts(question: str, *, full_text_search: bool, override: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    if override:
        text = override.get("text") or ""
        operator = override.get("operator")
        sql, binds, debug = build_fts_where("Contract", text, force_operator=operator)
        if override.get("tokens"):
            debug["tokens"] = list(override.get("tokens") or [])
        debug["enabled"] = bool(sql)
    elif full_text_search:
        sql, binds, debug = build_fts_where("Contract", question)
    else:
        return "", {}, _default_fts_debug()

    debug.setdefault("columns", dw_settings.get_fts_columns("Contract"))
    debug.setdefault("tokens", [])
    debug.setdefault("binds", {})
    debug["engine"] = dw_settings.get_fts_engine("like")
    return sql, binds, debug


def _plan_query(
    question: str,
    *,
    eq_filters: Optional[List[Dict[str, Any]]] = None,
    full_text_search: bool = False,
    fts_override: Optional[Dict[str, Any]] = None,
    sort_by: Optional[str] = None,
    sort_desc: bool = True,
) -> Dict[str, Any]:
    eq_filters = list(eq_filters or [])
    default_date_col = dw_settings.get_date_column("REQUEST_DATE")
    sort_column = (sort_by or default_date_col).strip() or "REQUEST_DATE"

    fts_sql, fts_binds, fts_debug = _prepare_fts(question, full_text_search=full_text_search, override=fts_override)
    eq_sqls, eq_binds = _build_eq_sql(eq_filters)

    where_parts: List[str] = []
    if fts_sql:
        where_parts.append(fts_sql)
    if eq_sqls:
        where_parts.append("(" + " AND ".join(eq_sqls) + ")")

    binds: Dict[str, Any] = {}
    binds.update(fts_binds)
    binds.update(eq_binds)

    sql_lines = [
        "SELECT *",
        'FROM "Contract"',
    ]
    if where_parts:
        sql_lines.append("WHERE " + " AND ".join(where_parts))
    direction = "DESC" if sort_desc else "ASC"
    sql_lines.append(f"ORDER BY {sort_column} {direction}")
    sql = "\n".join(sql_lines)

    intent = {
        "question": question,
        "eq_filters": eq_filters,
        "group_by": None,
        "sort_by": sort_column,
        "sort_desc": sort_desc,
        "measure_sql": None,
        "date_column": default_date_col,
        "fts_tokens": list(fts_debug.get("tokens") or []),
        "fts_operator": fts_debug.get("operator") or ("OR" if fts_debug.get("enabled") else None),
    }

    meta = {
        "binds": binds,
        "fts": fts_debug,
        "eq_filters": eq_filters,
        "sort_by": sort_column,
        "sort_desc": sort_desc,
        "gross": False,
        "clarifier_intent": intent,
    }

    debug = {
        "intent": intent,
        "fts": fts_debug,
    }

    return {
        "sql": sql,
        "meta": meta,
        "intent": intent,
        "debug": debug,
    }


def _next_inquiry_id() -> int:
    return next(_INQUIRY_COUNTER)


def save_answer_snapshot(inquiry_id: int, record: Dict[str, Any]) -> None:
    with _SNAPSHOT_LOCK:
        _SNAPSHOTS[inquiry_id] = copy.deepcopy(record)


def load_answer_snapshot(inquiry_id: int) -> Optional[Dict[str, Any]]:
    with _SNAPSHOT_LOCK:
        rec = _SNAPSHOTS.get(inquiry_id)
        return copy.deepcopy(rec) if rec is not None else None


def _rate_comment_hints(comment: str) -> Dict[str, Any]:
    hints: Dict[str, Any] = {}
    pieces = [p.strip() for p in (comment or "").split(";") if p.strip()]
    for piece in pieces:
        if ":" not in piece:
            continue
        key, value = piece.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if not value:
            continue
        if key == "fts":
            tokens = [tok.strip() for tok in re.split(r"[|,]", value) if tok.strip()]
            if tokens:
                operator = "AND" if " and " in value.lower() and "|" not in value else "OR"
                hints["fts_tokens"] = tokens
                hints["fts_operator"] = operator
                glue = " AND " if operator == "AND" else " OR "
                hints["fts_text"] = glue.join(tokens)
        elif key == "eq":
            eq_filters = _parse_eq_filters(value)
            if eq_filters:
                hints["eq_filters"] = eq_filters
        elif key == "order_by":
            parts = value.split()
            if parts:
                hints["sort_by"] = _normalize_col(parts[0])
                if len(parts) > 1:
                    hints["sort_desc"] = parts[1].strip().lower() != "asc"
        elif key == "group_by":  # pragma: no cover - currently unused but ready
            cols = [
                _normalize_col(c)
                for c in re.split(r",", value)
                if c.strip()
            ]
            if cols:
                hints["group_by"] = cols
        elif key == "gross":
            hints["gross"] = value.strip().lower() in {"1", "true", "yes", "y"}
    return hints


@dw_bp.post("/answer")
def answer() -> Any:
    payload = request.get_json(force=True) or {}
    question = payload.get("question", "")
    full_text_search = bool(payload.get("full_text_search"))

    eq_filters = _parse_eq_filters(question)
    plan = _plan_query(
        question,
        eq_filters=eq_filters,
        full_text_search=full_text_search,
    )

    inquiry_id = _next_inquiry_id()
    plan["meta"]["canary"] = choose_canary(inquiry_id)

    examples = retrieve_examples_for_question(question)
    if examples:
        plan["meta"]["examples_used"] = [
            {"q": row.get("q"), "score": row.get("score")}
            for row in examples[:3]
        ]

    explain_payload = {
        "intent": plan["intent"],
        "fts": plan["meta"].get("fts", {}),
        "sql": plan["sql"],
        "meta": plan["meta"],
    }
    user_explain = build_user_explain(explain_payload)
    plan["meta"]["user_explain"] = user_explain
    plan["meta"]["explain"] = plan["meta"].get("explain") or user_explain

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "sql": plan["sql"],
        "meta": plan["meta"],
        "debug": plan["debug"],
        "rows": [],
    }

    try:
        explain_text, explain_struct = build_explain(response)
        response["explain"] = explain_text
        response.setdefault("debug", {}).setdefault("explain_struct", explain_struct)
    except Exception:
        # Never block the response because of explain formatting.
        pass

    snapshot = {
        "inquiry_id": inquiry_id,
        "question": question,
        "sql": plan["sql"],
        "meta": plan["meta"],
        "intent": plan["intent"],
        "debug": plan["debug"],
        "created_at": time.time(),
        "payload": payload,
    }
    save_answer_snapshot(inquiry_id, snapshot)

    return jsonify(response)


@dw_bp.post("/admin/explain")
def admin_explain() -> Any:
    """Render a lightweight HTML view of the explain payload."""

    payload = request.get_json(force=True) or {}
    text, struct = build_explain(payload)
    debug = payload.get("debug", {}) or {}
    sql = payload.get("sql", "") or ""
    binds = (payload.get("meta") or {}).get("binds") or {}
    return render_template(
        "explain.html",
        explain_text=text,
        explain_struct=struct,
        sql=sql,
        debug=debug,
        binds=binds,
    )


@dw_bp.post("/rate")
def rate() -> Any:
    payload = request.get_json(force=True) or {}
    inquiry_id = payload.get("inquiry_id")
    if not inquiry_id:
        return jsonify({"ok": False, "error": "Missing inquiry_id"}), 400

    snapshot = load_answer_snapshot(int(inquiry_id))
    if not snapshot:
        return jsonify({"ok": False, "error": "Not found"}), 404

    hints = _rate_comment_hints(payload.get("comment", ""))
    orig_intent = snapshot.get("intent", {})

    eq_filters = hints.get("eq_filters", orig_intent.get("eq_filters") or [])
    sort_by = hints.get("sort_by") or orig_intent.get("sort_by")
    sort_desc = hints.get("sort_desc") if "sort_desc" in hints else orig_intent.get("sort_desc", True)

    fts_override = None
    if hints.get("fts_tokens"):
        fts_override = {
            "tokens": hints.get("fts_tokens"),
            "operator": hints.get("fts_operator"),
            "text": hints.get("fts_text"),
        }

    plan = _plan_query(
        snapshot.get("question", ""),
        eq_filters=eq_filters,
        full_text_search=bool(fts_override),
        fts_override=fts_override,
        sort_by=sort_by,
        sort_desc=bool(sort_desc),
    )

    plan["meta"]["source"] = "rate"
    plan["meta"]["user_explain"] = build_user_explain(
        {
            "intent": plan["intent"],
            "fts": plan["meta"].get("fts", {}),
            "sql": plan["sql"],
            "meta": plan["meta"],
        }
    )
    plan["meta"]["explain"] = plan["meta"].get("explain") or plan["meta"]["user_explain"]

    final_sql = plan["sql"]
    try:
        save_example_if_positive(
            inquiry_id=int(inquiry_id),
            question=snapshot.get("question", ""),
            sql=final_sql,
            rating=payload.get("rating"),
        )
    except Exception:
        pass

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "sql": final_sql,
        "meta": plan["meta"],
        "debug": plan["debug"],
    }
    return jsonify(response)


@dw_bp.get("/explain")
def explain_view() -> Any:
    enabled = SETTINGS.get_bool("DW_EXPLAIN_UI_ENABLED", default=True, scope="namespace")
    if enabled is False:
        return jsonify({"ok": False, "error": "Explain UI is disabled"}), 403

    inquiry_id = request.args.get("inquiry_id", type=int)
    if not inquiry_id:
        return jsonify({"ok": False, "error": "Missing inquiry_id"}), 400

    rec = load_answer_snapshot(inquiry_id)
    if not rec:
        return jsonify({"ok": False, "error": "Not found"}), 404

    intent = rec.get("intent") or rec.get("debug", {}).get("intent", {})
    meta = rec.get("meta", {})
    fts_meta = meta.get("fts") or rec.get("debug", {}).get("fts", {})

    data = {
        "inquiry_id": inquiry_id,
        "question": rec.get("question"),
        "intent": intent,
        "fts": fts_meta,
        "sql": rec.get("sql"),
        "meta": meta,
    }
    data["user_explain"] = build_user_explain(
        {"intent": intent, "fts": fts_meta, "sql": data["sql"], "meta": meta}
    )
    return render_template("dw/explain.html", data=data)


__all__ = [
    "dw_bp",
    "answer",
    "rate",
    "explain_view",
    "admin_explain",
    "save_answer_snapshot",
    "load_answer_snapshot",
]
