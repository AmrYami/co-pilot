from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

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


def _settings_lookup(settings_obj: Any, key: str, namespace: Optional[str]) -> Any:
    if settings_obj is None:
        return None
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
        if value:
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
    inquiry_id = int(data.get("inquiry_id") or 0)
    rating = int(data.get("rating") or 0)
    feedback = (data.get("feedback") or "").strip() or None
    comment = (data.get("comment") or "").strip()
    if not comment and feedback:
        comment = feedback
    structured_hints = parse_rate_comment_structured(comment or "")
    structured_hints["full_text_search"] = bool(structured_hints.get("fts_tokens"))
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
    settings_obj = app.config.get("SETTINGS") if app else None
    fts_columns = _resolve_fts_columns(settings_obj, effective_namespace)
    eq_allowed = _resolve_eq_columns(settings_obj, effective_namespace)

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
        except Exception as exc:
            hints_debug = {"error": str(exc)}

    structured_hint_present = bool(comment) and any(
        (
            structured_hints.get("fts_tokens"),
            structured_hints.get("eq_filters"),
            structured_hints.get("group_by"),
            structured_hints.get("sort_by"),
            structured_hints.get("gross") is not None,
        )
    )
    rate_sql: Optional[str] = None
    rate_binds: Dict[str, str] = {}
    rate_debug: Dict[str, Any] = {}
    if structured_hint_present:
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

    if rate_sql:
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
            debug_section = payload.setdefault("debug", {})
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
            payload.setdefault("debug", {})["rate_hints"] = hints_debug
        return payload

    question_text = inquiry_row[1] if inquiry_row and len(inquiry_row) >= 2 else None

    if rating >= 4 and question_text and intent_snapshot:
        try:
            save_positive_rule(engine, question_text, intent_snapshot)
        except Exception:
            pass

    if rating <= 2 and comment and question_text and hints_dict:
        store_rate_hints(question_text, hints_dict)
        try:
            save_patch(engine, inquiry_id, question_text, rating, comment, hints_dict)
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
