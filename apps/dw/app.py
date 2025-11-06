"""DocuWare DW blueprint backed by a deterministic contract planner."""
from __future__ import annotations

import copy
import json
import os
import csv
import logging
import re
import time
from dataclasses import dataclass
from collections import OrderedDict
from os import getenv
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # Ensure emoji compatibility for recognizers-text
    import emoji  # type: ignore

    if not hasattr(emoji, "UNICODE_EMOJI") and hasattr(emoji, "EMOJI_DATA"):
        setattr(emoji, "UNICODE_EMOJI", getattr(emoji, "EMOJI_DATA"))
except Exception:
    pass

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
from apps.dw.utils import env_flag
from apps.dw.order_utils import normalize_order_hint
from apps.dw.fts_utils import DEFAULT_CONTRACT_FTS_COLUMNS
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.settings_utils import load_explicit_filter_columns
from apps.dw.tables.contracts import build_contract_sql
from apps.mem.kv import get_settings_for_namespace
from apps.dw.online_learning import load_recent_hints
from apps.dw.builder import _where_from_eq_filters
from apps.dw import builder as _builder_mod
from apps.dw.db import get_memory_engine, get_memory_session
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
from apps.dw.learning_store import _merge_eq_filters_prefer_question as _merge_eq_prefer_question  # prefer-question EQ merge
try:
    from apps.dw.learning import _merge_or_groups_prefer_question as _merge_or_prefer_question
except Exception:  # pragma: no cover - fallback when learning module unavailable
    _merge_or_prefer_question = None
from apps.dw.explain import build_explain
from .contracts.fts import extract_fts_terms, build_fts_where_groups
from .contracts.filters import parse_explicit_filters
from .contracts.contract_planner import plan_contract_query
from .rating import rate_bp
from .answer.nlu_eq_parser import parse_from_question as parse_eq_inline
from .tests.rate_suite import rate_tests_bp
from .tests.routes import tests_bp as rate_builder_tests_bp
from .tests.golden_runner_rate import golden_rate_bp
from apps.dw.intent_parser import DwQuestionParser, ParsedIntent
try:
    from apps.dw.learning import load_rules_for_question as _load_rules_by_sig
    _LOAD_RULES_SRC = "learning"
except ImportError:  # pragma: no cover - fallback when packaged/renamed
    from apps.dw.learning_store import load_rules_for_question as _load_rules_by_sig  # type: ignore
    _LOAD_RULES_SRC = "learning_store"

LOGGER = logging.getLogger("dw.app")

def _bool_env(name: str, default: bool = True) -> bool:
    """Coerce typical truthy env values."""
    try:
        v = os.getenv(name, "1" if default else "0")
        return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    except Exception:
        return default

def _ensure_exports_dir() -> str:
    path = os.getenv("DW_EXPORTS_DIR", "exports")
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

    # Unconditional alias assignment capture (e.g., DEPARTMENTS = X), independent of inline parser
    try:
        alias_map3_raw = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}
        alias_map3: Dict[str, List[str]] = {}
        if isinstance(alias_map3_raw, dict):
            for k, cols in alias_map3_raw.items():
                if not isinstance(cols, (list, tuple, set)):
                    continue
                bucket: List[str] = []
                seen3: set[str] = set()
                for c in cols:
                    s3 = str(c or "").strip().upper()
                    if s3 and s3 not in seen3:
                        seen3.add(s3)
                        bucket.append(s3)
                if bucket:
                    alias_map3[str(k or "").strip().upper()] = bucket
        if not alias_map3:
            # conservative defaults when mapping is unavailable
            targets = [
                "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4",
                "DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
                "OWNER_DEPARTMENT",
            ]
            alias_map3 = {"DEPARTMENTS": targets, "DEPARTMENT": targets}
        alias_keys3 = list(alias_map3.keys())
        if alias_keys3:
            import re as _re
            alias_pat3 = r"(?:" + "|".join(_re.escape(a) for a in alias_keys3) + r")"
            rx3 = _re.compile(rf"(?i)\b({alias_pat3})\s*=\s*([^\n\r;]+)")
            found3: List[tuple[str,str]] = []
            for m in rx3.finditer(question or ""):
                kk = (m.group(1) or "").strip().upper()
                vv = (m.group(2) or "").strip().strip("'\"")
                if kk and vv:
                    found3.append((kk, vv))
            if found3:
                eq3 = online_intent.setdefault("eq_filters", [])
                # avoid duplicates
                present = {(str(it[0]).upper(), tuple(it[1]) if isinstance(it, (list,tuple)) and len(it)==2 and isinstance(it[1], (list,tuple)) else None) for it in eq3 if isinstance(it,(list,tuple))}
                for kkey, vval in found3:
                    candidate = (kkey, (vval,))
                    if candidate not in present:
                        eq3.append([kkey, [vval]])
                _expand_eq_aliases_with_map(online_intent, alias_map3)
    except Exception:
        pass
    return path

def _export_rows_to_csv(rows, columns, *, inquiry_id=None):
    """Best-effort CSV export for /dw/answer results. Returns relative file path on success."""
    if not isinstance(rows, (list, tuple)) or not isinstance(columns, (list, tuple)) or not columns:
        return None
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"dw_answer_{inquiry_id or ts}.csv"
    fpath = os.path.join(_ensure_exports_dir(), fname)
    try:
        with open(fpath, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(list(columns))
            for r in rows:
                w.writerow(list(r))
        logging.getLogger("dw").info(
            {
                "event": "answer.export.csv.ok",
                "file": fpath,
                "rows": len(rows),
                "cols": len(columns),
                "inquiry_id": inquiry_id,
            }
        )
        return fpath
    except Exception as exc:  # pragma: no cover
        logging.getLogger("dw").warning({"event": "answer.export.csv.err", "err": str(exc)})
        return None

def _normalize_question_text(value: Any) -> str:
    text_value = "" if value is None else str(value)
    return " ".join(text_value.strip().lower().split())

def _get_namespace_mapping(settings_obj, namespace: str, key: str, default=None):
    """Safely fetch a namespaced JSON mapping setting.

    Tries `get_json(key, scope='namespace', namespace=...)`, then falls back to
    `get(key, scope='namespace', namespace=...)`, then bare `get(key)`.
    Returns `default` if nothing suitable is found. Ensures a dict.
    """
    try:
        getter_json = getattr(settings_obj, "get_json", None)
        if callable(getter_json):
            try:
                val = getter_json(key, scope="namespace", namespace=namespace)
            except TypeError:
                # older signatures without scope/namespace
                val = getter_json(key)
            if isinstance(val, dict):
                return val
            if val is not None:
                try:
                    obj = json.loads(val)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass
    except Exception:
        pass
    try:
        getter = getattr(settings_obj, "get", None)
        if callable(getter):
            try:
                val = getter(key, scope="namespace", namespace=namespace)
            except TypeError:
                val = getter(key)
            if isinstance(val, dict):
                return val
            if val is not None:
                try:
                    obj = json.loads(val)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass
    except Exception:
        pass
    return default if isinstance(default, dict) else {}


def load_persisted_rules(session, limit: int = 50) -> List[Dict[str, Any]]:
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT id, rule_kind,
                           COALESCE(rule_payload, '{}'::jsonb) AS rule_payload,
                           enabled
                      FROM dw_rules
                     WHERE enabled = TRUE
                     ORDER BY id DESC
                     LIMIT :limit
                    """
                ),
                {"limit": limit},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        LOGGER.warning("[dw] rules loader fell back: %s", exc)
        return []

    loaded: List[Dict[str, Any]] = []
    for row in rows:
        payload = row.get("rule_payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        loaded.append(
            {
                "id": row.get("id"),
                "kind": row.get("rule_kind"),
                "payload": payload,
            }
        )
    return loaded


def _load_rate_hint_seed(question: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    normalized_question = _normalize_question_text(question)
    if not normalized_question:
        return {}, {}

    try:
        session = get_memory_session()
    except Exception as exc:
        LOGGER.warning("[dw] unable to open memory session for rate hints: %s", exc)
        return {}, {}

    try:
        rules = load_persisted_rules(session)
        hints: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        origin_ids: List[int] = []
        for entry in rules:
            if (entry.get("kind") or "").strip().lower() != "rate_hint":
                continue
            payload = entry.get("payload") or {}
            if not isinstance(payload, dict):
                continue
            payload_copy = dict(payload)
            meta = {"rule_id": entry.get("id")}
            hints.append((payload_copy, meta))
            origin_candidate = payload.get("origin_inquiry_id")
            try:
                origin_id = int(origin_candidate) if origin_candidate is not None else None
            except (TypeError, ValueError):
                origin_id = None
            if origin_id is not None:
                origin_ids.append(origin_id)

        if not hints:
            return {}, {}

        id_to_norm: Dict[int, str] = {}
        if origin_ids:
            try:
                mem_bind = session.get_bind()
            except Exception:
                mem_bind = None
            if mem_bind is None:
                try:
                    mem_bind = get_memory_engine()
                except Exception:
                    mem_bind = None

            has_q_norm = False
            if mem_bind is not None:
                try:
                    with mem_bind.connect() as cx:
                        has_q_norm = (
                            cx.execute(
                                text(
                                    """
                                    SELECT 1
                                      FROM information_schema.columns
                                     WHERE table_name = 'mem_inquiries'
                                       AND column_name = 'q_norm'
                                     LIMIT 1
                                    """
                                )
                            ).first()
                            is not None
                        )
                except Exception:
                    has_q_norm = False

            sql_norm = (
                text(
                    """
                    SELECT id,
                           COALESCE(NULLIF(q_norm, ''), LOWER(TRIM(question))) AS norm
                      FROM mem_inquiries
                     WHERE id = ANY(:ids)
                    """
                )
                if has_q_norm
                else text(
                    """
                    SELECT id, LOWER(TRIM(question)) AS norm
                      FROM mem_inquiries
                     WHERE id = ANY(:ids)
                    """
                )
            )

            rows = (
                session.execute(
                    sql_norm,
                    {"ids": origin_ids},
                )
                .mappings()
                .all()
            )
            for row in rows:
                norm = row.get("norm")
                if isinstance(norm, str):
                    id_to_norm[int(row.get("id"))] = _normalize_question_text(norm)

        for payload, meta in hints:
            origin_candidate = payload.get("origin_inquiry_id")
            try:
                origin_id = int(origin_candidate) if origin_candidate is not None else None
            except (TypeError, ValueError):
                origin_id = None
            if origin_id is None:
                continue
            if id_to_norm.get(origin_id) == normalized_question:
                return payload, meta

        return {}, {}
    finally:
        session.close()
dw_bp = Blueprint("dw", __name__)
init_db()
dw_bp.register_blueprint(rate_bp, url_prefix="")
dw_bp.register_blueprint(rate_tests_bp, url_prefix="/tests")
dw_bp.register_blueprint(rate_builder_tests_bp, url_prefix="")
dw_bp.register_blueprint(golden_rate_bp, url_prefix="")


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
    # Expose a lightweight counter alongside meta.rows
    try:
        response.setdefault("row_count", rows_count)
    except Exception:
        pass

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

    # Optional CSV export (default ON; can be disabled via DW_ANSWER_EXPORT_CSV=0)
    try:
        want_csv = (
            bool((payload or {}).get("export_csv"))
            or str((payload or {}).get("export") or "").strip().lower() in {"csv", "true", "1", "yes"}
            or _bool_env("DW_ANSWER_EXPORT_CSV", True)
        )
        if want_csv and isinstance(response.get("rows"), list) and isinstance(response.get("columns"), list):
            csv_path = _export_rows_to_csv(response["rows"], response["columns"], inquiry_id=response.get("inquiry_id"))
            if csv_path:
                meta["export_csv"] = csv_path
                response["export_csv"] = csv_path
    except Exception:
        pass

    debug_section = response.setdefault("debug", {}) if isinstance(response, dict) else {}
    precomputed_boolean_debug = None
    if isinstance(debug_section, dict):
        precomputed_boolean_debug = debug_section.pop("_precomputed_boolean_debug", None)
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
            question_text = _extract_question_for_debug(
                payload if isinstance(payload, dict) else {}, response
            )
            fts_columns = _extract_fts_columns_for_debug(response)
            plan = (
                precomputed_boolean_debug
                if isinstance(precomputed_boolean_debug, dict)
                else build_boolean_debug(question_text, fts_columns)
            )
            blocks = plan.get("blocks", []) or []
            summary_text = plan.get("summary", "") or ""
            debug_section["boolean_groups"] = blocks
            debug_section["boolean_groups_text"] = summary_text
            where_text = plan.get("where_text")
            if where_text:
                debug_section["where_text"] = where_text
            binds_text = plan.get("binds_text")
            if binds_text:
                debug_section["binds_text"] = binds_text
            if blocks and isinstance(blocks[0], dict):
                first_block = blocks[0]
                block_fts = (
                    first_block.get("fts") if isinstance(first_block.get("fts"), list) else []
                )
                if not block_fts and intent_tokens:
                    first_block["fts"] = intent_tokens
                    fts_text = "FTS(" + " OR ".join(intent_tokens) + ")"
                    if summary_text.startswith("(") and len(summary_text) > 1:
                        debug_section["boolean_groups_text"] = (
                            "(" + f"{fts_text} AND " + summary_text[1:]
                        )
                    else:
                        debug_section["boolean_groups_text"] = f"({fts_text})"
        except Exception as exc:  # pragma: no cover - debug best-effort
            debug_section["boolean_groups_error"] = str(exc)

    # Log the full response (without rows) for observability
    try:
        _logger = logging.getLogger("dw")
        resp_copy = dict(response)
        rows_field = resp_copy.get("rows")
        rows_count = 0
        try:
            rows_count = len(rows_field) if isinstance(rows_field, list) else int(rows_field or 0)
        except Exception:
            rows_count = 0
        if "rows" in resp_copy:
            resp_copy["rows"] = f"omitted({rows_count})"
        _logger.info({"event": "answer.response.full", "response": resp_copy})
    except Exception:
        pass

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
            # Canonicalize remaining eq filter values to uppercase for stability
            try:
                eq_clean = sanitized_patch.get("eq_filters")
                if isinstance(eq_clean, list):
                    normalized_eq: List[Any] = []
                    for entry in eq_clean:
                        if isinstance(entry, dict):
                            updated = dict(entry)
                            val = updated.get("val")
                            values = updated.get("values")
                            if isinstance(val, str):
                                updated["val"] = val.strip().upper()
                            if isinstance(values, list):
                                updated["values"] = [v.strip().upper() if isinstance(v, str) else v for v in values]
                            normalized_eq.append(updated)
                        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                            col = entry[0]
                            vals = entry[1]
                            if isinstance(vals, list):
                                vals = [v.strip().upper() if isinstance(v, str) else v for v in vals]
                            elif isinstance(vals, str):
                                vals = [vals.strip().upper()]
                            normalized_eq.append([col, vals])
                        else:
                            normalized_eq.append(entry)
                    sanitized_patch["eq_filters"] = normalized_eq
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
    # Guard: collapse duplicate ASC/DESC tokens in ORDER BY
    try:
        sql = _normalize_order_by_directions(sql)
    except Exception:
        pass
    with engine.connect() as cx:  # type: ignore[union-attr]
        rs = cx.execute(text(sql), safe_binds)
        cols = list(rs.keys()) if hasattr(rs, "keys") else []
        rows = [list(r) for r in rs.fetchall()]
    return rows, cols, {"rows": len(rows)}


def _normalize_order_by_directions(sql: str) -> str:
    """Collapse repeated direction tokens like 'DESC DESC' within ORDER BY clauses."""
    def _fix(m):
        clause = m.group(0)
        return re.sub(r"(?i)\b(DESC|ASC)\s+\1\b", r"\1", clause)
    return re.sub(r"(?i)ORDER\s+BY\s+[^\n;]+", _fix, sql)


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


# --- Signature-first light intent helpers ------------------------------------
EMAIL_RE = re.compile(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$")
NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?$")

_COMPARISON_TEXT_SYNONYMS: Dict[str, List[str]] = {
    "gte": [
        "greater than or equal to",
        "at least",
        "not less than",
    ],
    "gt": [
        "greater than",
        "more than",
        "over",
        "above",
    ],
    "lte": [
        "less than or equal to",
        "at most",
        "not more than",
    ],
    "lt": [
        "less than",
        "under",
        "below",
    ],
    "eq": [
        "equal to",
        "equals",
        "equal",
    ],
}

_COMPARISON_SYMBOLS: Dict[str, str] = {
    ">=": "gte",
    "≥": "gte",
    ">": "gt",
    "<=": "lte",
    "≤": "lte",
    "<": "lt",
    "==": "eq",
    "=": "eq",
}

_COMPARISON_TRAILING_MARKERS = sorted(
    {
        "greater than or equal to",
        "greater than",
        "more than",
        "over",
        "above",
        "at least",
        "less than or equal to",
        "less than",
        "under",
        "below",
        "at most",
        "equals",
        "equal to",
        "equal",
        ">=",
        "<=",
        ">",
        "<",
        "≥",
        "≤",
    },
    key=len,
    reverse=True,
)

_LARK_PARSER: Optional[DwQuestionParser] = None
_NUM_MODEL: Any = None


@dataclass(frozen=True)
class IntentPipelineConfig:
    pipeline_version: str
    parser: str
    use_lark: bool
    eq_value_policy: str
    tail_trim: bool
    signature_mode: str

    @property
    def cache_tag(self) -> str:
        return "|".join(
            [
                self.pipeline_version or "legacy",
                self.parser or "legacy",
                self.eq_value_policy or "default",
                "tail" if self.tail_trim else "notail",
            ]
        )


_INTENT_CACHE_MAX = 256
_INTENT_CACHE: OrderedDict[Any, Dict[str, Any]] = OrderedDict()


def _resolve_intent_pipeline_config() -> IntentPipelineConfig:
    pipeline = str(os.getenv("DW_INTENT_PIPELINE", "") or "").strip().lower()
    parser_env = str(os.getenv("DW_PARSER", "") or "").strip().lower()
    eq_policy = str(os.getenv("DW_EQ_VALUE_POLICY", "") or "").strip().lower() or "question_only"
    signature_mode = str(os.getenv("DW_LEARNING_RULES_MATCH", "signature_first") or "").strip().lower()
    tail_trim = _bool_env("DW_FTS_TAIL_TRIM", default=True)

    if not pipeline and _bool_env("DW_USE_LARK_INTENT", default=False):
        pipeline = "v2"
    if parser_env not in {"lark", "legacy"}:
        parser_env = "lark" if pipeline == "v2" else "legacy"

    use_lark = parser_env == "lark" or pipeline == "v2"

    return IntentPipelineConfig(
        pipeline_version=pipeline or "v1",
        parser=parser_env,
        use_lark=use_lark,
        eq_value_policy=eq_policy or "question_only",
        tail_trim=tail_trim,
        signature_mode=signature_mode or "signature_first",
    )


def _intent_cache_get(key: Any) -> Optional[Dict[str, Any]]:
    cached = _INTENT_CACHE.get(key)
    if cached is None:
        return None
    _INTENT_CACHE.move_to_end(key)
    return copy.deepcopy(cached)


def _intent_cache_put(key: Any, value: Dict[str, Any]) -> None:
    if key in _INTENT_CACHE:
        _INTENT_CACHE.move_to_end(key)
    _INTENT_CACHE[key] = copy.deepcopy(value)
    while len(_INTENT_CACHE) > _INTENT_CACHE_MAX:
        _INTENT_CACHE.popitem(last=False)


def _filter_fts_groups(groups: Optional[List[List[str]]], *, min_length: int = 2) -> List[List[str]]:
    if not groups:
        return []
    filtered: List[List[str]] = []
    for group in groups:
        tokens: List[str] = []
        for token in group or []:
            token_text = (token or "").strip()
            if not token_text:
                continue
            if len(token_text) < min_length:
                continue
            tokens.append(token_text)
        if tokens:
            filtered.append(tokens)
    return filtered


def _val_type(v: str) -> str:
    s = (v or "").strip()
    if EMAIL_RE.match(s):
        return "EMAIL"
    if NUMBER_RE.match(s):
        return "NUMBER"
    return "TEXT"


def _normalize_numbers_with_recognizers(text: str) -> str:
    global _NUM_MODEL
    if not text:
        return text
    if _NUM_MODEL is False:
        # Retry initialization if recognizers-text became available after first failure.
        try:
            import importlib  # noqa: F401
            import recognizers_text  # type: ignore  # noqa: F401
        except ImportError:
            _NUM_MODEL = False
            return text
        _NUM_MODEL = None
    if _NUM_MODEL is None:
        try:
            try:
                import emoji  # type: ignore
                if not hasattr(emoji, "UNICODE_EMOJI") and hasattr(emoji, "EMOJI_DATA"):
                    setattr(emoji, "UNICODE_EMOJI", getattr(emoji, "EMOJI_DATA"))
            except Exception:
                pass
            from recognizers_text import Culture  # type: ignore
            from recognizers_number import NumberRecognizer  # type: ignore
        except ImportError:
            _NUM_MODEL = False
            return text
        try:
            recognizer = NumberRecognizer(Culture.English)
            _NUM_MODEL = recognizer.get_number_model()
        except Exception:
            _NUM_MODEL = False
            return text
    if not _NUM_MODEL:
        return text
    try:
        results = _NUM_MODEL.parse(text)
    except Exception:
        return text
    if not results:
        return text
    updated = text
    for res in sorted(results, key=lambda r: getattr(r, "start", 0) or 0, reverse=True):
        start = getattr(res, "start", None)
        end = getattr(res, "end", None)
        length = getattr(res, "length", None)
        resolution = getattr(res, "resolution", {}) or {}
        if start is None or length is None:
            if start is None:
                continue
            length = len(getattr(res, "text", ""))
            if not length and end is not None and end > start:
                length = end - start
            if length <= 0:
                continue
        value = resolution.get("value") if isinstance(resolution, dict) else None
        if value is None:
            continue
        replacement = str(value)
        try:
            updated = updated[:start] + replacement + updated[start + length :]
        except Exception:
            continue
    return updated


def _augment_light_intent_with_aliases(
    question: str,
    light_intent: dict,
    alias_keys: List[str],
    column_tokens: List[str],
    comparison_markers: List[str],
) -> None:
    """Augment light intent with alias-based EQ for signature matching only.

    Scans the question for patterns like "ALIAS = value1 or value2" for any alias
    key from DW_EQ_ALIAS_COLUMNS and injects a logical EQ on that alias (not the
    real columns). This is used only to build a stable, value-agnostic signature
    so signature-first rule matching works even when users vary phrasing/values.
    """
    if not question or not isinstance(light_intent, dict) or not alias_keys:
        return
    q = question or ""
    eq_filters = light_intent.setdefault("eq_filters", [])
    stop_lookahead = r"(?=\s+and\b|[;,]|$)"

    alias_tokens_lower = [str(tok or "").strip().lower() for tok in alias_keys if str(tok or "").strip()]
    column_tokens_lower: List[str] = []
    for col in (column_tokens or []):
        token = str(col or "").strip()
        if not token:
            continue
        token = token.strip('"').strip()
        if "." in token:
            token = token.split(".")[-1]
        token = token.strip()
        if not token:
            continue
        column_tokens_lower.append(token.lower())
    comparison_tokens_lower = [mk.lower() for mk in comparison_markers or []]

    def _strip_trailing_clause(text: str) -> str:
        if not text:
            return text
        lower = text.lower()
        idx = lower.find(" and ")
        while idx != -1:
            tail = lower[idx + 5 :].strip()
            if (
                any(tail.startswith(tok) for tok in alias_tokens_lower)
                or any(tail.startswith(tok) for tok in column_tokens_lower)
                or any(tail.startswith(tok) for tok in comparison_tokens_lower)
            ):
                return text[:idx].strip()
            idx = lower.find(" and ", idx + 5)
        return text.strip()

    for raw_key in alias_keys:
        if not isinstance(raw_key, str):
            continue
        key = raw_key.strip()
        if not key:
            continue
        try:
            alias_norm = key.upper()

            def _already_has_alias() -> bool:
                for it in eq_filters:
                    if isinstance(it, (list, tuple)) and len(it) == 2 and str(it[0]).upper() == alias_norm:
                        return True
                    if isinstance(it, dict) and str(it.get("col") or it.get("field") or "").upper() == alias_norm:
                        return True
                return False

            if _already_has_alias():
                continue

            escaped = re.escape(key)
            patterns = [
                re.compile(rf"(?i)\b{escaped}\b\s*=\s*([^\n\r;]+?){stop_lookahead}"),
                re.compile(rf"(?i)\b{escaped}\b\s+(?:has|contains)\s+([^\n\r;]+?){stop_lookahead}"),
            ]

            captured_vals: List[str] = []
            for pat in patterns:
                for match in pat.finditer(q):
                    rhs = match.group(1).strip()
                    if not rhs:
                        continue
                    parts = re.split(r"(?i)\s*\bor\b\s*|,", rhs)
                    vals = [p.strip(" '\"\t()") for p in parts if p and p.strip()]
                    if vals:
                        captured_vals.extend(vals)
                if captured_vals:
                    break

            if captured_vals:
                cleaned_vals = []
                for val in captured_vals:
                    stripped = _strip_trailing_clause(val)
                    if stripped:
                        cleaned_vals.append(stripped)
                if cleaned_vals:
                    eq_filters.append([alias_norm, cleaned_vals])
        except Exception:
            continue


def _normalize_numeric_value(raw: str) -> Any:
    if raw is None:
        return raw
    text = str(raw).strip()
    if not text:
        return text
    text = text.replace(",", "")
    text = text.replace("%", "")
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _extract_comparison_filters(question: str, allowed_cols: Sequence[str]) -> List[Dict[str, Any]]:
    if not question:
        return []
    results: List[Dict[str, Any]] = []
    seen_spans: set[Tuple[int, int]] = set()
    text = question
    sorted_phrases = {
        op: sorted(phrases, key=len, reverse=True) for op, phrases in _COMPARISON_TEXT_SYNONYMS.items()
    }
    for raw_col in (allowed_cols or []):
        col = str(raw_col or "").strip()
        if not col:
            continue
        col_pat = re.escape(col)
        symbol_regex = re.compile(rf"(?i)\b{col_pat}\b\s*(>=|<=|==|=|>|<|≥|≤)\s*([-+]?\d+(?:\.\d+)?)")
        for match in symbol_regex.finditer(text):
            span = match.span()
            if span in seen_spans:
                continue
            op_symbol = match.group(1)
            val_text = match.group(2)
            op = _COMPARISON_SYMBOLS.get(op_symbol, "eq")
            value = _normalize_numeric_value(val_text)
            results.append(
                {
                    "col": str(col).upper(),
                    "val": value,
                    "op": op,
                    "ci": False,
                    "trim": False,
                }
            )
            seen_spans.add(span)
        for op, phrases in sorted_phrases.items():
            for phrase in phrases:
                phrase_pat = re.escape(phrase)
                regex = re.compile(rf"(?i)\b{col_pat}\b\s+{phrase_pat}\s+([-+]?\d+(?:\.\d+)?)")
                for match in regex.finditer(text):
                    span = match.span()
                    if span in seen_spans:
                        continue
                    value = _normalize_numeric_value(match.group(1))
                    results.append(
                        {
                            "col": str(col).upper(),
                            "val": value,
                            "op": op,
                            "ci": False,
                            "trim": False,
                        }
                    )
                    seen_spans.add(span)
    return results


def _build_light_intent_from_question(q: str, allowed_cols) -> dict:
    """Build a lightweight intent used for signature + learning overlays."""
    config = _resolve_intent_pipeline_config()
    namespace = _ns()
    normalized_question = _normalize_question_text(q or "")
    normalized_allowed = tuple(
        sorted(
            {
                str(col or "").strip().strip('"').upper()
                for col in (allowed_cols or [])
                if str(col or "").strip()
            }
        )
    )
    cache_key = (config.cache_tag, namespace, normalized_question, normalized_allowed)
    cached = _intent_cache_get(cache_key)
    if cached is not None:
        return cached

    settings_obj = get_settings()
    alias_map = _get_namespace_mapping(settings_obj, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}

    question_text = q or ""
    if config.use_lark and str(os.getenv("DW_NUM_EXTRACTOR", "")).strip().lower() == "recognizers_text":
        question_text = _normalize_numbers_with_recognizers(question_text)

    parser_used = "legacy"
    try:
        if config.use_lark:
            intent = _build_light_intent_via_lark(
                question_text,
                allowed_cols,
                alias_map,
                config,
            )
            parser_used = "lark"
        else:
            intent = _build_light_intent_via_regex(question_text, allowed_cols, config)
    except Exception as exc:
        logging.getLogger("dw").warning(
            {"event": "answer.intent.parser.fallback", "parser": "legacy", "err": str(exc)}
        )
        intent = _build_light_intent_via_regex(question_text, allowed_cols, config)
        parser_used = "legacy"

    try:
        _maybe_apply_entity_status_aggregation_heuristic(q or "", intent, allowed_cols)
    except Exception:
        pass

    meta = intent.setdefault("_meta", {})
    segments = meta.get("segments") or {}
    meta.update(
        {
            "pipeline": config.pipeline_version,
            "parser": parser_used,
            "question_norm": normalized_question,
            "allowed_cols": list(normalized_allowed),
            "eq_value_policy": config.eq_value_policy,
            "signature_mode": config.signature_mode,
        }
    )

    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.segments.parsed",
                "parser": parser_used,
                "pipeline": config.pipeline_version,
                "segments": segments,
            }
        )
    except Exception:
        pass

    _intent_cache_put(cache_key, intent)
    return intent


def _get_lark_parser() -> DwQuestionParser:
    global _LARK_PARSER
    if _LARK_PARSER is None:
        _LARK_PARSER = DwQuestionParser()
    return _LARK_PARSER


def _build_light_intent_via_lark(
    question: str,
    allowed_cols: Optional[Sequence[str]],
    alias_map: Optional[Dict[str, Sequence[str]]],
    config: IntentPipelineConfig,
) -> dict:
    parser = _get_lark_parser()

    normalized_alias_map: Dict[str, Sequence[str]] = {}
    if isinstance(alias_map, dict):
        for key, cols in alias_map.items():
            if not isinstance(cols, (list, tuple, set)):
                continue
            targets = [str(c or "").strip().upper() for c in cols if str(c or "").strip()]
            if targets:
                normalized_alias_map[str(key or "").strip().upper()] = targets

    allowed = []
    if allowed_cols:
        for col in allowed_cols:
            token = str(col or "").strip()
            if not token:
                continue
            token = token.strip('"').strip()
            if "." in token:
                token = token.split(".")[-1]
            token = token.strip()
            if token:
                allowed.append(token.upper())

    question_text = question or ""
    match = re.search(r"(?i)\bwhere\b(.+)", question_text)
    if match:
        question_text = match.group(1)
    question_text = question_text.strip()
    question_text = re.sub(r"(?i)^and\s+", "", question_text)

    parsed = parser.parse(question_text, alias_map=normalized_alias_map, allowed_columns=allowed)

    eq_filters: List[Any] = []
    eq_shape: Dict[str, Dict[str, Any]] = {}
    numeric_filters: List[Dict[str, Any]] = []
    numeric_shape_ops: Dict[str, set] = {}

    for col, values in parsed.eq_filters:
        eq_filters.append([col, values])
        eq_shape[col] = {
            "op": "in" if len(values) > 1 else "eq",
            "types": sorted({_val_type(str(v)) for v in values}),
        }

    for num_filter in getattr(parsed, "num_filters", []) or []:
        col = num_filter.get("col")
        op = num_filter.get("op")
        raw_values = num_filter.get("values")
        values = list(raw_values or [])
        if not values and num_filter.get("value") is not None:
            values = [num_filter["value"]]
        if col is None or op is None:
            continue
        col = str(col).strip().upper()
        op = str(op).strip().lower()
        if op == "between" and len(values) == 2:
            eq_filters.append(
                {
                    "col": col,
                    "op": "between",
                    "val": list(values),
                    "ci": False,
                    "trim": False,
                }
            )
            eq_shape[col] = {"op": "range", "types": ["NUMBER"]}
        else:
            val = values[0] if values else None
            eq_filters.append(
                {
                    "col": col,
                    "op": op,
                    "val": val,
                    "ci": False,
                    "trim": False,
                }
            )
            eq_shape[col] = {"op": op, "types": ["NUMBER"]}
        numeric_filters.append({"col": col, "op": op, "values": list(values)})
        numeric_shape_ops.setdefault(col, set()).add(op)

    fts_groups = [[token] for token in getattr(parsed, "fts_tokens", []) or []]
    if config.tail_trim:
        extracted_groups, _extracted_mode = extract_fts_terms(question, force=False)
        extracted_groups = _filter_fts_groups(extracted_groups, min_length=2)
        if extracted_groups:
            fts_groups = extracted_groups
    fts_groups = _filter_fts_groups(fts_groups, min_length=2)

    aggregations = list(getattr(parsed, "aggregations", []) or [])
    group_by_cols = list(getattr(parsed, "group_by", []) or [])
    order_hint = getattr(parsed, "order_hint", None)
    segments = {
        "eq_filters": len(getattr(parsed, "eq_filters", []) or []),
        "num_filters": len(numeric_filters),
        "fts_groups": len(fts_groups),
        "bool_groups": len(getattr(parsed, "bool_groups", []) or []),
        "aggregations": len(aggregations),
    }

    numeric_shape = {
        col: {"ops": sorted(list(ops))}
        for col, ops in numeric_shape_ops.items()
    }

    if order_hint:
        resolved_order = {"col": str(order_hint.get("col") or "").strip().upper(), "desc": bool(order_hint.get("desc"))}
    else:
        resolved_order = {"col": "REQUEST_DATE", "desc": True}

    intent = {
        "eq_filters": eq_filters,
        "eq": eq_shape,
        "numeric_filters": numeric_filters,
        "numeric_shape": numeric_shape,
        "fts_groups": fts_groups,
        "order": resolved_order,
        "pipeline_version": config.pipeline_version,
        "parser": config.parser,
        "_meta": {"segments": segments},
    }
    if aggregations:
        intent["aggregations"] = [
            {
                "func": str(agg.get("func") or "").upper(),
                "column": str(agg.get("column") or "").upper(),
                "alias": str(agg.get("alias") or "").upper(),
                "distinct": bool(agg.get("distinct")),
            }
            for agg in aggregations
        ]
    if group_by_cols:
        intent["group_by"] = [str(col or "").strip().upper() for col in group_by_cols if str(col or "").strip()]

    if getattr(parsed, "bool_tree", None):
        intent["boolean_tree"] = parsed.bool_tree
    if getattr(parsed, "clauses", None):
        intent["parsed_clauses"] = parsed.clauses
    return intent


def _build_light_intent_via_regex(
    question: str,
    allowed_cols: Optional[Sequence[str]],
    config: IntentPipelineConfig,
) -> dict:
    q = question or ""
    match = re.search(r"(?i)\bwhere\b(.+)", q)
    where_txt = (match.group(1) if match else q) or ""
    colset = {str(c or "").strip().strip('"').upper() for c in (allowed_cols or []) if str(c or "").strip()}

    eq_filters: List[Any] = []
    eq_shape: Dict[str, Dict[str, Any]] = {}
    numeric_filters: List[Dict[str, Any]] = []
    numeric_shape_ops: Dict[str, set] = {}
    simple_pairs: List[Tuple[str, List[str]]] = []

    # Detect inline "ENTITY NO" variants even without explicit "="
    entity_col = "ENTITY_NO"
    if entity_col in colset:
        entity_matches = re.findall(r"(?i)\bENTITY(?:\s*NO|_NO)\b(?:\s*=\s*|\s+)(['\"]?[A-Z0-9\-._]+['\"]?)", q)
        extracted_vals: List[str] = []
        for raw in entity_matches:
            cleaned = raw.strip().strip("'\"")
            if cleaned:
                extracted_vals.append(cleaned)
        if extracted_vals:
            simple_pairs.append((entity_col, extracted_vals))

    for part in re.split(r"(?i)\band\b", where_txt):
        snippet = part.strip().strip("() ")
        if not snippet:
            continue
        m2 = re.match(r"(?i)^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.+)$", snippet)
        if not m2:
            continue
        col = m2.group(1).upper()
        if col not in colset:
            continue
        rhs = m2.group(2).strip()
        vals = re.split(r"(?i)\s*\bor\b\s*|,", rhs)
        vals = [v.strip(" '\"\t()") for v in vals if v.strip()]
        if vals:
            simple_pairs.append((col, vals))

    comp_filters = _extract_comparison_filters(where_txt, allowed_cols or [])

    for col, vals in simple_pairs:
        eq_filters.append([col, vals])
        eq_shape[col] = {
            "op": "in" if len(vals) > 1 else "eq",
            "types": sorted({_val_type(str(v)) for v in vals}),
        }

    for comp in comp_filters:
        col = str(comp.get("col") or "").strip().upper()
        op = str(comp.get("op") or "eq").strip().lower()
        value = comp.get("val")
        eq_filters.append(
            {
                "col": col,
                "op": op,
                "val": value,
                "ci": False,
                "trim": False,
            }
        )
        eq_shape[col] = {"op": op, "types": ["NUMBER"]}
        numeric_filters.append({"col": col, "op": op, "values": [value]})
        numeric_shape_ops.setdefault(col, set()).add(op)

    raw_fts_groups, _fts_mode = extract_fts_terms(question, force=False)
    if config.tail_trim:
        fts_groups = _filter_fts_groups(raw_fts_groups, min_length=2)
    else:
        fts_groups = _filter_fts_groups(raw_fts_groups, min_length=1)

    segments = {
        "eq_filters": len(simple_pairs),
        "num_filters": len(numeric_filters),
        "fts_groups": len(fts_groups),
        "bool_groups": 0,
    }

    numeric_shape = {
        col: {"ops": sorted(list(ops))}
        for col, ops in numeric_shape_ops.items()
    }

    intent = {
        "eq_filters": eq_filters,
        "eq": eq_shape,
        "numeric_filters": numeric_filters,
        "numeric_shape": numeric_shape,
        "fts_groups": fts_groups,
        "order": {"col": "REQUEST_DATE", "desc": True},
        "pipeline_version": config.pipeline_version,
        "parser": "legacy",
        "_meta": {"segments": segments},
    }
    return intent


def _merge_hints(base: Optional[dict], extra: Optional[dict]) -> dict:
    base = base or {}
    extra = extra or {}
    out = dict(base)
    if extra.get("eq_filters"):
        out.setdefault("eq_filters", [])
        out["eq_filters"].extend(extra["eq_filters"])  # type: ignore[index]
    if extra.get("order"):
        out["order"] = extra["order"]
    if extra.get("fts_tokens"):
        out["fts_tokens"] = (out.get("fts_tokens") or []) + extra["fts_tokens"]
    return out


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
    numeric_sources: List[Dict[str, Any]] = []

    def _collect(obj: Optional[Dict[str, Any]]) -> None:
        if not isinstance(obj, dict):
            return
        sources.append(obj)
        if isinstance(obj.get("numeric_filters"), list):
            numeric_sources.append(obj)
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
    merged_aggs: List[Dict[str, Any]] = []
    merged_numeric: List[Dict[str, Any]] = []
    numeric_shape: Optional[Dict[str, Any]] = None

    for source in sources:
        if "eq_filters" in source and isinstance(source.get("eq_filters"), list):
            for entry in source["eq_filters"]:
                if isinstance(entry, dict):
                    merged_eq.append(dict(entry))
        if "aggregations" in source and isinstance(source.get("aggregations"), list):
            for entry in source["aggregations"]:
                if isinstance(entry, dict):
                    merged_aggs.append(dict(entry))
        if "numeric_filters" in source and isinstance(source.get("numeric_filters"), list):
            for entry in source["numeric_filters"]:
                if isinstance(entry, dict):
                    merged_numeric.append(dict(entry))
        if numeric_shape is None and isinstance(source.get("numeric_shape"), dict):
            numeric_shape = dict(source.get("numeric_shape") or {})
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
    if merged_aggs:
        intent["aggregations"] = merged_aggs
    if merged_numeric:
        intent["numeric_filters"] = merged_numeric
    if numeric_shape:
        intent["numeric_shape"] = numeric_shape

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

    if "or_groups" in raw and isinstance(raw.get("or_groups"), list):
        # Preserve cross-column OR groups from learned hints
        intent["or_groups"] = [grp for grp in raw.get("or_groups") if isinstance(grp, list) and grp]

    if "sort_desc" in intent:
        intent["sort_desc"] = _coerce_bool_flag(intent.get("sort_desc"), default=True)
    if "gross" in intent:
        intent["gross"] = _coerce_bool_flag(intent.get("gross"))
    if "full_text_search" in intent:
        intent["full_text_search"] = _coerce_bool_flag(intent.get("full_text_search"), default=False)

    # Expand alias EQ keys in eq_filters into OR groups here (centralized), so both
    # direct FTS and deterministic paths get the same overlays.
    try:
        eq_fp = intent.get("eq_filters") or []
        if isinstance(eq_fp, list) and eq_fp:
            # Build alias map via settings helper on demand (best-effort)
            alias_map_raw = {}
            try:
                settings_obj = get_settings()
                alias_map_raw = _get_namespace_mapping(settings_obj, _ns(), "DW_EQ_ALIAS_COLUMNS", {}) or {}
            except Exception:
                alias_map_raw = {}
            alias_map: Dict[str, List[str]] = {}
            if isinstance(alias_map_raw, dict):
                for k, cols in alias_map_raw.items():
                    if not isinstance(cols, (list, tuple, set)):
                        continue
                    bucket: List[str] = []
                    seen: set[str] = set()
                    for c in cols:
                        s = str(c or "").strip().upper()
                        if s and s not in seen:
                            seen.add(s)
                            bucket.append(s)
                    if bucket:
                        alias_map[str(k or "").strip().upper()] = bucket
            if not alias_map:
                targets = [
                    "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4",
                    "DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
                    "OWNER_DEPARTMENT",
                ]
                alias_map = {"DEPARTMENTS": targets, "DEPARTMENT": targets}

            def _norm_pair(it):
                if isinstance(it, (list, tuple)) and len(it)==2:
                    col = str(it[0] or "").strip().upper()
                    vals = it[1]
                    vals = list(vals) if isinstance(vals, (list,tuple,set)) else [vals]
                    return col, vals
                if isinstance(it, dict):
                    col = str((it.get("col") or it.get("field") or "")).strip().upper()
                    val = it.get("val") if it.get("val") is not None else it.get("value")
                    vals = [] if val is None else ([val] if not isinstance(val,(list,tuple,set)) else list(val))
                    return col, vals
                return None, None

            keep: List[Any] = []
            og_new: List[List[Dict[str, Any]]] = []
            for it in eq_fp:
                col, vals = _norm_pair(it)
                if not col:
                    keep.append(it)
                    continue
                targets = alias_map.get(col)
                if not targets:
                    keep.append(it)
                    continue
                for v in vals or []:
                    grp = [{"col": t, "val": v, "op":"eq", "ci": True, "trim": True} for t in targets]
                    if grp:
                        og_new.append(grp)
            if og_new:
                # Dedup existing groups + new ones
                og = intent.setdefault("or_groups", []) if isinstance(intent.get("or_groups"), list) else intent.setdefault("or_groups", [])
                for g in og_new:
                    og.append(g)
                intent["eq_filters"] = keep
    except Exception:
        pass

    return intent


def _sanitize_eq_values(intent: Dict[str, Any], allowed_cols: Sequence[str]) -> None:
    """
    - Collapse multi-values per column into flat list of dicts (later grouped via IN).
    - Move leaked phone digits out of email terms when obvious.
    - Leave only allowed columns.
    """
    eq = intent.get("eq_filters") or []
    if not isinstance(eq, list) or not eq:
        return
    allowed = {str(c).strip().upper() for c in (allowed_cols or []) if str(c).strip()}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    comparators: List[Dict[str, Any]] = []
    order: List[str] = []
    for f in eq:
        # Accept both dict form and list-of-lists [["COL", [v1,v2,...]], ...]
        if isinstance(f, dict):
            col = (f.get("col") or f.get("column") or "").strip().upper()
            if not col:
                continue
            # Keep only explicitly allowed columns
            if allowed and col not in allowed:
                continue
            op = str(f.get("op") or "eq").strip().lower()
            val = f.get("val") if f.get("val") is not None else f.get("value")
            entry = {
                "col": col,
                "val": val,
                "op": op,
                "ci": f.get("ci"),
                "trim": f.get("trim"),
                "synonyms": f.get("synonyms") if isinstance(f.get("synonyms"), dict) else None,
            }
            if op in {"eq", "in"} and entry["synonyms"] is None:
                bucket = grouped.setdefault(col, [])
                bucket.append(entry)
                if col not in order:
                    order.append(col)
            else:
                # Preserve comparison/like/operators verbatim
                if entry["ci"] is None:
                    entry["ci"] = False
                if entry["trim"] is None:
                    entry["trim"] = False
                comparators.append(entry)
        elif isinstance(f, (list, tuple)) and len(f) == 2:
            col = str(f[0] or "").strip().upper()
            if not col:
                continue
            # Keep only explicitly allowed columns
            if allowed and col not in allowed:
                continue
            vals = f[1]
            if isinstance(vals, (list, tuple, set)):
                items = list(vals)
            elif vals is not None:
                items = [vals]
            else:
                items = []
            if items:
                bucket = grouped.setdefault(col, [])
                if col not in order:
                    order.append(col)
                for v in items:
                    bucket.append({"col": col, "val": v, "op": "eq", "ci": True, "trim": True, "synonyms": None})
        else:
            continue

    def _is_email(x: Any) -> bool:
        return isinstance(x, str) and "@" in x

    def _digits(x: Any) -> str:
        return "".join(ch for ch in str(x) if ch.isdigit())

    if "REPRESENTATIVE_EMAIL" in grouped and "REPRESENTATIVE_PHONE" in allowed:
        move_phone: List[str] = []
        keep_email: List[Any] = []
        for v in grouped.get("REPRESENTATIVE_EMAIL", []):
            vs = str(v)
            d = _digits(vs)
            if (d and len(d) >= 8) and not _is_email(vs):
                move_phone.append(d)
            else:
                keep_email.append(v)
        grouped["REPRESENTATIVE_EMAIL"] = keep_email
        if move_phone:
            grouped.setdefault("REPRESENTATIVE_PHONE", []).extend(move_phone)

    canon: List[Dict[str, Any]] = []
    for col in order:
        vals = grouped.get(col) or []
        seen_keys: set = set()
        for entry in vals:
            val = entry.get("val")
            key = val.upper() if isinstance(val, str) else val
            if key in seen_keys:
                continue
            seen_keys.add(key)
            ci = entry.get("ci")
            trim = entry.get("trim")
            canon.append(
                {
                    "col": col,
                    "val": val,
                    "op": "eq",
                    "ci": True if ci is None else bool(ci),
                    "trim": True if trim is None else bool(trim),
                }
            )
    if comparators:
        for entry in comparators:
            canon.append(
                {
                    "col": entry["col"],
                    "val": entry["val"],
                    "op": entry["op"],
                    "ci": bool(entry.get("ci")),
                    "trim": bool(entry.get("trim")),
                    "synonyms": entry.get("synonyms"),
                }
            )
    if canon:
        intent["eq_filters"] = canon


def _expand_eq_aliases_with_map(intent: Dict[str, Any], alias_map: Dict[str, List[str]]) -> None:
    """Expand alias EQ filters into cross-column OR groups and remove alias entries.

    - Accepts eq_filters in either canonical list form [[COL, [vals...]], ...]
      or dict entries {col/field, val/values}.
    - For each alias COL in alias_map, creates an OR group across target columns
      for each value: [{col: T1, val: v}, {col: T2, val: v}, ...].
    - Leaves non-alias eq entries untouched.
    """
    if not isinstance(intent, dict) or not isinstance(alias_map, dict) or not alias_map:
        return

    normalized_alias_map: Dict[str, List[str]] = {}
    for raw_alias, cols in alias_map.items():
        alias_key = str(raw_alias or "").strip().upper()
        if not alias_key:
            continue
        targets: List[str] = []
        seen_targets: set[str] = set()
        if isinstance(cols, (list, tuple, set)):
            iterable = cols
        else:
            iterable = [cols]
        for col in iterable:
            target = str(col or "").strip().upper()
            if not target or target in seen_targets:
                continue
            seen_targets.add(target)
            targets.append(target)
        if targets:
            normalized_alias_map[alias_key] = targets

    if not normalized_alias_map:
        return

    def _norm_item(it) -> tuple[str, List[Any]] | None:
        if isinstance(it, (list, tuple)) and len(it) == 2:
            col = str(it[0] or "").strip().upper()
            vals = it[1]
            if vals is None:
                return None
            vals_list = list(vals) if isinstance(vals, (list, tuple, set)) else [vals]
            return col, vals_list
        if isinstance(it, dict):
            col = str((it.get("col") or it.get("field") or "")).strip().upper()
            val = it.get("val") if it.get("val") is not None else it.get("value")
            vals_list = [] if val is None else ([val] if not isinstance(val, (list, tuple, set)) else list(val))
            return col, vals_list
        return None

    raw = intent.get("eq_filters") or []
    if not isinstance(raw, list):
        return

    # Guard to avoid repeated expansions on the same intent.
    already_expanded = intent.get("_alias_expanded") is True
    if already_expanded:
        pending_alias = False
        for item in raw:
            norm = _norm_item(item)
            if not norm:
                continue
            col, _ = norm
            if normalized_alias_map.get(col):
                pending_alias = True
                break
        if not pending_alias:
            return

    keep: List[Any] = []
    covered_targets: set[str] = set()
    or_groups: List[List[Dict[str, Any]]] = (
        intent.setdefault("or_groups", []) if isinstance(intent.get("or_groups"), list) else intent.setdefault("or_groups", [])
    )
    expanded_any = False
    alias_value_map: Dict[str, List[Any]] = {}

    def _collapse_alias_variants(values: List[Any]) -> List[Any]:
        """Collapse singular/plural variants so the canonical learnt value wins."""
        from collections import OrderedDict as _OD
        import re as _re

        def _canonical_keys(text: str) -> set[str]:
            norm = _re.sub(r"\s+", " ", text.strip().upper())
            keys = {norm}
            if len(norm) > 3:
                if norm.endswith("IES"):
                    keys.add(norm[:-3] + "Y")
                if norm.endswith("ES"):
                    keys.add(norm[:-2])
                if norm.endswith("S"):
                    keys.add(norm[:-1])
            return keys

        def _base_key(text: str) -> str:
            keys = sorted(_canonical_keys(text), key=lambda k: (len(k), k))
            for key in keys:
                if not key.endswith("S") or key.endswith("SS"):
                    return key
            return keys[0] if keys else text.strip().upper()

        buckets: list[tuple[set[str], List[str]]] = []
        extras: "OrderedDict[Any, Any]" = _OD()
        for value in values:
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    continue
                keyset = _canonical_keys(text)
                inserted = False
                for bucket_keys, bucket_vals in buckets:
                    if bucket_keys & keyset:
                        up = text.upper()
                        if up not in bucket_vals:
                            bucket_vals.append(up)
                        bucket_keys.update(keyset)
                        inserted = True
                        break
                if not inserted:
                    buckets.append((set(keyset), [text.upper()]))
            else:
                # Non-string literal – keep the first occurrence
                if value not in extras:
                    extras[value] = value
        collapsed: List[Any] = []
        for _, bucket in buckets:
            if not bucket:
                continue
            canonical = max(bucket, key=lambda x: (len(x), x))
            collapsed.append(canonical)
        for val in extras.values():
            collapsed.append(val)

        def _prune_substrings(values: List[Any]) -> List[Any]:
            str_entries: List[Tuple[int, str, str]] = []
            out: List[Any] = []
            import re as _re

            for idx, value in enumerate(values):
                if isinstance(value, str):
                    normalized = _re.sub(r"\s+", " ", value.strip().upper())
                    alpha = _re.sub(r"[^A-Z0-9]+", "", normalized)
                    str_entries.append((idx, value.strip().upper(), alpha))
                else:
                    out.append(value)

            keep: List[Tuple[int, str, str]] = []
            for idx, text, alpha in sorted(str_entries, key=lambda x: (-len(x[1]), x[0])):
                if not alpha:
                    continue
                if any(alpha and alpha in existing_alpha for _, _, existing_alpha in keep):
                    continue
                keep.append((idx, text, alpha))

            keep_sorted = sorted(keep, key=lambda x: x[0])
            for _, text, _ in keep_sorted:
                out.append(text)
            return out

        return _prune_substrings(collapsed)

    alias_targets_index: Dict[str, tuple] = {}
    canonical_for_targets: Dict[tuple, str] = {}
    target_to_alias: Dict[str, str] = {}

    def _score_alias(name: str) -> tuple:
        name = name or ""
        return (1 if name.endswith("S") else 0, len(name), name)

    for alias, cols in (normalized_alias_map or {}).items():
        cols_tuple = tuple(sorted(str(c).strip().upper() for c in cols if str(c).strip()))
        alias_targets_index[alias] = cols_tuple
        if not cols_tuple:
            continue
        for target in cols_tuple:
            target_to_alias[target] = alias
        current = canonical_for_targets.get(cols_tuple)
        if current is None or _score_alias(alias) > _score_alias(current):
            canonical_for_targets[cols_tuple] = alias

    def _canonical_alias(alias: str) -> str:
        cols_tuple = alias_targets_index.get(alias)
        if not cols_tuple:
            return alias
        return canonical_for_targets.get(cols_tuple, alias)

    for it in list(raw):
        norm = _norm_item(it)
        if not norm:
            keep.append(it)
            continue
        col, vals = norm
        canonical_alias: Optional[str] = None
        targets: Optional[List[str]] = None
        if col in normalized_alias_map:
            canonical_alias = _canonical_alias(col)
            targets = normalized_alias_map.get(canonical_alias)
        elif col in target_to_alias:
            alias_key = target_to_alias[col]
            canonical_alias = _canonical_alias(alias_key)
            targets = normalized_alias_map.get(canonical_alias)
        if canonical_alias and targets:
            if vals:
                alias_value_map.setdefault(canonical_alias, []).extend(vals)
            covered_targets.update(targets)
            # Skip keeping entries covered by alias expansion
            continue
        keep.append(it)

    if covered_targets:
        filtered_keep: List[Any] = []
        for item in keep:
            norm = _norm_item(item)
            if not norm:
                filtered_keep.append(item)
                continue
            col, _ = norm
            if col in covered_targets:
                continue
            filtered_keep.append(item)
        keep = filtered_keep

    intent["eq_filters"] = keep
    alias_eq_entries: Dict[str, List[Any]] = {}

    for alias_col, values in alias_value_map.items():
        targets = normalized_alias_map.get(alias_col) or []
        if not targets:
            continue

        candidate_vals: List[Any] = []
        for v in values:
            if isinstance(v, str):
                text = v.strip()
                if text:
                    candidate_vals.append(text)
            elif v is not None:
                candidate_vals.append(v)

        cleaned_vals = _collapse_alias_variants(candidate_vals)

        if not cleaned_vals:
            continue
        alias_eq_entries[alias_col] = list(cleaned_vals)

        normalized_targets = tuple(sorted(str(t).strip().upper() for t in targets if str(t).strip()))
        normalized_values = tuple(sorted(str(v).strip().upper() if isinstance(v, str) else v for v in cleaned_vals))

        def _group_signature(grp: Any) -> Tuple[Tuple[str, ...], Tuple[Any, ...]]:
            columns: set[str] = set()
            values_set: set[Any] = set()
            for term in grp or []:
                if not isinstance(term, dict):
                    continue
                col = str((term.get("col") or term.get("column") or term.get("field") or "")).strip().upper()
                if not col:
                    continue
                columns.add(col)
                raw_vals = term.get("values") if term.get("values") is not None else term.get("val")
                if isinstance(raw_vals, (list, tuple, set)):
                    vals_iter = raw_vals
                elif raw_vals is None:
                    vals_iter = []
                else:
                    vals_iter = [raw_vals]
                for vv in vals_iter:
                    values_set.add(str(vv).strip().upper() if isinstance(vv, str) else vv)
            return tuple(sorted(columns)), tuple(sorted(values_set))

        already_present = False
        if normalized_targets and normalized_values:
            # Remove stale groups for the same columns so canonical values replace question literals.
            try:
                pruned: List[List[Dict[str, Any]]] = []
                for existing_group in or_groups:
                    cols_sig, _values_sig = _group_signature(existing_group)
                    if cols_sig == normalized_targets:
                        continue
                    pruned.append(existing_group)
                if len(pruned) != len(or_groups):
                    or_groups[:] = pruned
            except Exception:
                pass
            for existing_group in or_groups:
                cols_sig, values_sig = _group_signature(existing_group)
                if cols_sig == normalized_targets and values_sig == normalized_values:
                    already_present = True
                    break
        if already_present:
            continue

        group: List[Dict[str, Any]] = []
        for target in targets:
            group.append(
                {"col": target, "values": list(cleaned_vals), "op": "in", "ci": True, "trim": True}
            )
        if group:
            or_groups.append(group)
            expanded_any = True

    # Deduplicate OR groups by (col,val) signature regardless of insertion order
    try:
        uniq: List[List[Dict[str, Any]]] = []
        seen_sigs: set = set()
        for grp in or_groups:
            if not isinstance(grp, list) or not grp:
                continue
            sig_elems = []
            ok = True
            for t in grp:
                if not isinstance(t, dict):
                    ok = False
                    break
                col = str((t.get("col") or t.get("column") or "")).strip().upper()
                val = t.get("val") if t.get("val") is not None else t.get("value")
                sval = str(val).strip().upper() if isinstance(val, str) else val
                sig_elems.append((col, sval))
            if not ok:
                continue
            sig = tuple(sorted(sig_elems))
            if sig in seen_sigs:
                continue
            seen_sigs.add(sig)
            uniq.append(grp)
        if uniq:
            intent["or_groups"] = uniq
    except Exception:
        pass
    # Mark expanded to prevent duplicates from multiple passes, but only if we actually expanded.
    if expanded_any:
        intent["_alias_expanded"] = True

    if alias_eq_entries:
        eq_filters_list = intent.setdefault("eq_filters", keep)
        for alias_col, values in alias_eq_entries.items():
            entry = {
                "col": alias_col,
                "values": list(values),
                "op": "in" if len(values) > 1 else "eq",
                "ci": True,
                "trim": True,
            }
            eq_filters_list.append(entry)
        if not expanded_any:
            intent["_alias_expanded"] = True


def _maybe_apply_entity_status_aggregation_heuristic(
    question: str,
    intent: Dict[str, Any],
    allowed_cols: Optional[Sequence[str]],
) -> None:
    """Inject heuristic aggregation intent for paraphrased ENTITY_NO totals questions."""

    qtext = (question or "").strip()
    if not qtext:
        return
    q_lower = qtext.lower()
    if "entity" not in q_lower or "contract status" not in q_lower:
        return
    if "total" not in q_lower or "count" not in q_lower:
        return

    match = re.search(
        r"(?i)entity\s*(?:no|number)\s*(?:=|is|:)?\s*(?:['\"])?([A-Za-z0-9._-]+)(?:['\"])?",
        qtext,
    )
    if not match:
        return
    entity_value = match.group(1).strip()
    if not entity_value:
        return

    allowed_set = {
        str(col or "").strip().strip('"').upper()
        for col in (allowed_cols or [])
        if str(col or "").strip()
    }

    entity_col = "ENTITY_NO"
    group_col = "CONTRACT_STATUS"
    if entity_col not in allowed_set and allowed_set:
        return

    eq_filters = intent.setdefault("eq_filters", [])
    if not any(
        isinstance(item, (list, tuple))
        and len(item) == 2
        and str(item[0]).strip().upper() == entity_col
        for item in eq_filters
    ):
        eq_filters.append([entity_col, [entity_value]])

    eq_shape = intent.setdefault("eq", {})
    eq_shape[entity_col] = {
        "op": "eq",
        "types": ["TEXT"],
    }

    group_by_list = intent.setdefault("group_by", [])
    if group_col not in group_by_list:
        group_by_list.append(group_col)

    order_hint = intent.setdefault("order", {})
    order_hint["col"] = group_col
    order_hint["desc"] = False

    aggregations = intent.setdefault("aggregations", [])

    def _ensure_agg(func: str, column: str, alias: str) -> None:
        for agg in aggregations:
            if (
                str(agg.get("func") or "").upper() == func
                and str(agg.get("column") or "").upper() == column
                and str(agg.get("alias") or "").upper() == alias
            ):
                return
        aggregations.append(
            {
                "func": func,
                "column": column,
                "distinct": False,
                "alias": alias,
            }
        )

    _ensure_agg("SUM", "CONTRACT_VALUE_NET_OF_VAT", "TOTAL_AMOUNT")
    _ensure_agg("COUNT", "*", "TOTAL_COUNT")

    segments = intent.setdefault("_meta", {}).setdefault("segments", {})
    segments["eq_filters"] = len(eq_filters)
    segments.setdefault("bool_groups", 0)
    segments.setdefault("fts_groups", segments.get("fts_groups", 0))

    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.intent.heuristic",
                "kind": "entity_totals",
                "group_by": group_col,
                "alias_applied": entity_col,
            }
        )
    except Exception:
        pass

def _extract_or_groups_from_question(question: str, allowed_cols: Sequence[str]) -> List[List[Dict[str, Any]]]:
    """
    Detect patterns like 'COL_A = X or COL_B = Y' in the question across allowed columns.
    Returns list of OR groups, each a list of canonical EQ dicts.
    """
    if not question:
        return []
    q = " " + question.strip() + " "
    import re as _re
    cols_alt = "|".join(_re.escape(str(c).strip().upper()) for c in allowed_cols if str(c).strip())
    if not cols_alt:
        return []
    pat = _re.compile(rf"(?i)\b({cols_alt})\s*=\s*([^()]+?)(?=\s+(?:and|or)\s+\b({cols_alt})\b|\s*$)")
    conn_pat = _re.compile(r"(?i)\s+(and|or)\s+")
    matches = list(pat.finditer(q))
    groups: List[List[Dict[str, Any]]] = []
    for i, m in enumerate(matches[:-1]):
        tail = q[m.end() : matches[i + 1].start()]
        conn = conn_pat.search(tail)
        if not conn:
            continue
        if conn.group(1).lower() == "or":
            col1 = m.group(1).upper().strip()
            val1 = m.group(2).strip()
            col2 = matches[i + 1].group(1).upper().strip()
            val2 = matches[i + 1].group(2).strip()
            groups.append(
                [
                    {"col": col1, "val": val1, "op": "eq", "ci": True, "trim": True},
                    {"col": col2, "val": val2, "op": "eq", "ci": True, "trim": True},
                ]
            )
    return groups


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
    bind_prefix: str = "fts",
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
    numeric_filters_raw = intent.get("numeric_filters") or []
    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.apply_hints.start",
                "has_eq": bool(eq_filters_raw),
                "has_num": bool(numeric_filters_raw),
                "has_fts": bool(intent.get("fts_tokens") or intent.get("fts_groups")),
                "has_or_groups": bool(intent.get("or_groups")),
            }
        )
    except Exception:
        pass
    combined_clause_parts: List[str] = []

    def _append_clause(clause: str, temp: Dict[str, Any]) -> List[str]:
        if not clause or not isinstance(temp, dict) or not temp:
            return []
        rename_map: Dict[str, str] = {}
        for key in temp.keys():
            base = f"ol_{key}"
            new_key = base
            suffix = 1
            while new_key in binds or new_key in combined_binds or new_key in rename_map.values():
                new_key = f"{base}_{suffix}"
                suffix += 1
            rename_map[key] = new_key
        updated = clause
        for old, new in rename_map.items():
            updated = updated.replace(f":{old}", f":{new}")
        renamed = {rename_map[k]: v for k, v in temp.items()}
        if renamed:
            combined_binds.update(renamed)
        combined_clause_parts.append(updated)
        return list(renamed.keys())

    # Accept dict and pair shapes; grouping/dedup handled downstream
    eq_applied = False
    eq_clause = ""
    eq_temp_binds: Dict[str, Any] = {}
    numeric_filters_all: List[Dict[str, Any]] = []

    eq_alias_targets: Dict[str, List[str]] = {}

    if eq_filters_raw:
        eq_like: List[Any] = []
        range_like: List[Dict[str, Any]] = []

        def _extract_op(item: Any) -> str:
            if isinstance(item, dict):
                return str(item.get("op") or "eq").strip().lower()
            return "eq"

        for item in eq_filters_raw:
            op = _extract_op(item)
            if op in {"eq", "in"}:
                eq_like.append(item)
            else:
                if isinstance(item, dict):
                    range_like.append(item)

        try:
            if eq_like:
                eq_clause, eq_temp_binds, eq_alias_targets = _builder_mod._eq_clause_from_filters(eq_like, eq_temp_binds, bind_prefix="eq")  # type: ignore[attr-defined]
        except Exception:
            eq_clause = ""
            eq_temp_binds = {}
            eq_alias_targets = {}

        for item in range_like:
            if not isinstance(item, dict):
                continue
            col = str(item.get("col") or item.get("column") or "").strip().upper()
            op = str(item.get("op") or item.get("operator") or "").strip().lower()
            if not col or not op:
                continue
            values = item.get("values")
            if values is None:
                candidate = item.get("val") if item.get("val") is not None else item.get("value")
                if isinstance(candidate, (list, tuple, set)):
                    values = list(candidate)
                elif candidate is not None:
                    values = [candidate]
            numeric_filters_all.append({"col": col, "op": op, "values": values})

    if isinstance(numeric_filters_raw, list):
        for item in numeric_filters_raw:
            if isinstance(item, dict):
                numeric_filters_all.append(dict(item))

    if numeric_filters_all:
        seen_numeric: set[Tuple[str, str, Tuple[Any, ...]]] = set()
        dedup_numeric: List[Dict[str, Any]] = []
        for entry in numeric_filters_all:
            if not isinstance(entry, dict):
                continue
            col = str(entry.get("col") or entry.get("column") or "").strip().upper()
            op = str(entry.get("op") or entry.get("operator") or "").strip().lower()
            raw_vals = entry.get("values")
            if raw_vals is None and entry.get("val") is not None:
                raw_vals = [entry.get("val")]
            if isinstance(raw_vals, (list, tuple, set)):
                values_tuple = tuple(_builder_mod._coerce_numeric_literal(v) for v in raw_vals)  # type: ignore[attr-defined]
            elif raw_vals is None:
                values_tuple = tuple()
            else:
                values_tuple = (_builder_mod._coerce_numeric_literal(raw_vals),)  # type: ignore[attr-defined]
            key = (col, op, values_tuple)
            if not col or not op or key in seen_numeric:
                continue
            seen_numeric.add(key)
            cleaned = dict(entry)
            cleaned["col"] = col
            cleaned["op"] = op
            cleaned["values"] = list(values_tuple)
            dedup_numeric.append(cleaned)
        numeric_filters_all = dedup_numeric

    numeric_clause = ""
    numeric_temp_binds: Dict[str, Any] = {}
    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.apply_hints.numeric_raw",
                "count": len(numeric_filters_all),
                "filters": numeric_filters_all,
            }
        )
    except Exception:
        pass
    try:
        if numeric_filters_all:
            numeric_clause, numeric_temp_binds = _builder_mod.numeric_clause_from_filters(  # type: ignore[attr-defined]
                numeric_filters_all,
                numeric_temp_binds,
                bind_prefix="num",
            )
    except Exception:
        numeric_clause = ""
        numeric_temp_binds = {}

    new_eq_keys = _append_clause(eq_clause, eq_temp_binds)
    new_num_keys: List[str] = []
    if numeric_clause:
        new_num_keys = _append_clause(numeric_clause, numeric_temp_binds)

    if combined_clause_parts:
        combined_clause = " AND ".join(combined_clause_parts)
        try:
            logging.getLogger("dw").info(
                {"event": "answer.apply_hints.eq_clause", "clause": combined_clause}
            )
        except Exception:
            pass
        where_clauses.append(f"({combined_clause})")
        eq_applied = bool(new_eq_keys or new_num_keys)
        if new_eq_keys:
            try:
                logging.getLogger("dw").info(
                    {"event": "answer.apply_hints.eq", "applied": True, "bind_names": new_eq_keys}
                )
            except Exception:
                pass
        if new_num_keys:
            try:
                logging.getLogger("dw").info(
                    {"event": "answer.apply_hints.num", "applied": True, "bind_names": new_num_keys}
                )
            except Exception:
                pass

    # OR-groups across different columns from intent
    try:
        or_groups = intent.get("or_groups") or []
    except Exception:
        or_groups = []
    if isinstance(or_groups, list) and or_groups:
        seen_group_sigs: set[Tuple[Tuple[str, Tuple[Any, ...]], ...]] = set()
        for grp in or_groups:
            signature: Optional[Tuple[Tuple[str, Tuple[Any, ...]], ...]] = None
            try:
                items_sig: List[Tuple[str, Tuple[Any, ...]]] = []
                for term in grp or []:
                    if isinstance(term, dict):
                        col = str(term.get("col") or term.get("column") or term.get("field") or "").strip().upper()
                        raw_vals = (
                            term.get("values")
                            if term.get("values") is not None
                            else term.get("val")
                            if term.get("val") is not None
                            else term.get("value")
                        )
                    elif isinstance(term, (list, tuple)) and term:
                        col = str(term[0] or "").strip().upper()
                        raw_vals = term[1] if len(term) > 1 else []
                    else:
                        continue
                    if not col:
                        continue
                    if isinstance(raw_vals, (list, tuple, set)):
                        vals_iter = list(raw_vals)
                    elif raw_vals is None:
                        vals_iter = []
                    else:
                        vals_iter = [raw_vals]
                    normalized_vals: List[Any] = []
                    for val in vals_iter:
                        if isinstance(val, str):
                            norm = val.strip().upper()
                            if not norm:
                                continue
                            normalized_vals.append(norm)
                        else:
                            normalized_vals.append(val)
                    items_sig.append((col, tuple(sorted(normalized_vals))))
                if items_sig:
                    signature = tuple(sorted(items_sig))
            except Exception:
                signature = None
            if signature and signature in seen_group_sigs:
                continue
            try:
                grp_clause, grp_binds = _builder_mod.build_or_group(grp)  # type: ignore[attr-defined]
            except Exception:
                grp_clause, grp_binds = "", {}
            if grp_clause:
                if signature:
                    seen_group_sigs.add(signature)
                # Ensure bind names don't collide
                rename_map: Dict[str, str] = {}
                for key in list(grp_binds.keys()):
                    base = f"ol_or_{key}"
                    new_key = base
                    suffix = 1
                    while new_key in binds or new_key in combined_binds or new_key in rename_map.values():
                        new_key = f"{base}_{suffix}"
                        suffix += 1
                    rename_map[key] = new_key
                for old, new in rename_map.items():
                    grp_clause = grp_clause.replace(f":{old}", f":{new}")
                renamed = {rename_map.get(k, k): v for k, v in grp_binds.items()}
                combined_binds.update(renamed)
                where_clauses.append(grp_clause)

    # Apply persisted eq_like fragments (alias -> tokens) if present
    try:
        eq_like = intent.get("eq_like") if isinstance(intent, dict) else None
    except Exception:
        eq_like = None
    if isinstance(eq_like, dict) and eq_like:
        try:
            settings_obj = _get_settings()
            # Load alias targets
            try:
                alias_map_raw = getattr(settings_obj, "get_json", None)
                if callable(alias_map_raw):
                    alias_map_raw = settings_obj.get_json("DW_EQ_ALIAS_COLUMNS", scope="namespace", namespace=intent.get("namespace") or "dw::common")
                else:
                    alias_map_raw = settings_obj.get("DW_EQ_ALIAS_COLUMNS", scope="namespace", namespace=intent.get("namespace") or "dw::common")
            except TypeError:
                alias_map_raw = settings_obj.get("DW_EQ_ALIAS_COLUMNS") if hasattr(settings_obj, "get") else {}
            alias_map: Dict[str, List[str]] = {}
            if isinstance(alias_map_raw, dict):
                for k, cols in alias_map_raw.items():
                    if not isinstance(cols, (list, tuple, set)):
                        continue
                    bucket: List[str] = []
                    seen: set[str] = set()
                    for c in cols:
                        s = str(c or "").strip().upper()
                        if s and s not in seen:
                            seen.add(s)
                            bucket.append(s)
                    if bucket:
                        alias_map[str(k or "").strip().upper()] = bucket
            # Build LIKE across targets (AND within tokens)
            for alias_key, toks in eq_like.items():
                targets = alias_map.get(str(alias_key).upper()) or []
                if not targets:
                    continue
                like_groups = [list({str(t).upper().strip() for t in (toks or []) if str(t).strip()})]
                if not like_groups or not like_groups[0]:
                    continue
                like_clause, like_binds = _build_rate_fts_where(targets, like_groups, operator="AND", bind_prefix="eql")
                if like_clause:
                    # Ensure bind name uniqueness with ol_ prefix
                    rename_map: Dict[str, str] = {}
                    for key in list(like_binds.keys()):
                        base = f"ol_{key}"
                        new_key = base
                        suffix = 1
                        while new_key in binds or new_key in combined_binds or new_key in rename_map.values():
                            new_key = f"{base}_{suffix}"
                            suffix += 1
                        rename_map[key] = new_key
                    for old, new in rename_map.items():
                        like_clause = like_clause.replace(f":{old}", f":{new}")
                    renamed_like = {rename_map.get(k, k): v for k, v in like_binds.items()}
                    combined_binds.update(renamed_like)
                    where_clauses.append(like_clause)
        except Exception:
            pass

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
            bind_prefix="fts",
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
        _before_fts = set(list(binds.keys()) + list(combined_binds.keys()))
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
        try:
            _added = sorted(set(combined_binds.keys()) - _before_fts)
            logging.getLogger("dw").info(
                {"event": "answer.apply_hints.fts", "groups": len(tokens_groups or []), "bind_names": _added}
            )
        except Exception:
            pass

    if where_clauses:
        sql = append_where(sql, " AND ".join(where_clauses))

    if combined_binds:
        binds.update(combined_binds)

    group_by_raw = intent.get("group_by")
    group_items: List[str] = []
    if isinstance(group_by_raw, (list, tuple, set)):
        group_items = [str(item).strip() for item in group_by_raw if str(item).strip()]
    elif isinstance(group_by_raw, str) and group_by_raw.strip():
        group_items = [group_by_raw.strip()]
    group_by_clause = ", ".join(group_items)

    aggregations_raw = intent.get("aggregations") if isinstance(intent.get("aggregations"), list) else []
    aggregations: List[Dict[str, Any]] = []
    for entry in aggregations_raw or []:
        if not isinstance(entry, dict):
            continue
        func = str(entry.get("func") or "").strip().upper()
        if not func:
            continue
        column_raw = entry.get("column")
        if column_raw == "*" or str(column_raw or "").strip() == "*":
            column = "*"
        else:
            column = str(column_raw or "").strip().upper()
        distinct = bool(entry.get("distinct"))
        alias = entry.get("alias")
        alias_norm = str(alias or "").strip().upper() if alias else None
        aggregations.append(
            {
                "func": func,
                "column": column if column else "*",
                "distinct": distinct,
                "alias": alias_norm,
            }
        )
    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.apply_hints.agg.loaded",
                "count": len(aggregations),
                "aliases": [item.get("alias") for item in aggregations],
            }
        )
    except Exception:
        pass

    gross_flag = intent.get("gross")
    needs_aggregation = bool(group_items or aggregations or gross_flag)
    if needs_aggregation:
        sql = _strip_trailing_order_by(sql)
        inner = sql.strip()
        select_parts: List[str] = []
        if group_items:
            select_parts.extend(group_items)

        measure_parts: List[str] = []
        if aggregations:
            for agg_entry in aggregations:
                func = agg_entry["func"]
                column = agg_entry["column"]
                distinct = agg_entry["distinct"]
                alias_norm = agg_entry.get("alias")
                inner_arg = "*" if column == "*" else column
                if distinct and inner_arg != "*":
                    inner_arg = f"DISTINCT {inner_arg}"
                expr = f"{func}({inner_arg})"
                if alias_norm:
                    expr += f" AS {alias_norm}"
                measure_parts.append(expr)
        else:
            default_expr = "COUNT(*) AS CNT"
            if gross_flag is True:
                default_expr = f"SUM({_GROSS_EXPR}) AS TOTAL_GROSS"
            measure_parts.append(default_expr)

        select_parts.extend(measure_parts)
        select_clause = ", ".join(select_parts)
        sql = (
            "SELECT "
            + select_clause
            + "\nFROM (\n"
            + inner
            + "\n) RATE_WRAP"
        )
        if group_items:
            sql += "\nGROUP BY " + group_by_clause
            meta["group_by"] = group_by_clause
        if aggregations:
            meta["aggregations"] = aggregations
            meta["agg"] = aggregations
        if gross_flag is not None:
            meta["gross"] = bool(gross_flag)

    sort_by = intent.get("sort_by")
    sort_desc_flag = intent.get("sort_desc")
    norm_sort_by, norm_sort_desc = normalize_order_hint(sort_by, sort_desc_flag)
    if norm_sort_by:
        allow_order = True
        has_group_by = bool(re.search(r"\bGROUP\s+BY\b", sql or "", flags=re.IGNORECASE))
        select_clause = ""
        group_clause = ""
        if has_group_by:
            select_match = re.search(r"SELECT\s+(?P<select>.+?)\bFROM\b", sql, flags=re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group("select") or ""
            group_match = re.search(r"GROUP\s+BY\s+(?P<group>.+?)(\bORDER\b|\Z)", sql, flags=re.IGNORECASE | re.DOTALL)
            if group_match:
                group_clause = group_match.group("group") or ""
            target_pattern = re.compile(rf"\b{re.escape(norm_sort_by)}\b", flags=re.IGNORECASE)
            in_select = bool(target_pattern.search(select_clause))
            in_group = bool(target_pattern.search(group_clause))
            if not (in_select or in_group):
                allow_order = False
                try:
                    logging.getLogger("dw").warning(
                        {
                            "event": "answer.apply_hints.order.skip",
                            "reason": "order_target_not_grouped",
                            "order_by": norm_sort_by,
                        }
                    )
                except Exception:
                    pass
        try:
            logging.getLogger("dw").info(
                {
                    "event": "answer.apply_hints.order.eval",
                    "order_by": norm_sort_by,
                    "has_group_by": has_group_by,
                    "allow_order": allow_order,
                }
            )
        except Exception:
            pass
        if allow_order:
            intent["sort_by"] = norm_sort_by
            intent["sort_desc"] = norm_sort_desc
            effective_desc = True if norm_sort_desc is None else bool(norm_sort_desc)
            clause = f"ORDER BY {norm_sort_by} {'DESC' if effective_desc else 'ASC'}"
            sql = replace_or_add_order_by(sql, clause)
            meta["order_by"] = clause

    try:
        logging.getLogger("dw").info(
            {
                "event": "answer.apply_hints.done",
                "where_count": len(where_clauses),
                "binds_count": len(binds or {}),
                "numeric_filters": len(numeric_filters_all),
            }
        )
    except Exception:
        pass
    meta["eq_filters"] = eq_applied
    if eq_alias_targets:
        meta["eq_alias_targets"] = {
            str(alias).upper(): [str(col).upper() for col in targets]
            for alias, targets in eq_alias_targets.items()
        }
    meta["numeric_filters"] = len(numeric_filters_all)
    meta["fts"] = fts_meta
    return sql, binds, meta


_LIKE_BIND_PATTERN = re.compile(
    r"UPPER\(\s*TRIM\(\s*(?P<col>[A-Z0-9_]+)\s*\)\s*\)\s+LIKE\s+UPPER\(\s*TRIM\(:eq_bg_(?P<idx>\d+)\)\s*\)"
)


def _drop_like_when_in(
    sql: str,
    binds: Dict[str, Any],
    eq_alias_targets: Optional[Dict[str, List[str]]],
    *,
    enabled: Optional[bool] = None,
) -> Tuple[str, Dict[str, Any]]:
    flag = enabled
    if flag is None:
        flag = env_flag("PLANNER_DROP_LIKE_WHEN_IN", False)
    if not flag:
        return sql, binds
    if not eq_alias_targets:
        return sql, binds
    columns = {
        str(col or "").strip().upper()
        for targets in eq_alias_targets.values()
        for col in (targets or [])
        if str(col or "").strip()
    }
    if not columns:
        return sql, binds

    removed_binds: set[str] = set()

    def _replace(match: re.Match) -> str:
        col = match.group("col").upper()
        bind_name = f"eq_bg_{match.group('idx')}"
        if col not in columns:
            return match.group(0)
        removed_binds.add(bind_name)
        return "1=1"

    new_sql = _LIKE_BIND_PATTERN.sub(_replace, sql)
    if not removed_binds:
        return sql, binds

    for name in removed_binds:
        binds.pop(name, None)

    # Simplify trivial TRUE conjunctions introduced by replacements.
    new_sql = re.sub(r"\(\s*1=1\s*\)", "1=1", new_sql)
    new_sql = re.sub(r"\bAND\s+1=1\b", "", new_sql, flags=re.IGNORECASE)
    new_sql = re.sub(r"\b1=1\s+AND\b", "", new_sql, flags=re.IGNORECASE)
    # Collapse redundant whitespace
    new_sql = re.sub(r"\s{2,}", " ", new_sql)
    return new_sql, binds


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
    logger = logging.getLogger("dw")
    logger.info({"event": "start question"})  # موجودة لديك بالفعل
    t0 = time.time()
    payload = request.get_json(force=True, silent=False) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400
    # LOG: لوج لحالة الطلب الخام
    try:
        logger.info(
            {
                "event": "answer.payload",
                "auth_email": payload.get("auth_email"),
                "full_text_search": bool(payload.get("full_text_search")),
                "q_len": len(question or ""),
                "prefixes_len": len(payload.get("prefixes") or []),
            }
        )
    except Exception:
        logger.info({"event": "answer.payload"})

    # Observability: confirm env-driven feature flags per request
    try:
        logger.info(
            {
                "event": "answer.env.flags",
                "DW_USE_LARK_INTENT": os.getenv("DW_USE_LARK_INTENT"),
                "DW_NUM_EXTRACTOR": os.getenv("DW_NUM_EXTRACTOR"),
                "DW_USE_SPACY_ALIASES": os.getenv("DW_USE_SPACY_ALIASES"),
            }
        )
    except Exception:
        pass

    settings = _get_settings()
    online_intent: Dict[str, Any] = {}
    online_hints_applied = 0
    pipeline = _get_pipeline()
    seed_payload: Dict[str, Any] = {}
    seed_meta: Dict[str, Any] = {}
    seed_sql: str = ""
    seed_binds: Dict[str, Any] = {}
    try:
        seed_payload, seed_meta = _load_rate_hint_seed(question)
        if seed_payload:
            seed_intent = seed_payload.get("intent")
            if isinstance(seed_intent, dict) and seed_intent:
                online_intent.update(seed_intent)
                online_hints_applied += 1
            # LOG: وجدنا seed من /dw/rate
            logger.info(
                {
                    "event": "rules.seed.loaded",
                    "rule_id": seed_meta.get("rule_id"),
                    "has_intent": bool(seed_payload.get("intent")),
                    "has_sql": bool(seed_payload.get("resolved_sql")),
                }
            )
            raw_sql = seed_payload.get("resolved_sql")
            if isinstance(raw_sql, str):
                seed_sql = raw_sql.strip()
            elif raw_sql is not None:
                seed_sql = str(raw_sql)
            raw_binds = seed_payload.get("binds")
            if isinstance(raw_binds, dict):
                seed_binds = raw_binds
            if seed_sql:
                online_hints_applied = max(online_hints_applied, 1)
    except Exception as exc:
        LOGGER.warning("[dw] failed to load persisted rules: %s", exc)
    # Note: signature-first rule loading runs later after settings/columns are resolved
    try:
        recent_hints = load_recent_hints(question, ttl_seconds=900)
        online_hints_applied += len(recent_hints)
        for hint in recent_hints:
            apply_rate_hints(online_intent, hint, settings)
        # LOG: تلميحات حديثة (أونلاين)
        logger.info(
            {
                "event": "rules.recent.loaded",
                "count": len(recent_hints or []),
            }
        )
    except Exception as exc:
        LOGGER.warning("[dw] failed to load online hints: %s", exc)
        online_hints_applied = 1 if seed_payload else 0

    # --- Load persisted rules for this question (fts, order_by, eq, group_by) ---
    # We merge lightweight hints into online_intent so _apply_online_rate_hints()
    # can append WHERE/ORDER BY safely before executing SQL.
    try:
        qnorm = _normalize_question_text(question)
        kinds_loaded: list[str] = []
        rows = []
        with get_memory_session() as s:
            rows = (
                s.execute(
                    text(
                        """
                        SELECT rule_kind,
                               COALESCE(rule_payload, '{}'::jsonb) AS rule_payload
                          FROM dw_rules
                         WHERE enabled = TRUE
                           AND (COALESCE(question_norm, '') = '' OR question_norm = :qnorm)
                         ORDER BY id ASC
                        """
                    ),
                    {"qnorm": qnorm},
                )
                .mappings()
                .all()
            )
        for r in rows:
            kind = (r.get("rule_kind") or "").strip().lower()
            payload = r.get("rule_payload") or {}
            if isinstance(payload, str):
                try:
                    import json as _json
                    payload = _json.loads(payload)
                except Exception:
                    payload = {}
            if not isinstance(payload, dict):
                continue
            kinds_loaded.append(kind)
            if kind == "fts":
                tokens = payload.get("tokens") or []
                columns = payload.get("columns") or []
                operator = str(payload.get("operator") or "OR").upper()
                if tokens:
                    online_intent["fts_tokens"] = tokens
                    online_intent["full_text_search"] = True
                if columns:
                    online_intent["fts_columns"] = columns
                online_intent["fts_operator"] = "AND" if operator == "AND" else "OR"
            elif kind == "order_by":
                if payload.get("sort_by"):
                    online_intent["sort_by"] = payload["sort_by"]
                if "sort_desc" in payload:
                    online_intent["sort_desc"] = bool(payload.get("sort_desc"))
            elif kind == "eq":
                # Accept both shapes:
                #   1) [{"col": "ENTITY", "val": "DSFH", "op": "eq", "ci": true, "trim": true}, ...]
                #   2) [["ENTITY", ["DSFH"]], ["REPRESENTATIVE_EMAIL", ["samer@..."]]]
                raw = payload.get("eq_filters") or []
                canon: list[dict] = []
                if isinstance(raw, list):
                    for entry in raw:
                        if isinstance(entry, dict):
                            # Already canonical
                            canon.append(dict(entry))
                        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                            col, vals = entry
                            if not isinstance(col, str):
                                continue
                            vals_iter = vals if isinstance(vals, (list, tuple, set)) else [vals]
                            for v in vals_iter:
                                # Normalize into the shape expected by _apply_online_rate_hints/_where_from_eq_filters
                                canon.append({
                                    "col": col.strip(),
                                    "val": (v.strip() if isinstance(v, str) else v),
                                    "op": "eq",
                                    "ci": True,
                                    "trim": True,
                                })
                if canon:
                    existing = online_intent.setdefault("eq_filters", [])
                    for d in canon:
                        if d not in existing:
                            existing.append(d)
            elif kind == "group_by":
                if payload.get("group_by"):
                    online_intent["group_by"] = payload.get("group_by")
                if payload.get("gross") is not None:
                    online_intent["gross"] = bool(payload.get("gross"))
        if rows:
            online_hints_applied += len(rows)
        try:
            logger.info(
                {
                    "event": "answer.rules.persisted.loaded",
                    "count": len(rows),
                    "kinds": kinds_loaded,
                }
            )
        except Exception:
            logger.info({"event": "answer.rules.persisted.loaded"})
    except Exception as exc:
        LOGGER.warning("[dw] rules loader fell back: %s", exc)

    prefixes = _coerce_prefixes(payload.get("prefixes"))
    auth_email = payload.get("auth_email") or None
    full_text_search = bool(payload.get("full_text_search", False))
    overrides = {"full_text_search": full_text_search}
    if payload.get("_skip_stakeholder_has"):
        overrides["_skip_stakeholder_has"] = True

    namespace = (payload.get("namespace") or "dw::common").strip() or "dw::common"
    drop_like_flag_raw = _get_namespace_setting(settings, namespace, "PLANNER_DROP_LIKE_WHEN_IN", None)
    drop_like_flag = _coerce_bool_flag(drop_like_flag_raw, default=None)
    if drop_like_flag is None:
        drop_like_flag = env_flag("PLANNER_DROP_LIKE_WHEN_IN", False)

    table_name = _resolve_contract_table(settings, namespace)
    initial_getter = getattr(settings, "get_json", None) or getattr(settings, "get", None)
    allowed_columns_initial = load_explicit_filter_columns(
        initial_getter,
        namespace,
        DEFAULT_EXPLICIT_FILTER_COLUMNS,
    )
    fts_map_initial = _extract_fts_map(settings, namespace)
    fts_columns_initial = _resolve_fts_columns_from_map(fts_map_initial, table_name)
    # LOG: ملخص الإعدادات الفعالة
    try:
        logger.info(
            {
                "event": "answer.settings.loaded",
                "namespace": namespace,
                "table": table_name,
                "fts_engine": fts_engine(),
                "fts_cols_count": len(fts_columns_initial or []),
                "allowed_eq_cols": len(allowed_columns_initial or []),
                "drop_like_when_in": bool(drop_like_flag),
            }
        )
    except Exception:
        logger.info({"event": "answer.settings.loaded"})

    # Prefer signature-based rules using a light intent (inline EQ + default order)
    # Signature-first (gated by DW_LEARNING_RULES_MATCH)
    try:
        mem_engine = get_memory_engine()
    except Exception:
        mem_engine = None
    try:
        raw_mode = getenv("DW_LEARNING_RULES_MATCH", "question_norm")
        # Be tolerant to quotes/spaces in .env values
        _mode = str(raw_mode or "").strip().strip('"\'').lower()
    except Exception:
        _mode = "question_norm"
    # Attempt signature-first when memory engine is available; fall back gracefully
    if mem_engine is not None:
        qnorm = _normalize_question_text(question)
        light_intent = _build_light_intent_from_question(question, allowed_columns_initial)
        # Augment signature intent with alias-based EQ from settings (DW_EQ_ALIAS_COLUMNS)
        try:
            alias_map_raw = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}
            alias_keys = [str(k).strip() for k in alias_map_raw.keys() if str(k).strip()]
            comparison_markers = list(_COMPARISON_TRAILING_MARKERS)
            _augment_light_intent_with_aliases(
                question,
                light_intent,
                alias_keys,
                list(allowed_columns_initial or []),
                comparison_markers,
            )
            if isinstance(light_intent.get("eq_filters"), list) and alias_map_raw:
                alias_targets_index: Dict[str, Tuple[str, ...]] = {}
                canonical_for_targets: Dict[Tuple[str, ...], str] = {}

                def _score_alias(name: str) -> Tuple[int, int, str]:
                    text = name or ""
                    return (
                        1 if text.endswith("S") and not text.endswith("SS") else 0,
                        len(text),
                        text,
                    )

                for alias, cols in alias_map_raw.items():
                    alias_key = str(alias or "").strip().upper()
                    cols_tuple = tuple(
                        sorted(str(c or "").strip().upper() for c in (cols or []) if str(c or "").strip())
                    )
                    alias_targets_index[alias_key] = cols_tuple
                    if not cols_tuple:
                        continue
                    current = canonical_for_targets.get(cols_tuple)
                    if current is None or _score_alias(alias_key) > _score_alias(current):
                        canonical_for_targets[cols_tuple] = alias_key

                def _canonical_alias(name: str) -> str:
                    alias_key = str(name or "").strip().upper()
                    cols_tuple = alias_targets_index.get(alias_key)
                    if not cols_tuple:
                        return alias_key
                    return canonical_for_targets.get(cols_tuple, alias_key)

                existing_aliases: set[str] = set()
                for entry in list(light_intent["eq_filters"]):
                    if isinstance(entry, dict):
                        col = str(entry.get("col") or entry.get("field") or "").strip().upper()
                        canonical = _canonical_alias(col)
                        entry["col"] = col
                        existing_aliases.add(col)
                        if canonical and canonical != col:
                            existing_aliases.add(canonical)
                            if not any(
                                isinstance(it, dict) and str(it.get("col") or "").strip().upper() == canonical
                                for it in light_intent["eq_filters"]
                            ):
                                cloned = dict(entry)
                                cloned["col"] = canonical
                                light_intent["eq_filters"].append(cloned)
                    elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                        col = str(entry[0] or "").strip().upper()
                        canonical = _canonical_alias(col)
                        existing_aliases.add(col)
                        if canonical and canonical != col:
                            existing_aliases.add(canonical)
                            values = entry[1]
                            if not any(
                                isinstance(it, (list, tuple)) and len(it) == 2 and str(it[0] or "").strip().upper() == canonical
                                for it in light_intent["eq_filters"]
                            ):
                                light_intent["eq_filters"].append([canonical, values])
        except Exception:
            pass
        # Capture FTS groups for signature canonicalization
        try:
            sig_groups, _sig_mode = extract_fts_terms(question, force=False)
            if sig_groups:
                light_intent["fts_groups"] = sig_groups
        except Exception:
            pass
        # Alias-aware hint for signature-only: detect "departments = X" and add DEPARTMENT eq for matching
        try:
            m = re.search(r"(?i)\bdepartments?\s*=\s*([^\n\r;]+)", question or "")
            if m:
                val = m.group(1).strip().strip("'\"")
                if val:
                    eqs = light_intent.setdefault("eq_filters", [])
                    has_dep = False
                    for it in eqs:
                        if isinstance(it, (list, tuple)) and len(it) == 2 and str(it[0]).upper().strip() == "DEPARTMENT":
                            has_dep = True
                            break
                        if isinstance(it, dict) and str(it.get("col") or it.get("field") or "").upper().strip() == "DEPARTMENT":
                            has_dep = True
                            break
                    if not has_dep:
                        eqs.append(["DEPARTMENT", [val]])
                        try:
                            logger.info({"event": "answer.intent.alias_eq", "col": "DEPARTMENT", "val": val})
                        except Exception:
                            pass
        except Exception:
            pass
        try:
            eq_shape = (light_intent or {}).get("eq") if isinstance(light_intent, dict) else {}
            numeric_shape = (light_intent or {}).get("numeric_shape") if isinstance(light_intent, dict) else {}
            fts_groups = (light_intent or {}).get("fts_groups") if isinstance(light_intent, dict) else []
            meta = (light_intent or {}).get("_meta") if isinstance(light_intent, dict) else {}
            segments = {}
            if isinstance(meta, dict):
                segments = meta.get("segments") or {}
            logger.info(
                {
                    "event": "answer.intent.built",
                    "pipeline": (light_intent or {}).get("pipeline_version"),
                    "parser": (light_intent or {}).get("parser"),
                    "eq_cols": len(eq_shape or {}),
                    "numeric_cols": len(numeric_shape or {}),
                    "fts_groups": len(fts_groups or []),
                    "fts_tokens": sum(len(group) for group in (fts_groups or [])),
                    "segments": segments,
                }
            )
        except Exception:
            pass
        try:
            if globals().get("_LOAD_RULES_SRC") == "learning_store":
                logger.info({"event": "answer.rules.loader.fallback", "src": _LOAD_RULES_SRC})
        except Exception:
            pass
        log_intent_snapshot = None
        if str(os.getenv("LOG_INTENT_MATCH", "")).strip().lower() in {"1", "true", "yes", "on"}:
            try:
                from apps.dw.learning_store import intent_shape, signature_knobs  # type: ignore

                log_intent_snapshot = {
                    "question_norm": qnorm,
                    "signature": intent_shape(light_intent or {}),
                    "knobs": signature_knobs()._asdict(),
                }
            except Exception:
                log_intent_snapshot = None
        if log_intent_snapshot:
            try:
                logger.info({"event": "answer.intent.signature.snapshot", **log_intent_snapshot})
            except Exception:
                pass
        try:
            merged = _load_rules_by_sig(mem_engine, qnorm, intent=light_intent)
            try:
                logger.info(
                    {
                        "event": "answer.rules.signature.loaded",
                        "by": "intent_sha|rule_signature",
                        "has_eq": bool((merged or {}).get("eq_filters")),
                        "has_order": bool((merged or {}).get("order") or (merged or {}).get("sort_by")),
                    }
                )
            except Exception:
                pass
        except Exception as e:
            try:
                logger.warning({"event": "answer.rules.signature.failed", "err": str(e)})
            except Exception:
                pass
            merged = {}
        if isinstance(merged, dict) and merged:
            if merged.get("fts_tokens"):
                online_intent["fts_tokens"] = merged.get("fts_tokens")
                online_intent["full_text_search"] = True
            if merged.get("fts_columns"):
                online_intent["fts_columns"] = merged.get("fts_columns")
            if merged.get("fts_operator"):
                online_intent["fts_operator"] = merged.get("fts_operator")
            # Carry over learned eq_like fragments (alias -> tokens)
            try:
                if isinstance(merged.get("eq_like"), dict) and merged.get("eq_like"):
                    eq_like_src = merged.get("eq_like") or {}
                    eq_like_dst = online_intent.setdefault("eq_like", {})
                    for ak, toks in eq_like_src.items():
                        key = str(ak or "").strip().upper()
                        if not key:
                            continue
                        src_list = [str(t or "").strip().upper() for t in (toks or []) if str(t or "").strip()]
                        if not src_list:
                            continue
                        if key in eq_like_dst and isinstance(eq_like_dst.get(key), list):
                            cur = list(eq_like_dst.get(key) or [])
                            for t in src_list:
                                if t not in cur:
                                    cur.append(t)
                            eq_like_dst[key] = cur
                        else:
                            eq_like_dst[key] = src_list
            except Exception:
                pass
            # Merge EQ with preference to question values (light intent) over seed/rule values
            try:
                q_eq = (light_intent or {}).get("eq_filters") or []
                ex_eq = online_intent.get("eq_filters") or []
                mg_eq = merged.get("eq_filters") or []
                # First prefer question over existing (seed), then prefer question over merged
                combined = _merge_eq_prefer_question(q_eq, ex_eq)
                combined = _merge_eq_prefer_question(q_eq, combined)
                # Include any merged-only columns as well
                combined = _merge_eq_prefer_question(q_eq, mg_eq if mg_eq else combined)
                # Deduplicate by column preserving order and keep comparator metadata
                dedup: Dict[str, Any] = {}
                order: List[str] = []
                for item in combined:
                    if isinstance(item, dict):
                        col = item.get("col") or item.get("field")
                        if not isinstance(col, str):
                            continue
                        cu = col.strip().upper()
                        if not cu:
                            continue
                        if cu not in dedup:
                            canon_item = dict(item)
                            canon_item["col"] = cu
                            dedup[cu] = canon_item
                            order.append(cu)
                    elif isinstance(item, (list, tuple)) and len(item) == 2:
                        col = item[0]
                        if not isinstance(col, (str, bytes)):
                            continue
                        cu = str(col).strip().upper()
                        if not cu:
                            continue
                        vals = item[1]
                        values = []
                        if isinstance(vals, (list, tuple, set)):
                            values = list(vals)
                        elif vals is not None:
                            values = [vals]
                        if cu not in dedup:
                            dedup[cu] = [cu, values]
                            order.append(cu)
                    else:
                        continue
                if dedup:
                    canon_eq: List[Dict[str, Any]] = []
                    for cu in order:
                        entry = dedup.get(cu)
                        if entry is None:
                            continue
                        if isinstance(entry, dict):
                            canon_entry = dict(entry)
                            canon_entry.setdefault("op", "eq")
                            if canon_entry.get("ci") is None:
                                canon_entry["ci"] = True
                            if canon_entry.get("trim") is None:
                                canon_entry["trim"] = True
                            canon_eq.append(canon_entry)
                        elif isinstance(entry, list) and len(entry) == 2:
                            _, values = entry
                            values_seq = values if isinstance(values, (list, tuple, set)) else [values]
                            for v in values_seq:
                                canon_eq.append(
                                    {
                                        "col": cu,
                                        "val": v,
                                        "op": "eq",
                                        "ci": True,
                                        "trim": True,
                                    }
                                )
                    if canon_eq:
                        online_intent["eq_filters"] = canon_eq
                        try:
                            alias_map_for_expand = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}
                            if isinstance(alias_map_for_expand, dict):
                                mapped: Dict[str, List[str]] = {}
                                for ak, cols in alias_map_for_expand.items():
                                    if not isinstance(cols, (list, tuple, set)):
                                        continue
                                    bucket: List[str] = []
                                    seen_cols: set[str] = set()
                                    for c in cols:
                                        s = str(c or "").strip().upper()
                                        if s and s not in seen_cols:
                                            seen_cols.add(s)
                                            bucket.append(s)
                                    if bucket:
                                        mapped[str(ak or "").strip().upper()] = bucket
                                if mapped:
                                    _expand_eq_aliases_with_map(online_intent, mapped)
                        except Exception:
                            pass
            except Exception:
                # Fallback to simple extend
                if merged.get("eq_filters"):
                    existing = online_intent.setdefault("eq_filters", [])
                    for item in merged.get("eq_filters"):
                        if item not in existing:
                            existing.append(item)
            # Prefer numeric filters from the question intent when present
            try:
                if merged.get("numeric_filters") and not online_intent.get("numeric_filters"):
                    online_intent["numeric_filters"] = merged.get("numeric_filters")
                if merged.get("numeric_shape") and not online_intent.get("numeric_shape"):
                    online_intent["numeric_shape"] = merged.get("numeric_shape")
                q_numeric = (light_intent or {}).get("numeric_filters") or []
                if q_numeric:
                    online_intent["numeric_filters"] = q_numeric
                q_numeric_shape = (light_intent or {}).get("numeric_shape")
                if q_numeric_shape:
                    online_intent["numeric_shape"] = q_numeric_shape
            except Exception:
                pass
            # Carry over learned cross-column OR groups (e.g., alias expansions)
            try:
                og = merged.get("or_groups") or []
                if isinstance(og, list) and og:
                    existing_og = online_intent.setdefault("or_groups", [])
                    for grp in og:
                        if isinstance(grp, list) and grp:
                            existing_og.append(grp)
            except Exception:
                pass
            try:
                if online_intent.get("or_groups"):
                    payload.setdefault("_skip_stakeholder_has", True)
                    overrides["_skip_stakeholder_has"] = True
            except Exception:
                pass
            # Support both merged["order"] or sort_by/sort_desc keys
            order_obj = merged.get("order") if isinstance(merged.get("order"), dict) else None
            if order_obj:
                if order_obj.get("col"):
                    online_intent["sort_by"] = order_obj.get("col")
                if "desc" in order_obj:
                    online_intent["sort_desc"] = bool(order_obj.get("desc"))
            if merged.get("sort_by"):
                online_intent["sort_by"] = merged.get("sort_by")
            if merged.get("sort_desc") is not None:
                online_intent["sort_desc"] = bool(merged.get("sort_desc"))
            if merged.get("group_by"):
                online_intent["group_by"] = merged.get("group_by")
            if merged.get("gross") is not None:
                online_intent["gross"] = bool(merged.get("gross"))
            if merged.get("aggregations"):
                online_intent["aggregations"] = merged.get("aggregations")
            online_hints_applied += 1
            try:
                logger.info(
                    {
                        "event": "answer.rules.signature.loaded",
                        "hints": [k for k in merged.keys() if not str(k).startswith("_")],
                    }
                )
            except Exception:
                pass

    # If no EQ was loaded from rules/seed, fall back to light-intent EQ parsed from the question
    try:
        if not online_intent.get("eq_filters"):
            li_eq = (light_intent or {}).get("eq_filters") if isinstance(light_intent, dict) else None
            if isinstance(li_eq, list) and li_eq:
                online_intent["eq_filters"] = li_eq
    except Exception:
        pass

    # Sanitize EQ filters; expand alias columns into OR groups; detect cross-column OR groups from the question
    try:
        _sanitize_eq_values(online_intent, allowed_columns_initial)
        # Namespace alias expansion (e.g., DEPARTMENT/OWNER/EMAIL/PHONE etc.) using DW_EQ_ALIAS_COLUMNS
        # Skip early alias expansion here; we expand after inline EQ parsing to avoid duplicates.
        try:
            _ = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {})
        except Exception:
            pass
        try:
            logger.info(
                {
                    "event": "answer.eq.sanitized",
                    "eq_filters": online_intent.get("eq_filters"),
                }
            )
        except Exception:
            pass
        or_groups = _extract_or_groups_from_question(question, allowed_columns_initial)
        if or_groups:
            online_intent["or_groups"] = or_groups
        # NLU: inline equality from question with multi-value OR per column
        inline_pairs = parse_eq_inline(question, allowed_columns_initial)
        if inline_pairs:
            try:
                logger.info({"event": "answer.inline_eq.parsed", "pairs": len(inline_pairs)})
            except Exception:
                pass
            existing = online_intent.setdefault("eq_filters", [])
            for col, vals in inline_pairs:
                existing.append([col, list(vals)])
            # Re-run alias expansion after adding inline EQ so aliases (e.g., DEPARTMENTS)
            # are expanded into their target columns before applying rate hints.
            try:
                alias_map_raw2 = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}
                alias_map2: Dict[str, List[str]] = {}
                if isinstance(alias_map_raw2, dict):
                    for k, cols in alias_map_raw2.items():
                        if not isinstance(cols, (list, tuple, set)):
                            continue
                        bucket: List[str] = []
                        seen2: set[str] = set()
                        for c in cols:
                            s2 = str(c or "").strip().upper()
                            if s2 and s2 not in seen2:
                                seen2.add(s2)
                                bucket.append(s2)
                        if bucket:
                            alias_map2[str(k or "").strip().upper()] = bucket
                # Also capture inline alias assignments like 'DEPARTMENTS = X' directly from the question
                # so we don't depend solely on parse_eq_inline (which only sees allowed base columns).
                if alias_map2:
                    try:
                        alias_keys = list(alias_map2.keys())
                        if alias_keys:
                            import re as _re
                            alias_pat = r"(?:" + "|".join(_re.escape(a) for a in alias_keys) + r")"
                            rx = _re.compile(rf"(?i)\b({alias_pat})\s*=\s*([^\n\r;]+)")
                            found = []
                            for m in rx.finditer(question or ""):
                                key = (m.group(1) or "").strip().upper()
                                val = (m.group(2) or "").strip().strip("'\"")
                                if key and val:
                                    found.append((key, val))
                            if found:
                                existing2 = online_intent.setdefault("eq_filters", [])
                                for kkey, vval in found:
                                    existing2.append([kkey, [vval]])
                    except Exception:
                        pass
                    _expand_eq_aliases_with_map(online_intent, alias_map2)
            except Exception:
                pass
    except Exception:
        pass
    if full_text_search:
        direct_groups, direct_mode = extract_fts_terms(question, force=False)
        # LOG: مصطلحات FTS المستخرجة (وضع مباشر/غيره)
        logger.info(
            {
                "event": "answer.fts.terms",
                "mode": direct_mode,
                "groups_count": len(direct_groups or []),
                "columns_count": len(fts_columns_initial or []),
                "groups": direct_groups,
            }
        )
        if direct_groups:
            online_intent["fts_tokens"] = direct_groups
            online_intent["full_text_search"] = True
        # Prefer direct FTS path whenever groups exist and columns are resolved.
        # This keeps strategy stable even with minor token changes.
        if direct_groups and fts_columns_initial:
            direct_where, direct_binds = build_fts_where_groups(direct_groups, fts_columns_initial)
            if direct_where:
                direct_sql = f'SELECT * FROM "{table_name}"\nWHERE {direct_where}'
            else:
                direct_sql = f'SELECT * FROM "{table_name}"'
            direct_sql = _append_order_by(direct_sql, "REQUEST_DATE", descending=True)

            # Observability: log online_intent EQ/OR presence before applying hints
            try:
                logger.info(
                    {
                        "event": "answer.apply_hints.pre",
                        "eq_len": len(online_intent.get("eq_filters") or []),
                        "or_len": len(online_intent.get("or_groups") or []),
                        "eq_preview": online_intent.get("eq_filters"),
                    }
                )
            except Exception:
                pass
            sanitized_patch = {
                key: value
                for key, value in online_intent.items()
                if key not in {"fts_tokens", "fts_columns", "fts_operator", "fts_op", "full_text_search"}
            }
            # Ensure alias EQ keys are expanded into OR groups for the direct FTS path
            try:
                eq_fp = sanitized_patch.get("eq_filters") or []
                if isinstance(eq_fp, list) and eq_fp:
                    alias_map_d_raw = _get_namespace_mapping(settings, namespace, "DW_EQ_ALIAS_COLUMNS", {}) or {}
                    alias_map_d: Dict[str, List[str]] = {}
                    if isinstance(alias_map_d_raw, dict):
                        for k, cols in alias_map_d_raw.items():
                            if not isinstance(cols, (list, tuple, set)):
                                continue
                            bucket: List[str] = []
                            seen_d: set[str] = set()
                            for c in cols:
                                sd = str(c or "").strip().upper()
                                if sd and sd not in seen_d:
                                    seen_d.add(sd)
                                    bucket.append(sd)
                            if bucket:
                                alias_map_d[str(k or "").strip().upper()] = bucket
                    # Fallback targets
                    if not alias_map_d:
                        targets = [
                            "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4",
                            "DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
                            "OWNER_DEPARTMENT",
                        ]
                        alias_map_d = {"DEPARTMENTS": targets, "DEPARTMENT": targets}
                    produced: List[List[Dict[str, Any]]] = []
                    keep_eq: List[Any] = []
                    def _norm_pair(it):
                        if isinstance(it, (list, tuple)) and len(it)==2:
                            col = str(it[0] or "").strip().upper()
                            vals = it[1]
                            vals = list(vals) if isinstance(vals, (list,tuple,set)) else [vals]
                            return col, vals, "eq"
                        if isinstance(it, dict):
                            col = str((it.get("col") or it.get("field") or "")).strip().upper()
                            val = it.get("val") if it.get("val") is not None else it.get("value")
                            vals = [] if val is None else ([val] if not isinstance(val,(list,tuple,set)) else list(val))
                            op = str(it.get("op") or "eq").strip().lower()
                            return col, vals, op
                        return None, None, None
                    for it in eq_fp:
                        col, vals, op = _norm_pair(it)
                        if not col:
                            keep_eq.append(it)
                            continue
                        if op and op not in {"eq", "in"}:
                            keep_eq.append(it)
                            continue
                        targets = alias_map_d.get(col)
                        if not targets:
                            keep_eq.append(it)
                            continue
                        for v in vals or []:
                            if isinstance(v, str):
                                value = v.strip().upper()
                            else:
                                value = v
                            grp = [
                                {
                                    "col": t,
                                    "values": [value],
                                    "op": op,
                                    "ci": True,
                                    "trim": True,
                                }
                                for t in targets
                            ]
                            if grp:
                                produced.append(grp)
                    if produced:
                        existing = sanitized_patch.get("or_groups")
                        if not isinstance(existing, list):
                            existing = []

                        def _dedupe_groups(groups: List[List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
                            def _canon(val: Any) -> str:
                                if isinstance(val, str):
                                    upper = val.strip().upper()
                                    key = upper
                                    if upper.endswith("IES"):
                                        key = upper[:-3] + "Y"
                                    elif upper.endswith("ES"):
                                        key = upper[:-2]
                                    elif upper.endswith("S"):
                                        key = upper[:-1]
                                    return key
                                return str(val)

                            seen: set[Tuple[Tuple[str, Tuple[str, ...]], ...]] = set()
                            deduped: List[List[Dict[str, Any]]] = []
                            for grp in groups or []:
                                if not isinstance(grp, list):
                                    continue
                                cleaned_entries: List[Dict[str, Any]] = []
                                signature_items: List[Tuple[str, Tuple[str, ...]]] = []
                                for item in grp:
                                    if not isinstance(item, dict):
                                        continue
                                    entry = dict(item)
                                    col = str((entry.get("col") or entry.get("column") or "")).strip().upper()
                                    vals = entry.get("values")
                                    if isinstance(vals, list):
                                        entry["values"] = [v.strip().upper() if isinstance(v, str) else v for v in vals]
                                        canon_vals = tuple(_canon(v) for v in entry["values"])
                                    else:
                                        entry["values"] = []
                                        canon_vals = tuple()
                                    if isinstance(entry.get("val"), str):
                                        entry["val"] = entry["val"].strip().upper()
                                    if entry["values"]:
                                        entry["val"] = entry["values"][0]
                                    cleaned_entries.append(entry)
                                    signature_items.append((col, canon_vals))
                                if not cleaned_entries:
                                    continue
                                signature = tuple(sorted(signature_items))
                                if signature in seen:
                                    continue
                                seen.add(signature)
                                deduped.append(cleaned_entries)
                            return deduped

                        produced_cols = {
                            str((entry.get("col") or entry.get("column") or "")).strip().upper()
                            for grp in produced for entry in grp if isinstance(entry, dict)
                        }

                        existing_filtered: List[List[Dict[str, Any]]] = []
                        for grp in existing:
                            if not isinstance(grp, list):
                                continue
                            cols = {
                                str((entry.get("col") or entry.get("column") or "")).strip().upper()
                                for entry in grp if isinstance(entry, dict)
                            }
                            if cols and cols.issubset(produced_cols):
                                continue
                            existing_filtered.append(grp)

                        if existing_filtered and _merge_or_prefer_question:
                            try:
                                merged_groups = _merge_or_prefer_question(existing_filtered, produced)
                            except Exception:
                                merged_groups = existing_filtered + produced
                        else:
                            merged_groups = existing_filtered + produced

                        sanitized_patch["or_groups"] = _dedupe_groups(merged_groups)
                        sanitized_patch["eq_filters"] = keep_eq
            except Exception:
                pass
            # Final fallback: if no OR groups yet, scan the question text for alias assignment
            try:
                if not sanitized_patch.get("or_groups"):
                    import re as _re
                    alias_targets = [
                        "DEPARTMENT_1","DEPARTMENT_2","DEPARTMENT_3","DEPARTMENT_4",
                        "DEPARTMENT_5","DEPARTMENT_6","DEPARTMENT_7","DEPARTMENT_8",
                        "OWNER_DEPARTMENT",
                    ]
                    m = _re.search(r"(?i)\bdepartments?\s*=\s*([^\n\r;]+)", question or "")
                    if m:
                        val = m.group(1).strip().strip("'\"")
                        if val:
                            value = val.upper()
                            grp = [
                                {
                                    "col": t,
                                    "values": [value],
                                    "op": "eq",
                                    "ci": True,
                                    "trim": True,
                                }
                                for t in alias_targets
                            ]
                            if grp:
                                og = sanitized_patch.setdefault("or_groups", [])
                                og.append(grp)
            except Exception:
                pass
            binds = dict(direct_binds)
            online_meta: Dict[str, Any] = {}
            if sanitized_patch:
                direct_sql, binds, online_meta = _apply_online_rate_hints(
                    direct_sql, binds, sanitized_patch
                )
                direct_sql, binds = _drop_like_when_in(
                    direct_sql,
                    binds,
                    online_meta.get("eq_alias_targets") if isinstance(online_meta, dict) else None,
                    enabled=drop_like_flag,
                )

            # LOG: تنفيذ SQL لمسار FTS المباشر
            logger.info(
                {
                    "event": "answer.sql.exec",
                    "strategy": "fts_direct",
                    "preview": (direct_sql[:500] + "…")
                    if len(direct_sql) > 500
                    else direct_sql,
                    "bind_names": list(binds.keys()),
                }
            )
            t_exec = time.time()
            binds = _coerce_bind_dates(binds)
            rows, cols, exec_meta = _execute_oracle(direct_sql, binds)
            logger.info(
                {
                    "event": "answer.sql.done",
                    "strategy": "fts_direct",
                    "rows": len(rows or []),
                    "cols": len(cols or []),
                    "ms": int((time.time() - t_exec) * 1000),
                }
            )
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

    # LOG: بدء تخطيط المسار الحتمي (Contract planner)
    logger.info({"event": "planner.contract.plan.start"})
    contract_sql, contract_binds, contract_meta = _plan_contract_sql(
        question,
        namespace,
        today=date.today(),
        overrides=overrides,
    )
    if contract_sql:
        # LOG: نجاح التخطيط (ملخص)
        try:
            logger.info(
                {
                    "event": "planner.contract.plan.ok",
                    "has_sql": True,
                    "binds_count": len(contract_binds or {}),
                    "meta_keys": list((contract_meta or {}).keys()),
                }
            )
        except Exception:
            logger.info({"event": "planner.contract.plan.ok"})
        binds = _coerce_bind_dates(dict(contract_binds or {}))
        # Final guard: if no EQ present in online intent, fallback to light-intent EQ
        try:
            if not (online_intent.get("eq_filters") or online_intent.get("or_groups")):
                li_eq = (light_intent or {}).get("eq_filters") if isinstance(light_intent, dict) else None
                if isinstance(li_eq, list) and li_eq:
                    online_intent["eq_filters"] = li_eq
                    try:
                        logger.info({"event": "answer.intent.eq.fallback_li", "cols": len(li_eq)})
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            logger.info(
                {
                    "event": "answer.online_intent.snapshot",
                    "keys": sorted(list(online_intent.keys())),
                    "sort_by": online_intent.get("sort_by"),
                    "sort_desc": online_intent.get("sort_desc"),
                }
            )
        except Exception:
            pass
        contract_sql, binds, online_meta = _apply_online_rate_hints(contract_sql, binds, online_intent)
        contract_sql, binds = _drop_like_when_in(
            contract_sql,
            binds,
            online_meta.get("eq_alias_targets") if isinstance(online_meta, dict) else None,
            enabled=drop_like_flag,
        )
        if ":top_n" in contract_sql and "top_n" not in binds:
            binds["top_n"] = 10
        # LOG: تنفيذ SQL للمسار الحتمي
        logger.info(
            {
                "event": "answer.sql.exec",
                "strategy": "contract_deterministic",
                "preview": (contract_sql[:500] + "…")
                if len(contract_sql) > 500
                else contract_sql,
                "bind_names": list(binds.keys()),
            }
        )
        t_exec = time.time()
        rows, cols, exec_meta = _execute_oracle(contract_sql, binds)
        logger.info(
            {
                "event": "answer.sql.done",
                "strategy": "contract_deterministic",
                "rows": len(rows or []),
                "cols": len(cols or []),
                "ms": int((time.time() - t_exec) * 1000),
            }
        )
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

    if seed_sql:
        try:
            seeded_binds = _coerce_bind_dates(dict(seed_binds or {}))
            # LOG: تنفيذ SQL لمسار seed (من /dw/rate)
            logger.info(
                {
                    "event": "answer.sql.exec",
                    "strategy": "seed_rule",
                    "preview": (seed_sql[:500] + "…")
                    if len(seed_sql) > 500
                    else seed_sql,
                    "bind_names": list(seeded_binds.keys()),
                }
            )
            t_exec = time.time()
            rows, cols, exec_meta = _execute_oracle(seed_sql, seeded_binds)
            logger.info(
                {
                    "event": "answer.sql.done",
                    "strategy": "seed_rule",
                    "rows": len(rows or []),
                    "cols": len(cols or []),
                    "ms": int((time.time() - t_exec) * 1000),
                }
            )
            inquiry_id = _log_inquiry(
                question,
                auth_email,
                status="answered",
                rows=len(rows),
                prefixes=prefixes,
                payload=payload,
            )
            duration_ms = int((time.time() - t0) * 1000)
            seed_learning: Dict[str, Any] = {
                "hints": max(online_hints_applied, 1),
                "seed_rule_id": seed_meta.get("rule_id"),
            }
            if seed_payload.get("intent"):
                seed_learning["seed_intent"] = seed_payload.get("intent")
            if seeded_binds:
                seed_learning["seed_bind_keys"] = list(seeded_binds.keys())
            response = {
                "ok": True,
                "inquiry_id": inquiry_id,
                "rows": rows,
                "columns": cols,
                "sql": seed_sql,
                "meta": {
                    "strategy": "rate_hint_seed",
                    "binds": _json_safe_binds(seeded_binds),
                    **exec_meta,
                    "duration_ms": duration_ms,
                    "online_learning": seed_learning,
                },
                "debug": {
                    "seed_rule_id": seed_meta.get("rule_id"),
                    "online_learning": seed_learning,
                },
            }
            return _respond(payload, response)
        except Exception as exc:
            LOGGER.warning("[dw] failed to execute seed SQL: %s", exc)

    # LOG: تجربة fallback (LIKE + EQ) عند تعسّر المسارات السابقة
    logger.info({"event": "answer.like_eq_fallback.try"})
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
        try:
            meta = like_response.get("meta") or {}
            logger.info(
                {
                    "event": "answer.like_eq_fallback.used",
                    "rows": int(meta.get("rows") or 0),
                    "duration_ms": int(meta.get("duration_ms") or 0),
                }
            )
        except Exception:
            logger.info({"event": "answer.like_eq_fallback.used"})
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
        # LOG: تنفيذ SQL لمسار explicit_filters
        logger.info(
            {
                "event": "answer.sql.exec",
                "strategy": "explicit_filters",
                "preview": (sql[:500] + "…") if len(sql) > 500 else sql,
                "bind_names": list(binds.keys()),
            }
        )
        t_exec = time.time()
        binds = _coerce_bind_dates(binds)
        rows, cols, exec_meta = _execute_oracle(sql, binds)
        logger.info(
            {
                "event": "answer.sql.done",
                "strategy": "explicit_filters",
                "rows": len(rows or []),
                "cols": len(cols or []),
                "ms": int((time.time() - t_exec) * 1000),
            }
        )

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

    boolean_debug = build_boolean_debug(question, fts_columns)

    final_sql = sql
    LOGGER.info(json.dumps({"final_sql": {"size": len(final_sql.split()), "sql": final_sql}}))
    # LOG: تنفيذ SQL للمسار النهائي (planner fallback)
    logger.info(
        {
            "event": "answer.sql.exec",
            "strategy": "planner_fallback",
            "preview": (sql[:500] + "…") if len(sql) > 500 else sql,
            "bind_names": list((binds or {}).keys()),
        }
    )
    t_exec = time.time()
    sql, binds, online_meta = _apply_online_rate_hints(sql, binds or {}, online_intent)
    binds = _coerce_bind_dates(binds or {})
    rows, cols, exec_meta = _execute_oracle(sql, binds)
    logger.info(
        {
            "event": "answer.sql.done",
            "strategy": "planner_fallback",
            "rows": len(rows or []),
            "cols": len(cols or []),
            "ms": int((time.time() - t_exec) * 1000),
        }
    )

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
    intent_debug = {
        "explicit_dates": _dates_to_iso(explicit_dates),
        "top_n": top_n,
        "full_text_search": full_text_search,
        "fts": {
            "mode": fts_mode,
            "tokens": fts_groups,
            "columns": fts_columns,
        },
    }
    if isinstance(boolean_debug, dict):
        blocks_dbg = boolean_debug.get("blocks")
        if blocks_dbg:
            intent_debug["boolean_groups"] = blocks_dbg
        where_dbg = boolean_debug.get("where_text")
        if isinstance(where_dbg, str) and where_dbg.strip():
            intent_debug["boolean_groups_where"] = where_dbg.strip()
        bind_dbg = boolean_debug.get("binds")
        if isinstance(bind_dbg, dict) and bind_dbg:
            intent_debug["boolean_groups_binds"] = list(bind_dbg.keys())

    response = {
        "ok": True,
        "inquiry_id": inquiry_id,
        "rows": rows,
        "columns": cols,
        "sql": sql,
        "meta": meta_out,
        "explain": explain,
        "debug": {"intent": intent_debug},
    }
    debug_section = response.get("debug") if isinstance(response, dict) else None
    if isinstance(debug_section, dict):
        debug_section["_precomputed_boolean_debug"] = boolean_debug

    eq_where_text = (
        boolean_debug.get("where_text") if isinstance(boolean_debug, dict) else None
    )
    where_parts: List[str] = []
    if fts_where_sql:
        where_parts.append(str(fts_where_sql))
    if isinstance(eq_where_text, str) and eq_where_text.strip():
        eq_clause = eq_where_text.strip()
        if not (eq_clause.startswith("(") and eq_clause.endswith(")")):
            eq_clause = f"({eq_clause})"
        where_parts.append(eq_clause)

    final_sql_lines: List[str] = [f'SELECT * FROM "{table_name}"']
    if where_parts:
        final_sql_lines.append("WHERE " + " AND ".join(where_parts))
    final_sql_lines.append("ORDER BY REQUEST_DATE DESC")
    existing_meta_binds = (
        response.get("meta", {}).get("binds")
        if isinstance(response.get("meta"), dict)
        else None
    )
    if isinstance(existing_meta_binds, dict) and "top_n" in existing_meta_binds:
        final_sql_lines.append("FETCH FIRST :top_n ROWS ONLY")
    response["sql"] = "\n".join(final_sql_lines)

    combined_binds: Dict[str, Any] = {}
    if fts_where_sql and isinstance(fts_binds, dict):
        for key, value in fts_binds.items():
            combined_binds.setdefault(key, value)
    eq_bind_map = (
        boolean_debug.get("binds") if isinstance(boolean_debug, dict) else None
    )
    if isinstance(eq_bind_map, dict):
        for key, value in eq_bind_map.items():
            combined_binds[key] = value
    filtered_meta_binds: Dict[str, Any] = {}
    if isinstance(existing_meta_binds, dict):
        filtered_meta_binds = dict(existing_meta_binds)
        if isinstance(eq_bind_map, dict) and eq_bind_map:
            filtered_meta_binds = {
                key: value
                for key, value in filtered_meta_binds.items()
                if not (
                    isinstance(key, str)
                    and key.lower().startswith("eq_")
                    and not key.lower().startswith("eq_bg_")
                )
            }
        for key, value in filtered_meta_binds.items():
            combined_binds.setdefault(key, value)
    if combined_binds and isinstance(response.get("meta"), dict):
        response["meta"]["binds"] = _json_safe_binds(combined_binds)

    if isinstance(response.get("debug"), dict):
        error_value = meta_fts.get("error") if isinstance(meta_fts, dict) else None
        response["debug"]["fts"] = {
            "enabled": bool(meta_fts.get("enabled")) if isinstance(meta_fts, dict) else False,
            "mode": meta_fts.get("mode") if isinstance(meta_fts, dict) else None,
            "tokens": meta_fts.get("tokens") if isinstance(meta_fts, dict) else [],
            "columns": meta_fts.get("columns") if isinstance(meta_fts, dict) else [],
            "binds": meta_fts.get("binds") if isinstance(meta_fts, dict) else None,
        }
        if error_value and error_value != "no_engine":
            response["debug"]["fts"]["error"] = error_value
        debug_section = response["debug"]
        fts_debug = debug_section.setdefault("fts", {})
        settings_obj = get_settings()
        if hasattr(settings_obj, "get"):
            try:
                fts_debug["engine"] = settings_obj.get("DW_FTS_ENGINE", "like")
            except TypeError:
                fts_debug["engine"] = settings_obj.get("DW_FTS_ENGINE") or "like"
        else:
            fts_debug["engine"] = "like"
        if fts_debug.get("error") == "no_engine":
            fts_debug.pop("error", None)
        debug_section["online_learning"] = {
            "hints": online_hints_applied,
            **({} if not online_meta else online_meta),
        }
    # LOG: نهاية دورة السؤال
    try:
        logger.info(
            {
                "event": "answer.end",
                "inquiry_id": inquiry_id,
                "strategy": response.get("meta", {}).get("strategy"),
                "duration_ms": int(response.get("meta", {}).get("duration_ms") or 0),
            }
        )
    except Exception:
        logger.info({"event": "answer.end"})
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
