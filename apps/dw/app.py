"""DocuWare DW blueprint backed by a deterministic contract planner."""
from __future__ import annotations

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
from apps.dw.rate_hints import append_where, parse_rate_hints, replace_or_add_order_by
from apps.dw.settings_defaults import DEFAULT_EXPLICIT_FILTER_COLUMNS
from apps.dw.settings_utils import load_explicit_filter_columns
from apps.dw.tables.contracts import build_contract_sql
from apps.mem.kv import get_settings_for_namespace
from .contracts.contract_common import build_fts_clause
from .contracts.filters import parse_explicit_filters
from .contracts.contract_planner import plan_contract_query
from .rating import rate_bp

LOGGER = logging.getLogger("dw.app")


dw_bp = Blueprint("dw", __name__)
dw_bp.register_blueprint(rate_bp, url_prefix="")


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


def _json_safe_binds(binds: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {}
    for key, value in (binds or {}).items():
        if isinstance(value, (date, datetime)):
            safe[key] = value.isoformat()
        else:
            safe[key] = value
    return safe


def _plan_contract_sql(
    question: str,
    namespace: str,
    *,
    today: date | None = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    settings = get_settings_for_namespace(namespace)
    sql, binds, meta = build_contract_sql(question, settings or {}, today=today)
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
            full_text_search=False,
            fts_columns=fts_columns,
            fts_tokens=[],
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


def _normalize_fts_clause(clause: str) -> str:
    text = (clause or "").strip()
    if not text:
        return ""
    if text.upper().startswith("AND "):
        text = text[4:].strip()
    return text


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


def _get_fts_columns(*, table: str, namespace: str) -> List[str]:
    settings = _get_settings()
    if settings is None:
        return []
    getter = getattr(settings, "get_fts_columns", None)
    if callable(getter):
        return getter(table)  # type: ignore[return-value]
    fts_map: Dict[str, Any] = {}
    json_getter = getattr(settings, "get_json", None)
    if callable(json_getter):
        try:
            fts_map = json_getter("DW_FTS_COLUMNS", scope="namespace", namespace=namespace) or {}
        except TypeError:
            fts_map = json_getter("DW_FTS_COLUMNS") or {}
    elif hasattr(settings, "get"):
        try:
            fts_map = settings.get("DW_FTS_COLUMNS", scope="namespace", namespace=namespace) or {}
        except TypeError:
            fts_map = settings.get("DW_FTS_COLUMNS") or {}
    if isinstance(fts_map, dict):
        columns = fts_map.get(table) or fts_map.get("*") or []
        if isinstance(columns, list):
            return [str(col) for col in columns]
        if isinstance(columns, str):
            return [part.strip() for part in columns.split(",") if part.strip()]
    return []


def _extract_fts_tokens(question: str) -> List[str]:
    tokens = [tok for tok in re.split(r"\W+", question or "") if len(tok) >= 3]
    return [tok.upper() for tok in tokens]


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


@dw_bp.post("/answer")
def answer():
    t0 = time.time()
    payload = request.get_json(force=True) or {}
    question = (payload.get("question") or "").strip()
    if not question:
        return jsonify({"ok": False, "error": "question required"}), 400

    prefixes = _coerce_prefixes(payload.get("prefixes"))
    auth_email = payload.get("auth_email") or None
    full_text_search = bool(payload.get("full_text_search") or False)

    namespace = (payload.get("namespace") or "dw::common").strip() or "dw::common"
    contract_sql, contract_binds, contract_meta = _plan_contract_sql(
        question,
        namespace,
        today=date.today(),
    )
    if contract_sql:
        binds = _coerce_bind_dates(dict(contract_binds or {}))
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
            },
            "explain": (contract_meta or {}).get("explain"),
            "debug": {"contract_planner": True},
        }
        return jsonify(response)

    namespace = "dw::common"
    settings = _get_settings()

    table_name = "Contract"
    getter = getattr(settings, "get", None) if settings is not None else None
    if callable(getter):
        try:
            configured_table = getter("DW_CONTRACT_TABLE", scope="namespace", namespace=namespace)
        except TypeError:
            configured_table = getter("DW_CONTRACT_TABLE")
        if configured_table:
            table_name = str(configured_table)

    explicit_dates = _resolve_window(question)

    allowed_columns = load_explicit_filter_columns(
        getattr(settings, "get_json", None) or getter,
        namespace,
        DEFAULT_EXPLICIT_FILTER_COLUMNS,
    )

    fts_columns = _get_fts_columns(table=table_name, namespace=namespace)
    fts_tokens = _extract_fts_tokens(question) if full_text_search else []

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

        if full_text_search and fts_columns and fts_tokens:
            fts_clause, fts_binds = build_fts_clause(fts_columns, fts_tokens)
            normalized = _normalize_fts_clause(fts_clause)
            if normalized:
                where_clauses.append(normalized)
                explain_bits.append(f"Applied full-text search for tokens {fts_tokens}.")
            binds.update(fts_binds)

        sql = f'SELECT * FROM "{table_name}"'
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        sql += f" ORDER BY {order_by} DESC"
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
                    "fts_tokens": fts_tokens,
                },
            },
        }
        return jsonify(response)

    sql, binds, meta, explain = plan_contract_query(
        question,
        explicit_dates=explicit_dates,
        top_n=top_n,
        full_text_search=full_text_search,
        fts_columns=fts_columns,
        fts_tokens=fts_tokens,
    )

    LOGGER.info("[dw] final_sql: %s", {"size": len(sql), "sql": sql})
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
    meta_out: Dict[str, Any] = {**(meta or {}), **exec_meta, "duration_ms": duration_ms, "explicit_filters": False}
    if "binds" not in meta_out:
        meta_out["binds"] = _json_safe_binds(binds or {})
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
                "fts_tokens": fts_tokens,
            }
        },
    }
    return jsonify(response)


def create_dw_blueprint(*args, **kwargs):
    return dw_bp
