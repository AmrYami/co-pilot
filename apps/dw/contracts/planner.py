"""SQL planner for DocuWare Contract table based on DWIntent."""

from __future__ import annotations

import re
from datetime import date
from typing import Dict, List, Optional, Tuple

from .intent import DWIntent
from .enums import load_enum_synonyms
from .sql_builder import attach_where_clause, build_where_from_filters
from apps.dw.contracts.eq_filters import detect_explicit_equality_filters
from apps.settings import get_setting_json

DIMENSIONS_ALLOWED = {"OWNER_DEPARTMENT", "DEPARTMENT_OUL", "ENTITY_NO", "ENTITY"}


def _overlap_clause() -> str:
    return (
        "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        "AND START_DATE <= :date_end AND END_DATE >= :date_start)"
    )


def _build_window(intent: DWIntent, binds: Dict[str, object]) -> Tuple[Optional[str], Optional[str]]:
    """Return WHERE clause for window and the window kind label."""

    has_start = "date_start" in binds and binds["date_start"] is not None
    has_end = "date_end" in binds and binds["date_end"] is not None
    if not (has_start and has_end):
        return None, None

    if intent.date_column == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end", "REQUEST"
    if intent.date_column == "END_ONLY":
        return "END_DATE BETWEEN :date_start AND :date_end", "END_ONLY"
    return _overlap_clause(), "OVERLAP"


def _apply_sort_asc_if_bottom(intent: DWIntent, default_desc: bool) -> bool:
    """Return final sort_desc considering 'bottom/lowest' signals."""

    if intent.sort_desc is not None:
        return bool(intent.sort_desc)
    if intent.is_bottom:
        return False
    return default_desc


def _extract_request_type_value(question: str) -> Optional[str]:
    """Detect explicit REQUEST TYPE value in the free-form question."""

    if not question:
        return None
    match = re.search(
        r"(?i)\bREQUEST[\s_\-]*TYPE\b\s*(?:=|:|is)?\s*['\"]?([A-Za-z][\w\s/\-]{0,64})",
        question,
    )
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def _build_request_type_predicate(
    value: str, settings_get
) -> Tuple[Optional[str], Dict[str, object]]:
    """Build a predicate for REQUEST_TYPE using DW_ENUM_SYNONYMS when available."""

    token = (value or "").strip()
    if not token:
        return None, {}

    synonyms = load_enum_synonyms(settings_get, table="Contract", column="REQUEST_TYPE")
    rules = synonyms.get(token.lower()) if synonyms else None

    fragments: List[str] = []
    binds: Dict[str, object] = {}
    idx = 0

    if rules:
        for eq in rules.get("equals", []) or []:
            if not eq:
                continue
            key = f"reqtype_eq_{idx}"
            idx += 1
            binds[key] = eq
            fragments.append(f"UPPER(TRIM(REQUEST_TYPE)) = UPPER(:{key})")
        for pref in rules.get("prefix", []) or []:
            if not pref:
                continue
            key = f"reqtype_px_{idx}"
            idx += 1
            binds[key] = f"{pref}%"
            fragments.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")
        for sub in rules.get("contains", []) or []:
            if not sub:
                continue
            key = f"reqtype_ct_{idx}"
            idx += 1
            binds[key] = f"%{sub}%"
            fragments.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")

    key = f"reqtype_like_{idx}"
    binds[key] = f"%{token}%"
    fragments.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")

    clause = "(" + " OR ".join(fragments) + ")" if fragments else None
    return clause, binds


def build_owner_vs_oul_mismatch_sql() -> str:
    """Rows where OWNER_DEPARTMENT and DEPARTMENT_OUL differ (lead = OUL)."""

    return (
        'SELECT OWNER_DEPARTMENT, DEPARTMENT_OUL, COUNT(*) AS CNT
'
        'FROM "Contract"
'
        "WHERE DEPARTMENT_OUL IS NOT NULL
"
        "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')
"
        "GROUP BY OWNER_DEPARTMENT, DEPARTMENT_OUL
"
        "ORDER BY CNT DESC"
    )


def _apply_intent_binds(intent: DWIntent, binds: Dict[str, object]) -> None:
    if intent.explicit_dates:
        start = intent.explicit_dates.get("start")
        end = intent.explicit_dates.get("end")
        if start and "date_start" not in binds:
            binds["date_start"] = start
        if end and "date_end" not in binds:
            binds["date_end"] = end

    if intent.notes.get("ytd"):
        if "date_start" not in binds or "date_end" not in binds:
            today = date.today()
            binds.setdefault("date_start", date(today.year, 1, 1))
            binds.setdefault("date_end", today)




def build_sql(intent: DWIntent) -> Tuple[str, Dict[str, object], Dict[str, object]]:
    """Build final SQL + binds + meta for the Contract table based on resolved intent."""

    binds: Dict[str, object] = {}
    meta: Dict[str, object] = {}
    where_parts: List[str] = []

    settings_get = None
    namespace = "dw::common"
    if isinstance(intent.notes, dict):
        settings_get = intent.notes.get("settings_get_json")
        ns_raw = intent.notes.get("namespace")
        if isinstance(ns_raw, str) and ns_raw.strip():
            namespace = ns_raw.strip()

    def _load_setting(key: str, default):
        if callable(settings_get):
            for kwargs in ({"default": default, "scope": "namespace"}, {"default": default}, {}):
                try:
                    value = settings_get(key, **kwargs)
                except TypeError:
                    continue
                if value is not None:
                    return value
        try:
            return get_setting_json(namespace, key, default)
        except Exception:
            return default

    def _ensure_list(value) -> List[str]:
        if isinstance(value, (list, tuple, set)):
            items: List[str] = []
            for v in value:
                if v is None:
                    continue
                text = str(v).strip()
                if text:
                    items.append(text.upper())
            return items
        if isinstance(value, str):
            return [part.strip().upper() for part in value.split(",") if part.strip()]
        return []

    def _normalize_explicit_setting(value):
        if isinstance(value, dict):
            normalized: Dict[str, List[str]] = {}
            for key, arr in value.items():
                normalized[str(key)] = _ensure_list(arr)
            return normalized
        return _ensure_list(value)

    explicit_cols_setting = _normalize_explicit_setting(
        _load_setting("DW_EXPLICIT_FILTER_COLUMNS", []) or []
    )
    fts_setting_raw = _load_setting("DW_FTS_COLUMNS", {}) or {}
    fts_setting = (
        {str(k): _ensure_list(v) for k, v in fts_setting_raw.items()}
        if isinstance(fts_setting_raw, dict)
        else _ensure_list(fts_setting_raw)
    )
    enum_syn_setting = _load_setting("DW_ENUM_SYNONYMS", {}) or {}

    filters_raw = getattr(intent, "filters", None) or []
    filter_fragments, filter_binds = build_where_from_filters(settings_get, filters_raw)
    filters_applied = bool(filter_fragments)
    request_type_applied = any(
        isinstance(f, dict) and (f.get("column") or "").upper() == "REQUEST_TYPE"
        for f in (filters_raw or [])
    ) and filters_applied

    request_type_detected = False
    explicit_request = _extract_request_type_value(intent.question or "")
    if explicit_request and not request_type_applied:
        clause, rt_binds = _build_request_type_predicate(explicit_request, settings_get)
        if clause:
            where_parts.append(clause)
            binds.update(rt_binds)
            request_type_detected = True

    measure = intent.measure_sql or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    q_lower = (intent.question or "").lower()
    if intent.notes.get("owner_vs_oul") or ("vs" in q_lower and "department_oul" in q_lower):
        sql = build_owner_vs_oul_mismatch_sql()
        meta.update({"explain": "Owner vs OUL mismatch rows (non-equal)."})
        return sql, binds, meta

    _apply_intent_binds(intent, binds)

    where_sql, window_kind = _build_window(intent, binds)
    if where_sql:
        where_parts.append(where_sql)
    if window_kind:
        meta["window_kind"] = window_kind

    request_type_applied = request_type_applied or request_type_detected

    eq_where, eq_binds, eq_suggested_order = detect_explicit_equality_filters(
        intent.question or "",
        table="Contract",
        explicit_cols_setting=explicit_cols_setting,
        fts_setting=fts_setting,
        enum_syn=enum_syn_setting,
    )
    eq_filters_applied = bool(eq_where)
    if eq_filters_applied:
        where_parts.append(f"({eq_where})")
        binds.update(eq_binds)

    if intent.group_by is None:
        sort_desc = _apply_sort_asc_if_bottom(intent, default_desc=True)

        top_sql = None
        if intent.top_n:
            binds["top_n"] = intent.top_n
            top_sql = "FETCH FIRST :top_n ROWS ONLY"

        base_parts = ['SELECT * FROM "Contract"']
        if where_parts:
            base_parts.append("WHERE " + " AND ".join(where_parts))
        base_sql = "\n".join(base_parts)
        base_sql = attach_where_clause(base_sql, filter_fragments)
        if filters_applied:
            binds.update(filter_binds)

        default_order_col: Optional[str] = None
        if eq_filters_applied:
            default_order_col = eq_suggested_order or "REQUEST_DATE"
        elif (filters_applied or request_type_applied) and not intent.has_time_window:
            default_order_col = "REQUEST_DATE"

        if default_order_col:
            order_sql = f"ORDER BY {default_order_col} DESC"
        else:
            order_sql = f"ORDER BY {measure} {'DESC' if sort_desc else 'ASC'}"

        sql_parts = [base_sql, order_sql]
        if top_sql:
            sql_parts.append(top_sql)
        sql = "\n".join(part for part in sql_parts if part)

        explain = (
            f"{'Top' if sort_desc else 'Bottom'} {intent.top_n or ''} by "
            f"{'GROSS' if measure != 'NVL(CONTRACT_VALUE_NET_OF_VAT,0)' else 'NET'}"
        ).strip()
        if request_type_applied:
            explain = "Applied REQUEST_TYPE filter from question. " + explain
        elif filters_applied:
            explain = "Applied filters from question. " + explain
        elif eq_filters_applied:
            explain = "Applied equality filters from question. " + explain

        meta_bits = {
            "explain": explain,
            "binds": {k: v for k, v in binds.items() if k == "top_n"},
        }
        if eq_filters_applied:
            meta_bits["eq_filters"] = True
        meta.update(meta_bits)
        return sql, binds, meta

    group_col = intent.group_by
    if group_col not in DIMENSIONS_ALLOWED:
        group_col = "OWNER_DEPARTMENT"

    agg = (intent.agg or ("SUM" if measure != "COUNT(*)" else "COUNT")).upper()
    if agg not in {"SUM", "AVG", "COUNT", "MEDIAN"}:
        agg = "SUM"

    sort_desc = _apply_sort_asc_if_bottom(intent, default_desc=True)
    order_sql = f"ORDER BY MEASURE {'DESC' if sort_desc else 'ASC'}"

    top_sql = None
    if intent.top_n:
        binds["top_n"] = intent.top_n
        top_sql = "FETCH FIRST :top_n ROWS ONLY"

    if agg == "COUNT":
        select_measure = "COUNT(*)"
    else:
        select_measure = f"{agg}({measure})"

    select_lines = [
        "SELECT",
        f"  {group_col} AS GROUP_KEY,",
        f"  {select_measure} AS MEASURE",
    ]

    base_parts = ["\n".join(select_lines), 'FROM "Contract"']
    if where_parts:
        base_parts.append("WHERE " + " AND ".join(where_parts))
    base_sql = "\n".join(base_parts)
    base_sql = attach_where_clause(base_sql, filter_fragments)
    if filters_applied:
        binds.update(filter_binds)

    sql_parts = [base_sql, f"GROUP BY {group_col}", order_sql]
    if top_sql:
        sql_parts.append(top_sql)

    sql = "\n".join(part for part in sql_parts if part)

    explain_group = f"{agg.title()} per {group_col} using {window_kind or 'ALL_TIME'} window."
    if request_type_applied:
        explain_group = "Applied REQUEST_TYPE filter from question. " + explain_group
    elif filters_applied:
        explain_group = "Applied filters from question. " + explain_group
    elif eq_filters_applied:
        explain_group = "Applied equality filters from question. " + explain_group

    meta_bits = {
        "group_by": group_col,
        "agg": agg.lower(),
        "gross": measure != "NVL(CONTRACT_VALUE_NET_OF_VAT,0)",
        "explain": explain_group,
        "binds": {k: v for k, v in binds.items() if k == "top_n"},
    }
    if eq_filters_applied:
        meta_bits["eq_filters"] = True
    meta.update(meta_bits)
    return sql, binds, meta
