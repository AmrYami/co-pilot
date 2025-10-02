"""SQL planner for DocuWare Contract table based on DWIntent."""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .intent import DWIntent
from .enums import load_enum_synonyms
from .sql_builder import attach_where_clause, build_where_from_filters
from apps.settings import get_setting_json

DIMENSIONS_ALLOWED = {"OWNER_DEPARTMENT", "DEPARTMENT_OUL", "ENTITY_NO", "ENTITY"}


# ---------- helpers: FTS + aliases ----------


def _expand_numbered_columns(cols: List[str]) -> List[str]:
    """Expand patterns like DEPARTMENT_1 to DEPARTMENT_1..8 and CONTRACT_STAKEHOLDER_1..8."""

    out: List[str] = []
    seen = set()
    for col in cols:
        if col in seen:
            continue
        seen.add(col)
        out.append(col)
        match = re.match(r"^(DEPARTMENT|CONTRACT_STAKEHOLDER)_(\d+)$", col, re.I)
        if match:
            base = match.group(1).upper()
            for i in range(1, 9):
                expanded = f"{base}_{i}"
                if expanded not in seen:
                    seen.add(expanded)
                    out.append(expanded)
    return out


def _get_fts_columns(ns_settings: Optional[Dict[str, Any]]) -> List[str]:
    """
    Try to read DW_FTS_COLUMNS['Contract'] or ['*'], else provide a sane fallback.
    Auto-include numbered columns 1..8 when one of them appears.
    """

    cfg = (ns_settings or {}).get("DW_FTS_COLUMNS") or {}
    cols = cfg.get("Contract") or cfg.get("*") or []
    if not cols:
        cols = [
            "CONTRACT_SUBJECT",
            "CONTRACT_PURPOSE",
            "OWNER_DEPARTMENT",
            "DEPARTMENT_OUL",
            "CONTRACT_STAKEHOLDER_1",
            "DEPARTMENT_1",
        ]
    cols = [c.upper() for c in cols]
    cols = _expand_numbered_columns(cols)
    seen: set[str] = set()
    uniq: List[str] = []
    for col in cols:
        if col not in seen:
            seen.add(col)
            uniq.append(col)
    return uniq


def _extract_fts_terms(text: str, force_short: bool = False) -> Tuple[List[str], str]:
    """
    Extract FTS tokens/phrases.
    - If 'or' appears => OR logic, otherwise AND.
    - Keep short tokens when force_short=True (for full_text_search=true).
    - Preserve dashes (e.g. E-123), allow phrases like 'home care'.
    """

    match = re.search(r"\b(has|contains|with)\s+(.+)$", text, re.I)
    source = match.group(2) if match else text

    op = "OR" if re.search(r"\bor\b", source, re.I) else "AND"

    parts = re.split(r"\b(?:and|or)\b", source, flags=re.I)
    tokens: List[str] = []
    for part in parts:
        chunk = part.strip(" ,;")
        if not chunk:
            continue
        quoted = re.findall(r"'([^']+)'|\"([^\"]+)\"", chunk)
        if quoted:
            for a, b in quoted:
                phrase = (a or b).strip()
                if phrase:
                    tokens.append(phrase)
            chunk = re.sub(r"'[^']+'|\"[^\"]+\"", " ", chunk)

        for word in re.split(r"[\s,;]+", chunk):
            word = word.strip()
            if not word:
                continue
            if not force_short and len(word) < 3:
                continue
            tokens.append(word)

    seen_tokens: set[str] = set()
    ordered: List[str] = []
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        lowered = token.lower()
        if lowered not in seen_tokens:
            seen_tokens.add(lowered)
            ordered.append(token)
    return ordered, op


def _build_fts_clause(
    fts_columns: List[str],
    tokens: List[str],
    op: str = "AND",
    binds: Optional[Dict[str, object]] = None,
    bind_prefix: str = "fts",
) -> Tuple[Optional[str], Dict[str, object]]:
    """
    Build ( (C1 LIKE :b0 OR C2 LIKE :b0 ...) [op] (C1 LIKE :b1 OR ...) ... )
    Returns: (sql_fragment, new_binds)
    """

    binds = binds or {}
    disjunctions: List[str] = []
    idx = 0
    for token in tokens:
        like_bind = f"{bind_prefix}_{idx}"
        binds[like_bind] = f"%{token}%"
        predicates = [f"UPPER({col}) LIKE UPPER(:{like_bind})" for col in fts_columns]
        disjunctions.append("(" + " OR ".join(predicates) + ")")
        idx += 1
    if not disjunctions:
        return None, binds
    glue = " OR " if op.upper() == "OR" else " AND "
    fragment = "(" + glue.join(disjunctions) + ")"
    return fragment, binds


_DEPT_ALIAS_COLS = [
    "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL",
] + [f"DEPARTMENT_{i}" for i in range(1, 9)]
_STAKEHOLDER_ALIAS_COLS = [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)]


def _build_eq_alias_or(
    cols: List[str], value: str, binds: Dict[str, object], bind_prefix: str
) -> str:
    predicates: List[str] = []
    for idx, col in enumerate(cols):
        bind_name = f"{bind_prefix}_{idx}"
        binds[bind_name] = value
        predicates.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))")
    return "(" + " OR ".join(predicates) + ")"


# ---------- utils: normalization & settings ----------


def _norm_col_name(raw: str) -> str:
    """Normalize human-entered column labels to DB column style."""

    return re.sub(r"\s+", "_", raw.strip()).upper()


def _load_json_setting(mem, namespace: str, key: str, default):
    try:
        value = mem.get(namespace, key, scope="namespace")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


# ---------- explicit equality extraction ----------


_EQ_RE = re.compile(r"(?i)\b([A-Z][A-Z _0-9]+)\s*=\s*([\"\']?)([^\"\';\)\n]+)\2")


def _extract_explicit_equals_filters(question: str):
    """Return list of (raw_col, value) extracted from text like 'COL = value'."""

    out: List[Tuple[str, str]] = []
    if not question:
        return out
    for match in _EQ_RE.finditer(question):
        raw_col = match.group(1)
        val = match.group(3).strip()
        val = re.split(r"\s*\(", val, 1)[0].strip()
        if val.endswith("'") or val.endswith('"'):
            val = val[:-1].strip()
        out.append((raw_col, val))
    return out


def _build_enum_synonym_predicates(
    table_name: str,
    col: str,
    value: str,
    enum_map: dict,
    binds: Dict[str, object],
):
    """Expand equality using DW_ENUM_SYNONYMS if present."""

    key = f"{table_name}.{col}"
    col_map = enum_map.get(key) or enum_map.get(col)
    if not isinstance(col_map, dict):
        return None, binds
    value_u = value.strip().upper()
    equals_list: List[str] = []
    prefix_list: List[str] = []
    contains_list: List[str] = []

    for bucket, rules in col_map.items():
        for eqv in (rules.get("equals", []) or []):
            if value_u == eqv.upper():
                equals_list.extend(rules.get("equals", []) or [])
                prefix_list.extend(rules.get("prefix", []) or [])
                contains_list.extend(rules.get("contains", []) or [])
                break
        if equals_list:
            continue
        for px in (rules.get("prefix", []) or []):
            if value_u.startswith(px.upper()):
                equals_list.extend(rules.get("equals", []) or [])
                prefix_list.extend(rules.get("prefix", []) or [])
                contains_list.extend(rules.get("contains", []) or [])
                break

    if not (equals_list or prefix_list or contains_list):
        bind_name = f"eq_{col.lower()}"
        binds[bind_name] = value
        fragment = f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))"
        return fragment, binds

    ors: List[str] = []
    for idx, eqv in enumerate(equals_list):
        if not eqv:
            continue
        bind_name = f"eq_{col.lower()}_{idx}"
        binds[bind_name] = eqv
        ors.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))")
    for idx, px in enumerate(prefix_list):
        if not px:
            continue
        bind_name = f"px_{col.lower()}_{idx}"
        binds[bind_name] = f"{px}%"
        ors.append(f"UPPER(TRIM({col})) LIKE UPPER(:{bind_name})")
    for idx, ct in enumerate(contains_list):
        if not ct:
            continue
        bind_name = f"ct_{col.lower()}_{idx}"
        binds[bind_name] = f"%{ct}%"
        ors.append(f"UPPER(TRIM({col})) LIKE UPPER(:{bind_name})")

    fragment = "(" + " OR ".join(ors) + ")"
    return fragment, binds


def apply_explicit_equals_filters(
    question: str,
    table_name: str,
    mem,
    where_clauses: List[str],
    binds: Dict[str, object],
) -> bool:
    """Detect and apply explicit equality filters from the question text."""

    explicit_cols = _load_json_setting(
        mem, "dw::common", "DW_EXPLICIT_FILTER_COLUMNS", []
    )
    if isinstance(explicit_cols, dict):
        allowed = set(_norm_col_name(c) for c in explicit_cols.get(table_name, []))
        allowed |= set(_norm_col_name(c) for c in explicit_cols.get("*", []))
    else:
        allowed = set(_norm_col_name(c) for c in explicit_cols)

    enum_map = _load_json_setting(mem, "dw::common", "DW_ENUM_SYNONYMS", {}) or {}

    extracted = _extract_explicit_equals_filters(question)
    any_applied = False

    for raw_col, raw_val in extracted:
        col = _norm_col_name(raw_col)
        if col not in allowed:
            continue

        fragment, _ = _build_enum_synonym_predicates(
            table_name, col, raw_val, enum_map, binds
        )
        if fragment is None:
            bind_name = f"eq_{col.lower()}"
            binds[bind_name] = raw_val
            fragment = f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))"

        where_clauses.append(fragment)
        any_applied = True

    return any_applied


class _SettingsMemAdapter:
    """Adapter to provide a mem.get interface using settings accessors."""

    def __init__(
        self,
        namespace: str,
        settings_get,
        *,
        preload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._namespace = namespace
        self._settings_get = settings_get
        self._preload = preload or {}

    def get(self, namespace: str, key: str, scope: str = "namespace") -> Any:
        if key in self._preload:
            return self._preload[key]

        target_ns = namespace or self._namespace
        getter = self._settings_get
        if callable(getter):
            for kwargs in (
                {"scope": scope, "namespace": target_ns},
                {"scope": scope},
                {"namespace": target_ns},
                {},
            ):
                try:
                    value = getter(key, **kwargs)
                except TypeError:
                    continue
                if value is not None:
                    return value

        try:
            return get_setting_json(target_ns, key, None)
        except Exception:
            return None


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
    debug: Dict[str, object] = meta.setdefault("debug", {})

    question = intent.question or ""
    payload: Dict[str, Any] = {}
    if isinstance(intent.notes, dict):
        payload_raw = intent.notes.get("payload")
        if isinstance(payload_raw, dict):
            payload = payload_raw

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

    explicit_cols_raw = _load_setting("DW_EXPLICIT_FILTER_COLUMNS", []) or []
    enum_syn_setting = _load_setting("DW_ENUM_SYNONYMS", {}) or {}

    mem_settings_client = None
    if isinstance(intent.notes, dict):
        for key in ("mem_settings_client", "mem_settings"):
            candidate = intent.notes.get(key)
            if hasattr(candidate, "get"):
                mem_settings_client = candidate
                break
    if mem_settings_client is None:
        preload = {
            "DW_EXPLICIT_FILTER_COLUMNS": explicit_cols_raw,
            "DW_ENUM_SYNONYMS": enum_syn_setting,
        }
        mem_settings_client = _SettingsMemAdapter(
            namespace, settings_get, preload=preload
        )

    ns_settings: Optional[Dict[str, Any]] = None
    if isinstance(intent.notes, dict):
        ns_settings_raw = intent.notes.get("namespace_settings")
        if isinstance(ns_settings_raw, dict):
            ns_settings = ns_settings_raw
    if ns_settings is None:
        try:
            fts_cols_setting = mem_settings_client.get(
                namespace, "DW_FTS_COLUMNS", scope="namespace"
            )
        except Exception:
            fts_cols_setting = None
        ns_settings = {"DW_FTS_COLUMNS": fts_cols_setting} if fts_cols_setting else {}

    filters_raw = getattr(intent, "filters", None) or []
    filter_fragments, filter_binds = build_where_from_filters(settings_get, filters_raw)
    filters_applied = bool(filter_fragments)
    request_type_applied = any(
        isinstance(f, dict) and (f.get("column") or "").upper() == "REQUEST_TYPE"
        for f in (filters_raw or [])
    ) and filters_applied

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

    eq_filters_applied = apply_explicit_equals_filters(
        question=intent.question or "",
        table_name="Contract",
        mem=mem_settings_client,
        where_clauses=where_parts,
        binds=binds,
    )

    request_type_detected = False
    explicit_request = _extract_request_type_value(intent.question or "")
    if explicit_request and not request_type_applied:
        has_request_type_clause = any(
            "REQUEST_TYPE" in clause for clause in where_parts
        )
        if not has_request_type_clause:
            clause, rt_binds = _build_request_type_predicate(explicit_request, settings_get)
            if clause:
                where_parts.append(clause)
                binds.update(rt_binds)
                request_type_detected = True

    request_type_applied = request_type_applied or request_type_detected

    # ------------- Forced / Heuristic FTS -------------
    wants_fts = bool(payload.get("full_text_search"))
    has_fts_cue = bool(re.search(r"\b(has|contains|with)\b", question or "", re.I))
    if wants_fts or has_fts_cue:
        fts_cols = _get_fts_columns(ns_settings)
        tokens, op = _extract_fts_terms(question or "", force_short=True)
        if fts_cols and tokens:
            frag, binds = _build_fts_clause(fts_cols, tokens, op, binds, bind_prefix="fts")
            if frag:
                where_parts.append(frag)
                fts_debug = debug.setdefault("fts", {})
                fts_debug["enabled"] = True
                fts_debug["columns"] = fts_cols
                fts_debug["tokens"] = tokens
                fts_debug["mode"] = "override"
        else:
            fts_debug = debug.setdefault("fts", {})
            fts_debug["enabled"] = False
            fts_debug["error"] = "no_columns" if not fts_cols else "no_tokens"

    # ------------- Departments = X (alias across OWNER/DEPARTMENT_1..8/OUL) -------------
    m_dept = re.search(
        r"\bdepartments?\s*=\s*['\"]?([^'\"\n\r;]+)", (question or ""), re.I
    )
    if m_dept:
        dept_val = m_dept.group(1).strip()
        if dept_val:
            frag = _build_eq_alias_or(_DEPT_ALIAS_COLS, dept_val, binds, "eq_dept")
            where_parts.append(frag)
            debug.setdefault("eq_alias", {})["departments"] = {
                "value": dept_val,
                "cols": _DEPT_ALIAS_COLS,
            }

    # ------------- Stakeholder has A or B or C -------------
    m_st = re.search(
        r"\bstakeholder[s]?\b.*?\b(has|contains|with)\b\s+(.+)$",
        (question or ""),
        re.I,
    )
    if m_st:
        names_src = m_st.group(2)
        raw = re.split(r"\bor\b|,|;", names_src, flags=re.I)
        tokens = [t.strip(" '\"") for t in raw if t and t.strip(" '\"")]
        if tokens:
            frag, binds = _build_fts_clause(
                _STAKEHOLDER_ALIAS_COLS, tokens, op="OR", binds=binds, bind_prefix="st"
            )
            if frag:
                where_parts.append(frag)
                stake_debug = debug.setdefault("stakeholder", {})
                stake_debug["tokens"] = tokens
                stake_debug["cols"] = _STAKEHOLDER_ALIAS_COLS

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
            default_order_col = "REQUEST_DATE"
        elif (filters_applied or request_type_applied) and not intent.has_time_window:
            default_order_col = "REQUEST_DATE"

        explicit_order_by = (
            bool(intent.sort_by) or intent.sort_desc is not None or bool(intent.top_n)
        )

        if default_order_col:
            order_sql = f"ORDER BY {default_order_col} DESC"
        else:
            if (not explicit_order_by) and (
                debug.get("fts", {}).get("enabled") or debug.get("stakeholder")
            ):
                order_sql = "ORDER BY REQUEST_DATE DESC"
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
