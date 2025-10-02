from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

from apps.dw.fts_utils import build_boolean_fts_where, resolve_fts_columns

from .contract_common import GROSS_SQL, OVERLAP_PRED, explain_window


# --- utilities (keep comments English only) ---


def _get_fts_columns(settings: Any) -> List[str]:
    """Load configured FTS columns for Contract from dw::common settings."""

    def _settings_getter(key: str, default=None):
        if settings is None:
            return default
        getter = getattr(settings, "get_json", None)
        if callable(getter):
            try:
                value = getter(key, default)
            except TypeError:
                value = getter(key)  # type: ignore[misc]
            return value if value is not None else default
        if isinstance(settings, dict):
            return settings.get(key, default)
        return default

    resolved = resolve_fts_columns(_settings_getter, "Contract")
    seen: set[str] = set()
    normalized: List[str] = []
    for col in resolved:
        if not isinstance(col, str):
            continue
        norm = col.strip().strip('"')
        if not norm:
            continue
        upper = norm.upper()
        if upper not in seen:
            seen.add(upper)
            normalized.append(upper)
    return normalized


# allow common aliases -> real columns
_COLUMN_ALIASES = {
    # equality aliases
    "DEPARTMENT": "OWNER_DEPARTMENT",
    "DEPARTMENTS": "OWNER_DEPARTMENT",
    "OWNER_DEPT": "OWNER_DEPARTMENT",
    # stakeholder typos/variants -> special handler across 1..8
    "STAKEHOLDER": "STAKEHOLDER*",  # marker for 8-slot fanout
    "STACKHOLDER": "STAKEHOLDER*",
}


# 8-slot stakeholder column list
_STAKEHOLDER_SLOTS = [
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5",
    "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7",
    "CONTRACT_STAKEHOLDER_8",
]


def _alias_column(raw: str) -> str:
    """Map a free-text column-like token to a real column or special marker."""

    normalized = (raw or "").strip().upper().replace("-", "_").replace(" ", "_")
    return _COLUMN_ALIASES.get(normalized, normalized)


def _extract_has_terms(q: str) -> List[str]:
    """Extract unique search terms following 'has' or 'where has'."""

    m = re.search(r"\b(?:where\s+)?has\s+(.+)$", q or "", flags=re.IGNORECASE)
    if not m:
        return []
    tail = m.group(1).strip()
    if not tail:
        return []
    parts = re.split(r"\s+or\s+|,", tail, flags=re.IGNORECASE)
    terms: List[str] = []
    seen: set[str] = set()
    for part in parts:
        token = part.strip().strip("\"'")
        if not token:
            continue
        upper = token.upper()
        if upper in seen:
            continue
        seen.add(upper)
        terms.append(token)
    return terms


def _build_fts_clause(
    question: str,
    overrides: Optional[Dict[str, Any]],
    settings: Any,
    binds: Dict[str, Any],
    *,
    columns_override: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, Any]]:
    q = question or ""
    overrides = overrides or {}

    terms: List[str] = []
    if overrides.get("full_text_search"):
        terms = _extract_has_terms(q)
    if not terms and re.search(r"\bhas\b", q, flags=re.IGNORECASE):
        terms = _extract_has_terms(q)

    cols: List[str] = []
    if columns_override:
        seen: set[str] = set()
        for raw in columns_override:
            if not isinstance(raw, str):
                continue
            norm = raw.strip().strip('"')
            if not norm:
                continue
            upper = norm.upper()
            if upper not in seen:
                seen.add(upper)
                cols.append(upper)
    if not cols:
        cols = _get_fts_columns(settings)

    predicate = ""
    error: Optional[str] = None
    new_bind_keys: List[str] = []
    join_op: Optional[str] = None
    if terms and cols:
        before = set(binds.keys())
        raw_sql, binds, join_op = build_boolean_fts_where(
            question_text=q,
            terms=terms,
            fts_columns=cols,
            binds=binds,
            bind_prefix="fts",
        )
        if raw_sql:
            predicate = "(" + raw_sql + ")"
        new_bind_keys = [key for key in binds.keys() if key not in before]
    elif terms and not cols:
        error = "no_columns"

    mode = "override" if overrides.get("full_text_search") else ("implicit" if terms else None)
    meta: Dict[str, Any] = {
        "enabled": bool(predicate),
        "columns": cols if predicate else (cols or []),
        "tokens": terms if predicate else [],
        "mode": mode,
        "error": error,
    }
    if predicate:
        meta["binds"] = new_bind_keys
        if join_op:
            meta["join"] = join_op

    return predicate, meta


ALIAS = {
    "DEPARTMENTS": "OWNER_DEPARTMENT",
    "DEPARTMENT": "OWNER_DEPARTMENT",
    "OWNER DEPARTMENT": "OWNER_DEPARTMENT",
}


def _eq_pairs(q: str) -> List[Tuple[str, str]]:
    pairs = re.findall(
        r"\bwhere\s+([a-zA-Z0-9_ \-]+)\s*=\s*[\"']?(.+?)[\"']?(?:$|[.;,])",
        q or "",
        flags=re.IGNORECASE,
    )
    out: List[Tuple[str, str]] = []
    for raw_col, raw_val in pairs:
        norm = raw_col.strip().upper()
        if not norm:
            continue
        col = ALIAS.get(norm, norm.replace(" ", "_"))
        col = re.sub(r"[^A-Z0-9_]+", "_", col).strip("_")
        if not col:
            continue
        out.append((col.upper(), raw_val.strip()))
    return out


def _apply_fts_if_requested(
    question: str,
    payload: Dict[str, Any],
    settings: Any,
    where_clauses: List[str],
    binds: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """Append FTS clause when requested and return FTS metadata."""

    clause, meta = _build_fts_clause(question, payload, settings, binds)
    if clause:
        where_clauses.append(clause)
    return bool(clause), meta


def _apply_eq_filters_from_text(
    question: str,
    settings: Any,
    where_clauses: List[str],
    binds: Dict[str, Any],
) -> bool:
    """
    Parse simple 'column = value' expressions from free text using DW_EXPLICIT_FILTER_COLUMNS.
    Also supports aliases like 'departments' -> OWNER_DEPARTMENT.
    """

    import re

    allowed_raw: List[str] = []
    if settings is not None:
        getter = getattr(settings, "get_json", None)
        if callable(getter):
            allowed_raw = getter("DW_EXPLICIT_FILTER_COLUMNS", []) or []
        elif isinstance(settings, dict):
            allowed_raw = settings.get("DW_EXPLICIT_FILTER_COLUMNS", []) or []
    allowed = [str(c).upper() for c in allowed_raw if isinstance(c, str) and c.strip()]
    if not allowed:
        return False

    q = question or ""
    # find patterns: <word(s)> = <value> ; accept quotes or not
    matches = re.findall(r"(\b[ A-Za-z_]+?)\s*=\s*[\"']?([^\"']+)[\"']?", q)
    applied = False
    for raw_col, raw_val in matches:
        col = _alias_column(raw_col)
        if col == "STAKEHOLDER*":
            # treat as multi-slot equality (rare), but here we prefer 'has' pattern; skip
            continue

        if col not in allowed:
            continue

        bind_name = f"eq_{len(binds)}"
        binds[bind_name] = raw_val.strip()
        where_clauses.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))")
        applied = True
    return applied


def _apply_stakeholder_has(
    question: str,
    where_clauses: List[str],
    binds: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """
    Detect 'stakeholder has X [or Y ...]' (including 'stackholder') and create
    OR across 8 stakeholder slots for each term, then OR across terms.
    """

    import re

    match = re.search(r"\b(stackholder|stakeholder)\b\s+has\s+(.+)$", question, flags=re.IGNORECASE)
    if not match:
        return False, []
    tail = match.group(2)
    parts = re.split(r"\s+or\s+|,", tail, flags=re.IGNORECASE)
    terms = [p.strip().strip("\"'") for p in parts if p.strip()]
    if not terms:
        return False, []

    or_groups: List[str] = []
    for idx, term in enumerate(terms):
        bind_name = f"sh_{idx}"
        binds[bind_name] = f"%{term}%"
        ors = []
        for col in _STAKEHOLDER_SLOTS:
            ors.append(f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})")
        or_groups.append("(" + " OR ".join(ors) + ")")
    where_clauses.append("(" + " OR ".join(or_groups) + ")")
    return True, terms


def _build_text_filter_sql(
    question: str,
    payload: Dict[str, Any],
    settings: Any,
) -> Tuple[str, Dict[str, Any], Dict[str, Any], str, bool]:
    """Attempt to build simple Contract SQL using text-driven filters."""

    where_clauses: List[str] = []
    binds: Dict[str, Any] = {}

    stakeholder_applied, stakeholder_terms = _apply_stakeholder_has(question, where_clauses, binds)
    eq_applied = _apply_eq_filters_from_text(question, settings, where_clauses, binds)
    fts_applied, fts_meta = _apply_fts_if_requested(
        question, payload, settings, where_clauses, binds
    )

    applied = stakeholder_applied or eq_applied or fts_applied
    if not applied:
        return "", {}, {}, "", False

    sql = 'SELECT * FROM "Contract"\nWHERE ' + "\n  AND ".join(where_clauses) + "\nORDER BY REQUEST_DATE DESC"
    explain_bits: List[str] = []
    if stakeholder_applied:
        explain_bits.append("Matched stakeholder terms across stakeholder slots.")
    if eq_applied:
        explain_bits.append("Applied equality filters from the question.")
    if fts_applied:
        tokens_preview = ", ".join(fts_meta.get("tokens", [])) if isinstance(fts_meta, dict) else ""
        if tokens_preview:
            explain_bits.append(
                "Applied LIKE-based search over configured FTS columns for tokens: "
                + tokens_preview
                + "."
            )
        else:
            explain_bits.append("Applied LIKE-based search over configured FTS columns.")
    if not explain_bits:
        explain_bits.append("Applied text/FTS filters.")
    explain = " ".join(explain_bits)

    meta: Dict[str, Any] = {
        "strategy": "contract_deterministic",
        "contract_planner": True,
        "text_filters": True,
    }
    meta["fts"] = fts_meta if isinstance(fts_meta, dict) else {"enabled": bool(fts_applied)}
    if stakeholder_applied:
        meta["stakeholder_terms"] = stakeholder_terms

    return sql, binds, meta, explain, True

# Dimension aliases we support for GROUP BY on Contract table
DIMENSIONS = {
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "owner": "CONTRACT_OWNER",
    "owner_department": "OWNER_DEPARTMENT",
    "department_oul": "DEPARTMENT_OUL",
    "entity": "ENTITY",
    "entity_no": "ENTITY_NO",
}


RE_EQ_GENERIC = re.compile(
    r"(?i)\b([A-Z0-9_ ]+?)\s*=\s*(?:'([^']*)'|\"([^\"]*)\"|([^\s]+))"
)


def _norm_col(col: str) -> str:
    return col.strip().upper().replace(" ", "_")


def _extract_eq_filter(question: str) -> Optional[Dict[str, Any]]:
    match = RE_EQ_GENERIC.search(question or "")
    if not match:
        return None

    col = _norm_col(match.group(1))
    val = (match.group(2) or match.group(3) or match.group(4) or "").strip()
    if not col or not val:
        return None

    allowed = {"REQUEST_TYPE", "ENTITY_NO"}
    if col not in allowed:
        return None

    bind = "eq_0"
    predicate = f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind}))"
    order = "REQUEST_DATE DESC" if col == "REQUEST_TYPE" else None
    return {"predicate": predicate, "binds": {bind: val}, "order": order, "col": col}


def _pick_measure(q: str) -> str:
    ql = (q or "").lower()
    if "gross" in ql:
        return GROSS_SQL
    # default: net contract value
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def _pick_window_strategy(q: str) -> str:
    """
    RULE:
      - If question explicitly mentions 'requested', use REQUEST_DATE window.
      - Otherwise, for generic 'contracts last X', use OVERLAP (START..END).
    """
    ql = (q or "").lower()
    if "request" in ql or "requested" in ql:
        return "REQUEST_DATE"
    return "OVERLAP"


def _build_window_pred(date_col: str) -> str:
    dc = date_col.upper()
    if dc == "REQUEST_DATE":
        return "REQUEST_DATE BETWEEN :date_start AND :date_end"
    if dc == "OVERLAP":
        return OVERLAP_PRED
    # Fallback to request_date
    return "REQUEST_DATE BETWEEN :date_start AND :date_end"


def _resolve_groupby(q: str) -> Optional[str]:
    ql = (q or "").lower()
    # heuristic for "by/per X"
    for key, col in DIMENSIONS.items():
        if f" by {key}" in ql or f" per {key}" in ql:
            return col
        # also support exact phrases common in your examples
        if key in ql and (" by " in ql or " per " in ql):
            return col
    if "stakeholder" in ql:
        return DIMENSIONS["stakeholder"]
    if "owner department" in ql or "department" in ql:
        return DIMENSIONS["owner_department"]
    if "department_oul" in ql:
        return DIMENSIONS["department_oul"]
    if "entity no" in ql:
        return DIMENSIONS["entity_no"]
    if "entity" in ql:
        return DIMENSIONS["entity"]
    return None


def plan_contract_query(
    q: str,
    *,
    explicit_dates: Optional[Tuple[date, date]],
    top_n: Optional[int],
    payload: Optional[Dict[str, Any]],
    settings: Optional[Dict[str, Any]] = None,
    fts_columns: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any], str]:
    """
    Deterministic planner for Contract table queries.
    Returns: (sql, binds, meta, explain)
    """
    payload = payload or {}

    text_sql, text_binds, text_meta, text_explain, text_applied = _build_text_filter_sql(
        q or "", payload, settings
    )
    if text_applied:
        return text_sql, text_binds, text_meta, text_explain

    measure = _pick_measure(q)
    group_col = _resolve_groupby(q)
    wants_count = "(count" in (q or "").lower() or " count" in (q or "").lower()
    date_col = _pick_window_strategy(q)
    window_pred = _build_window_pred(date_col)
    explain_bits: List[str] = []

    binds: Dict[str, Any] = {}
    if explicit_dates:
        ds, de = explicit_dates
        binds["date_start"] = ds
        binds["date_end"] = de
        explain_bits.append(explain_window(date_col, ds, de))
    else:
        explain_bits.append("No explicit window; using default or none.")

    fts_clause, fts_meta = _build_fts_clause(
        q or "",
        payload,
        settings,
        binds,
        columns_override=fts_columns,
    )
    if not isinstance(fts_meta, dict):
        fts_meta = {"enabled": False}

    if fts_meta.get("enabled"):
        tokens_preview = ", ".join(fts_meta.get("tokens", []))
        if tokens_preview:
            explain_bits.append(
                "Applied full-text search over "
                + str(len(fts_meta.get("columns", [])))
                + " columns for tokens: "
                + tokens_preview
                + "."
            )
        else:
            explain_bits.append("Applied full-text search over configured FTS columns.")
    elif fts_meta.get("error") == "no_columns" and payload.get("full_text_search"):
        explain_bits.append("Full-text search requested but no FTS columns configured.")

    eq_filter = _extract_eq_filter(q)
    if eq_filter:
        binds.update(eq_filter["binds"])

    eq_pairs = _eq_pairs(q or "")
    skip_eq_cols = {eq_filter["col"]} if eq_filter else set()
    eq_predicates: List[str] = []
    for col, val in eq_pairs:
        if col in skip_eq_cols:
            continue
        base = re.sub(r"[^A-Z0-9]+", "_", col).strip("_").lower()
        if not base:
            continue
        bind_name = f"eq_{base}"
        suffix = 1
        while bind_name in binds:
            bind_name = f"eq_{base}_{suffix}"
            suffix += 1
        binds[bind_name] = val
        eq_predicates.append(f"UPPER(TRIM({col})) = UPPER(TRIM(:{bind_name}))")

    def _attach_fts(meta: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(meta)
        enriched["fts"] = dict(fts_meta)
        return enriched

    # Patterns
    ql = (q or "").lower()
    sql: str

    if "expiring" in ql and wants_count:
        # Contracts expiring in X days (count) → COUNT on END_DATE window (inclusive)
        date_col = "END_DATE"
        explain_bits.append("Interpreting 'expiring' as END_DATE between window.")
        window_pred = "END_DATE BETWEEN :date_start AND :date_end"
        where_parts = [window_pred]
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        sql = "SELECT COUNT(*) AS CNT FROM \"Contract\""
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        return sql, binds, _attach_fts({"group_by": None, "measure": "COUNT", "date_col": date_col}), " ".join(explain_bits)

    if group_col and not wants_count:
        where_parts: List[str] = []
        if explicit_dates:
            where_parts.append(window_pred)
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)

        sql_lines: List[str] = []
        if eq_filter:
            sql_lines.append(
                "SELECT\n"
                f"  {group_col} AS GROUP_KEY,\n"
                f"  SUM({GROSS_SQL}) AS TOTAL_GROSS,\n"
                "  COUNT(*) AS CNT\n"
                "FROM \"Contract\""
            )
        else:
            sql_lines.append(
                "SELECT\n"
                f"  {group_col} AS GROUP_KEY,\n"
                f"  SUM({measure}) AS MEASURE\n"
                "FROM \"Contract\""
            )

        if where_parts:
            sql_lines.append("WHERE " + " AND ".join(where_parts))

        sql = "\n".join(sql_lines)
        sql += f"\nGROUP BY {group_col}"
        if eq_filter:
            sql += "\nORDER BY TOTAL_GROSS DESC"
            explain_bits.append(
                f"Aggregating gross and count by {group_col} with equality filter."
            )
            meta = {"group_by": group_col, "measure": "GROSS", "date_col": date_col}
        else:
            sql += "\nORDER BY MEASURE DESC"
            explain_bits.append(
                f"Aggregating by {group_col} and ordering by SUM(measure) DESC."
            )
            meta = {"group_by": group_col, "measure": "SUM", "date_col": date_col}

        if top_n:
            binds["top_n"] = int(top_n)
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds, _attach_fts(meta), " ".join(explain_bits)

    if wants_count and not group_col:
        # Count by request window (or overlap if mentioned)
        if "status" in ql:
            # "Count of contracts by status" → grouped count
            where_parts: List[str] = []
            if eq_filter:
                where_parts.append(eq_filter["predicate"])
            if eq_predicates:
                where_parts.extend(eq_predicates)
            if fts_clause:
                where_parts.append(fts_clause)
            sql = "SELECT CONTRACT_STATUS AS GROUP_KEY, COUNT(*) AS CNT FROM \"Contract\""
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
            sql += " GROUP BY CONTRACT_STATUS ORDER BY CNT DESC"
            explain_bits.append("Grouped count by CONTRACT_STATUS.")
            return sql, binds, _attach_fts({"group_by": "CONTRACT_STATUS", "measure": "COUNT"}), " ".join(explain_bits)
        # Else: simple count in window if exists, else all time
        where_parts: List[str] = []
        if explicit_dates:
            where_parts.append(window_pred)
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        sql = "SELECT COUNT(*) AS CNT FROM \"Contract\""
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        explain_bits.append("Returning COUNT(*) without grouping.")
        return sql, binds, _attach_fts({"group_by": None, "measure": "COUNT", "date_col": date_col}), " ".join(explain_bits)

    # Top contracts (no group) by measure
    if "top" in ql and "contract" in ql:
        select_cols = "*"
        where_parts: List[str] = []
        if explicit_dates:
            where_parts.append(window_pred)
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        sql = (
            f"SELECT {select_cols} FROM \"Contract\"\n"
            f"{where_clause}\n"
            f"ORDER BY {measure} DESC"
        )
        if top_n:
            binds["top_n"] = int(top_n)
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        explain_bits.append("Top contracts by measure (descending).")
        return sql, binds, _attach_fts({"group_by": None, "measure": measure, "date_col": date_col}), " ".join(explain_bits)

    # Requested last X (explicit on REQUEST_DATE)
    if "requested" in ql:
        where_parts = ["REQUEST_DATE BETWEEN :date_start AND :date_end"]
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE " + " AND ".join(where_parts) + "\nORDER BY REQUEST_DATE DESC"
        )
        explain_bits.append("Requested window detected; sorting by REQUEST_DATE DESC.")
        return sql, binds, _attach_fts({"date_col": "REQUEST_DATE"}), " ".join(explain_bits)

    # Specific filters:
    if "vat" in ql and ("null" in ql or "zero" in ql):
        # VAT null or zero and positive contract value
        pred = "(NVL(VAT, 0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0)"
        where_parts = [pred]
        if explicit_dates:
            where_parts.append(window_pred)
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        base = "SELECT * FROM \"Contract\"\nWHERE " + " AND ".join(where_parts)
        base += "\nORDER BY " + measure + " DESC"
        explain_bits.append("Applied VAT null/zero and value > 0 predicate.")
        return base, binds, _attach_fts({"filter": "vat_zero_or_null"}), " ".join(explain_bits)

    if "distinct entity" in ql or ("list" in ql and "entity" in ql and "count" in ql):
        where_parts: List[str] = []
        if eq_filter:
            where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        sql = "SELECT ENTITY AS GROUP_KEY, COUNT(*) AS CNT FROM \"Contract\""
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        sql += " GROUP BY ENTITY ORDER BY CNT DESC"
        explain_bits.append("Distinct ENTITY with counts.")
        return sql, binds, _attach_fts({"group_by": "ENTITY", "measure": "COUNT"}), " ".join(explain_bits)

    if eq_filter:
        where_parts = []
        if explicit_dates:
            where_parts.append(window_pred)
        where_parts.append(eq_filter["predicate"])
        if eq_predicates:
            where_parts.extend(eq_predicates)
        if fts_clause:
            where_parts.append(fts_clause)
        sql = "SELECT * FROM \"Contract\""
        if where_parts:
            sql += "\nWHERE " + " AND ".join(where_parts)
        order = eq_filter.get("order") or "REQUEST_DATE DESC"
        sql += f"\nORDER BY {order}"
        explain_bits.append(f"Applied equality filter on {eq_filter['col']} from the question.")
        return sql, binds, _attach_fts({"group_by": None, "filter": eq_filter["col"], "date_col": date_col}), " ".join(explain_bits)

    # Fallback: list in window (if any) else all
    sql = "SELECT * FROM \"Contract\""
    where_parts = []
    if explicit_dates:
        where_parts.append(window_pred)
    if eq_predicates:
        where_parts.extend(eq_predicates)
    if fts_clause:
        where_parts.append(fts_clause)
    if where_parts:
        sql += "\nWHERE " + " AND ".join(where_parts)
    sql += "\nORDER BY REQUEST_DATE DESC"
    explain_bits.append("Fallback listing ordered by REQUEST_DATE DESC.")
    return sql, binds, _attach_fts({"fallback": True, "date_col": date_col}), " ".join(explain_bits)
