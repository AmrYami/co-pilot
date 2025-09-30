from __future__ import annotations
import re
from datetime import date, datetime
from typing import Dict, Tuple, Optional, List

from .rules_extra import try_build_special_cases

# NOTE: Keep this module strictly table-specific (Contract).
#       Cross-table / DocuWare-generic helpers should live elsewhere.

_NET = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

_BOTTOM_RE = re.compile(r"\b(bottom|lowest|least|أقل)\b", re.IGNORECASE)
_REQUEST_RE = re.compile(r"\brequest(?:ed|s)?\b", re.IGNORECASE)

def gross_expr(alias: str | None = None) -> str:
    base = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    vat = "NVL(VAT,0)"
    expr = f"{base} + CASE WHEN {vat} BETWEEN 0 AND 1 THEN {base} * {vat} ELSE {vat} END"
    return f"{expr} AS {alias}" if alias else expr


# --- Helpers: measures / overlap predicate ---
GROSS_EXPR = gross_expr()


def overlap_pred() -> str:
    return _overlap_pred()


def build_top_gross_ytd(q: str, binds: Dict[str, object] | None, top_n: int, ascending: bool = False) -> Tuple[str, Dict[str, object]]:
    """Top-N contracts by gross for a YTD window inferred from question/binds."""
    text = q or ""
    lowered = text.lower()
    out_binds = dict(binds or {})
    today_hint = out_binds.pop("today", None)
    today_val = today_hint or date.today()
    if not isinstance(today_val, date):
        today_val = _as_date(today_val)

    # Prefer a year mentioned near YTD; otherwise fall back to current year-to-date.
    year = None
    near_year = re.search(r"\b(20\d{2})\b[^0-9a-z]{0,10}\bYTD\b", text, re.IGNORECASE)
    if near_year:
        year = int(near_year.group(1))
    else:
        near_year = re.search(r"\bYTD\b[^0-9a-z]{0,10}\b(20\d{2})\b", text, re.IGNORECASE)
        if near_year:
            year = int(near_year.group(1))
    if year is None and "ytd" in lowered:
        generic_year = re.search(r"\b(20\d{2})\b", text, re.IGNORECASE)
        if generic_year:
            year = int(generic_year.group(1))

    if year is not None:
        ds = date(year, 1, 1)
        de = date(year, 12, 31)
    else:
        ds = date(today_val.year, 1, 1)
        de = today_val

    try:
        top_n_int = int(top_n)
    except (TypeError, ValueError):
        top_n_int = 5
    if top_n_int <= 0:
        top_n_int = 5

    out_binds.update({
        "date_start": ds,
        "date_end": de,
        "top_n": top_n_int,
    })
    _ensure_date_binds(out_binds, "date_start", "date_end")

    order_dir = "ASC" if ascending else "DESC"
    sql = (
        'SELECT * FROM "Contract"\n'
        f"WHERE {_overlap_pred()}\n"
        f"ORDER BY {gross_expr()} {order_dir}\n"
        "FETCH FIRST :top_n ROWS ONLY"
    )
    return sql, out_binds


def build_yoy_gross_overlap(binds: Dict[str, object] | None) -> Tuple[str, Dict[str, object]]:
    """YoY gross totals using overlap windows for current and previous periods."""
    out = dict(binds or {})
    _ensure_date_binds(out, "ds", "de", "p_ds", "p_de")
    sql = (
        "SELECT 'CURRENT' AS PERIOD, SUM(" + gross_expr() + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {_overlap_pred(':ds', ':de')}\n"
        "UNION ALL\n"
        "SELECT 'PREVIOUS' AS PERIOD, SUM(" + gross_expr() + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {_overlap_pred(':p_ds', ':p_de')}"
    )
    return sql, out


def build_yoy_gross_requested(binds: Dict[str, object] | None) -> Tuple[str, Dict[str, object]]:
    """YoY gross totals using REQUEST_DATE windows for current and previous periods."""
    out = dict(binds or {})
    _ensure_date_binds(out, "ds", "de", "p_ds", "p_de")
    sql = (
        "SELECT 'CURRENT' AS PERIOD, SUM(" + gross_expr() + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        "WHERE REQUEST_DATE BETWEEN :ds AND :de\n"
        "UNION ALL\n"
        "SELECT 'PREVIOUS' AS PERIOD, SUM(" + gross_expr() + ") AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        "WHERE REQUEST_DATE BETWEEN :p_ds AND :p_de"
    )
    return sql, out


def build_owner_vs_oul_mismatch(binds: Dict[str, object] | None = None) -> Tuple[str, Dict[str, object]]:
    sql = (
        "SELECT NVL(TRIM(OWNER_DEPARTMENT), '(None)') AS OWNER_DEPARTMENT,\n"
        "       NVL(TRIM(DEPARTMENT_OUL), '(None)')   AS DEPARTMENT_OUL,\n"
        "       COUNT(*) AS CNT\n"
        'FROM "Contract"\n'
        "WHERE DEPARTMENT_OUL IS NOT NULL\n"
        "  AND NVL(TRIM(OWNER_DEPARTMENT), '(None)') <> NVL(TRIM(DEPARTMENT_OUL), '(None)')\n"
        "GROUP BY NVL(TRIM(OWNER_DEPARTMENT), '(None)'), NVL(TRIM(DEPARTMENT_OUL), '(None)')\n"
        "ORDER BY CNT DESC"
    )
    return sql, dict(binds or {})


GROUPABLE_DIMENSIONS = {
    "owner department": "OWNER_DEPARTMENT",
    "owner_department": "OWNER_DEPARTMENT",
    "owner dept": "OWNER_DEPARTMENT",
    "department_oul": "DEPARTMENT_OUL",
    "department oul": "DEPARTMENT_OUL",
    "entity": "ENTITY",
    "entity_no": "ENTITY_NO",
    "entity no": "ENTITY_NO",
    "status": "CONTRACT_STATUS",
    "contract_status": "CONTRACT_STATUS",
    "request_type": "REQUEST_TYPE",
    "request type": "REQUEST_TYPE",
}


# --- Case (15): missing CONTRACT_ID ---
def sql_missing_contract_id() -> str:
    return (
        'SELECT * FROM "Contract"\n'
        "WHERE CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID) = ''\n"
        "ORDER BY REQUEST_DATE DESC"
    )


def _as_date(obj: object) -> date:
    if isinstance(obj, datetime):
        return obj.date()
    if isinstance(obj, date):
        return obj
    return date.fromisoformat(str(obj)[:10])


def _ensure_date_binds(binds: Dict[str, object], *keys: str) -> None:
    for key in keys:
        if key in binds and binds[key] is not None:
            binds[key] = _as_date(binds[key])

def _overlap_pred(date_start_bind: str = ":date_start", date_end_bind: str = ":date_end") -> str:
    # Strict overlap: start <= end AND end >= start (both not null)
    return (
        "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
        f"AND START_DATE <= {date_end_bind} AND END_DATE >= {date_start_bind})"
    )

def build_contracts_sql(
    intent: Dict,
    *,
    table: str = "Contract",
    fts_columns: Optional[List[str]] = None
) -> Tuple[str, Dict[str, object]]:
    """
    Build Oracle SQL for the Contract table based on a normalized intent dict.
    Returns (sql, binds).
    Accuracy-first: attempt known high-value shortcuts before generic rules.
    Expected intent fields (subset):
      - explicit_dates: {start, end} or None
      - date_column: 'REQUEST_DATE' | 'END_DATE' | 'OVERLAP' | None
      - group_by: a column or None
      - agg: 'count' | 'sum' | 'avg' | None (for grouped measures)
      - measure_sql: SQL expr string for measure (defaults to NET)
      - sort_by, sort_desc, top_n
      - full_text_search: bool, fts_tokens: [str]
    """
    notes = intent.get("notes")
    if not isinstance(notes, dict):
        notes = {}
        intent["notes"] = notes
    else:
        intent["notes"] = notes
    q_text = str(
        notes.get("q")
        or intent.get("raw_question")
        or intent.get("question")
        or intent.get("q")
        or ""
    )

    sc_sql, sc_binds, _ = try_build_special_cases(q_text)
    if sc_sql:
        return sc_sql, (sc_binds or {})
    q_norm = str(intent.get("raw_question_norm") or q_text).strip().lower()
    wants_bottom = bool(_BOTTOM_RE.search(q_text))
    top_n_value = intent.get("top_n")
    if top_n_value is not None and not isinstance(top_n_value, int):
        try:
            top_n_value = int(top_n_value)
        except (TypeError, ValueError):
            top_n_value = None
    if top_n_value is None:
        match_top = re.search(r"\b(?:top|highest|bottom|lowest|least)\s+(\d+)\b", q_text, re.IGNORECASE)
        if match_top:
            try:
                top_n_value = int(match_top.group(1))
            except ValueError:
                top_n_value = None
    if top_n_value is not None and top_n_value <= 0:
        top_n_value = None

    # Special deterministic cases mapped by the parser or fallback keyword match.
    if "missing contract_id" in q_norm or "data quality" in q_norm:
        return sql_missing_contract_id(), {}

    if (
        "ytd" in q_norm
        and ("gross" in q_norm or "contract value" in q_norm)
        and ("top" in q_norm or "highest" in q_norm or wants_bottom or top_n_value)
    ):
        top_candidate = top_n_value if top_n_value is not None else 5
        today_hint = intent.get("today") or notes.get("today")
        binds_hint = {"today": today_hint} if today_hint else {}
        sql, binds_out = build_top_gross_ytd(q_text, binds_hint, top_candidate, ascending=wants_bottom)
        return sql, binds_out

    if re.search(r"\byear-?over-?year\b|\bYoY\b", q_text, re.IGNORECASE):
        today_obj = intent.get("today") or notes.get("today") or date.today()
        today_date = _as_date(today_obj)
        explicit = intent.get("explicit_dates") or {}
        ds = explicit.get("ds") or explicit.get("start") or intent.get("ds")
        de = explicit.get("de") or explicit.get("end") or intent.get("de")
        p_ds = explicit.get("p_ds") or explicit.get("previous_ds") or intent.get("p_ds")
        p_de = explicit.get("p_de") or explicit.get("previous_de") or intent.get("p_de")
        if not all([ds, de, p_ds, p_de]):
            this_year = today_date.year
            ds = ds or date(this_year, 1, 1)
            de = de or date(this_year, 3, 31)
            p_ds = p_ds or date(this_year - 1, 1, 1)
            p_de = p_de or date(this_year - 1, 3, 31)
        binds = {"ds": ds, "de": de, "p_ds": p_ds, "p_de": p_de}
        overlap = not _REQUEST_RE.search(q_text)
        if overlap:
            sql, out_binds = build_yoy_gross_overlap(binds)
            notes["yoy"] = "overlap"
        else:
            sql, out_binds = build_yoy_gross_requested(binds)
            notes["yoy"] = "request_date"
        return sql, out_binds

    if (
        re.search(
            r"\b(owner[_\s]?department)\b.*\b(vs|compare|comparison)\b.*\b(department[_\s]?oul)\b",
            q_text,
            re.IGNORECASE,
        )
        or re.search(
            r"\b(department[_\s]?oul)\b.*\b(vs|compare|comparison)\b.*\b(owner[_\s]?department)\b",
            q_text,
            re.IGNORECASE,
        )
        or re.search(r"\bOUL\b.*\blead\b", q_text, re.IGNORECASE)
        or re.search(
            r"\bowner[_\s]?department\b.*\bdepartment[_\s]?oul\b.*(compare|comparison|mismatch|lead)",
            q_text,
            re.IGNORECASE,
        )
    ):
        sql, binds_out = build_owner_vs_oul_mismatch()
        return sql, binds_out

    q_parts: List[str] = []
    binds: Dict[str, object] = {}
    select_list = "*"

    # WHERE parts
    where_parts: List[str] = []

    # 1) Time window / expiry semantics
    explicit = intent.get("explicit_dates")
    date_col = (intent.get("date_column") or "").upper() if intent.get("date_column") else None
    if explicit:
        binds["date_start"] = _as_date(explicit["start"])
        binds["date_end"] = _as_date(explicit["end"])
        if date_col == "REQUEST_DATE":
            where_parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "END_DATE":
            where_parts.append("END_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "START_DATE":
            where_parts.append("START_DATE BETWEEN :date_start AND :date_end")
        elif date_col == "OVERLAP" or date_col is None:
            where_parts.append(_overlap_pred())
        else:
            # Fallback: safe overlap
            where_parts.append(_overlap_pred())

    # 2) Full-text-like filtering over configured columns (simple LIKE ORs)
    if intent.get("full_text_search") and intent.get("fts_tokens") and fts_columns:
        like_terms = []
        k = 0
        for tok in intent["fts_tokens"]:
            k += 1
            kb = f"kw{k}"
            binds[kb] = f"%{tok}%"
            ors = [f"UPPER({col}) LIKE UPPER(:{kb})" for col in fts_columns]
            like_terms.append("(" + " OR ".join(ors) + ")")
        if like_terms:
            where_parts.append("(" + " AND ".join(like_terms) + ")")

    # 3) Direct column filter (e.g., CONTRACT_STATUS = 'EXPIRE')
    #    Expect intent["direct_filter"] like {"column":"CONTRACT_STATUS","op":"=","value":"expire"}
    df = intent.get("direct_filter")
    if df and df.get("column"):
        col = df["column"]
        op  = df.get("op", "=").upper()
        val = df.get("value")
        if val is not None:
            binds["df_val"] = val
            where_parts.append(f"UPPER({col}) {op} UPPER(:df_val)")

    # 3b) Extra column filters inferred from question text
    for extra in intent.get("extra_filters", []) or []:
        col = extra.get("col")
        op = (extra.get("op") or "").lower()
        bind_name = extra.get("bind")
        val = extra.get("value")
        if not col or not bind_name or val is None:
            continue
        binds[bind_name] = val
        if op == "like_ci":
            where_parts.append(f"UPPER({col}) LIKE UPPER(:{bind_name})")
        elif op == "eq_ci":
            where_parts.append(f"UPPER({col}) = UPPER(:{bind_name})")
        else:
            where_parts.append(f"{col} = :{bind_name}")

    # 4) SELECT list and GROUP BY / measure
    group_by = intent.get("group_by")
    group_by_token = intent.get("group_by_token")

    def _map_group(candidate: Optional[str]) -> Optional[str]:
        if not isinstance(candidate, str):
            return None
        key = candidate.strip().lower()
        if not key:
            return None
        mapped = GROUPABLE_DIMENSIONS.get(key)
        if mapped:
            return mapped
        return candidate.strip()

    mapped = _map_group(group_by_token)
    if mapped:
        group_by = mapped
    else:
        mapped = _map_group(group_by)
        if mapped:
            group_by = mapped
    agg = intent.get("agg")
    base_measure = intent.get("measure_sql")
    if not base_measure and "gross" in q_norm:
        base_measure = GROSS_EXPR
    measure_sql = base_measure or _NET

    order_by: Optional[str] = None
    sort_desc_flag = intent.get("sort_desc")
    if sort_desc_flag is None:
        desc = bool(top_n_value) and not wants_bottom
    else:
        desc = bool(sort_desc_flag)
    if wants_bottom:
        desc = False

    if group_by:
        # GROUPED output
        alias_measure = "MEASURE"
        if agg == "count":
            measure_expr = "COUNT(*)"
        elif agg == "avg":
            measure_expr = f"AVG({measure_sql})"
        elif agg == "sum" or agg is None:
            measure_expr = f"SUM({measure_sql})"
        else:
            measure_expr = f"SUM({measure_sql})"
        select_list = f"{group_by} AS GROUP_KEY, {measure_expr} AS {alias_measure}"
        order_by = alias_measure
    else:
        # ROW-LEVEL output (SELECT *)
        # Nothing special; ordering will be by sort_by if provided.
        order_by = intent.get("sort_by") or None

    # 5) Build SQL
    q_parts.append(f'SELECT {select_list} FROM "{table}"')
    if where_parts:
        q_parts.append("WHERE " + " AND ".join(where_parts))

    if order_by:
        q_parts.append(f"ORDER BY {order_by} {'DESC' if desc else 'ASC'}")

    # 6) Top-N
    if top_n_value:
        q_parts.append("FETCH FIRST :top_n ROWS ONLY")
        binds["top_n"] = int(top_n_value)

    sql = "\n".join(q_parts)
    return sql, binds
