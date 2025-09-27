"""
Contract-specific deterministic intent parsing and SQL builder.
All code/comments/messages are English-only by project policy.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List
import re
import datetime as dt
from dateutil.relativedelta import relativedelta

# -----------------------------
# Dimension synonyms (lowercased)
# -----------------------------
DIMENSION_SYNONYMS = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",   # default "department" -> owner department
    "department_oul": "DEPARTMENT_OUL",
    "oul": "DEPARTMENT_OUL",
    "manager": "DEPARTMENT_OUL",
    "entity": "ENTITY_NO",              # "entity" rolls up to ENTITY_NO per your note
    "entity_no": "ENTITY_NO",
    "entity number": "ENTITY_NO",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "stakeholders": "CONTRACT_STAKEHOLDER_1",
}

# Gross vs net
def expr_gross() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
        "ELSE NVL(VAT,0) END"
    )

def expr_net() -> str:
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

@dataclass
class NLIntent:
    table: str = "Contract"
    # time / window
    date_mode: str = "OVERLAP"   # OVERLAP | REQUEST | END_ONLY | START_ONLY
    date_start: Optional[dt.date] = None
    date_end: Optional[dt.date] = None
    # selection
    select_columns: Optional[List[str]] = None   # None => SELECT * unless aggregated
    # measure / group / agg
    measure_sql: str = field(default_factory=expr_net)
    agg: Optional[str] = None                    # sum | count | avg | None
    group_by: Optional[str] = None               # OWNER_DEPARTMENT | DEPARTMENT_OUL | ENTITY_NO | ...
    # sorting / limiting
    order_by: Optional[str] = None
    order_desc: bool = True
    top_n: Optional[int] = None
    user_requested_top_n: bool = False
    # filters
    where_clauses: List[str] = field(default_factory=list)
    binds: Dict[str, Any] = field(default_factory=dict)
    # fts
    full_text_search: bool = False
    fts_tokens: List[str] = field(default_factory=list)
    # notes
    notes: Dict[str, Any] = field(default_factory=dict)

# -----------------------------
# Regex helpers
# -----------------------------
RE_TOP = re.compile(r"\btop\s+(\d+)\b", re.I)
RE_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
RE_LAST_MONTH = re.compile(r"\blast\s+month\b", re.I)
RE_LAST_N_DAYS = re.compile(r"\blast\s+(\d+)\s+days?\b", re.I)
RE_NEXT_N_DAYS = re.compile(r"\bnext\s+(\d+)\s+days?\b", re.I)
RE_COUNT = re.compile(r"\bcount\b|\(count\)", re.I)
RE_AVG = re.compile(r"\bavg(?:erage)?\b", re.I)
RE_GROSS = re.compile(r"\bgross\b", re.I)
RE_REQUESTED = re.compile(r"\brequested?\b", re.I)
RE_EXPIRE = re.compile(r"\bexpir(?:e|ing)\b", re.I)
RE_REQTYPE_EQ = re.compile(r"request\s*type\s*=\s*([A-Za-z0-9 _-]+)", re.I)
RE_YEAR = re.compile(r"\b(20\d{2})\b")
RE_DISTINCT_ENTITY = re.compile(r"\bdistinct\s+entity\b|\bentity values\b", re.I)
RE_MISSING_CONTRACT_ID = re.compile(r"\bmissing\s+contract_id\b|\bwithout\s+contract_id\b", re.I)
RE_MONTHLY_TREND = re.compile(r"\bmonthly\s+trend\b", re.I)
RE_LIST_OWNER_DEPT = re.compile(r"\blist\s+contracts?\s+owner(?:s)?\s+department\b", re.I)

def _first_day_of_last_month(today: dt.date) -> dt.date:
    first_this = today.replace(day=1)
    return first_this - relativedelta(months=1)

def _last_day_of_last_month(today: dt.date) -> dt.date:
    first_this = today.replace(day=1)
    last_prev = first_this - dt.timedelta(days=1)
    return last_prev

def _rolling_months(today: dt.date, n: int) -> Tuple[dt.date, dt.date]:
    return (today - relativedelta(months=n), today)

def _last_n_days(today: dt.date, n: int) -> Tuple[dt.date, dt.date]:
    return (today - dt.timedelta(days=n), today)

def _next_n_days(today: dt.date, n: int) -> Tuple[dt.date, dt.date]:
    return (today, today + dt.timedelta(days=n))

def _year_window(year: int) -> Tuple[dt.date, dt.date]:
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    return (start, end)

def parse_intent_contract(question: str, *, today: Optional[dt.date]=None, full_text_search: bool=False) -> NLIntent:
    """
    Deterministic parser for Contract questions.
    """
    q = (question or "").strip()
    intent = NLIntent()
    intent.full_text_search = full_text_search
    if today is None:
        today = dt.date.today()

    # top N
    m = RE_TOP.search(q)
    if m:
        intent.top_n = int(m.group(1))
        intent.user_requested_top_n = True

    # gross / net
    if RE_GROSS.search(q):
        intent.measure_sql = expr_gross()
    else:
        intent.measure_sql = expr_net()

    # count / average
    if RE_COUNT.search(q):
        intent.agg = "count"
    if RE_AVG.search(q):
        intent.agg = "avg"

    # grouping ("by" or "per")
    lower_q = q.lower()
    if " by " in lower_q or " per " in lower_q:
        for k, col in DIMENSION_SYNONYMS.items():
            if f" by {k} " in lower_q or f" per {k} " in lower_q:
                intent.group_by = col
                break

    # explicit patterns (distinct entity)
    if RE_DISTINCT_ENTITY.search(q):
        intent.group_by = "ENTITY"
        intent.agg = "count"  # count(*) per entity

    if RE_MISSING_CONTRACT_ID.search(q):
        intent.where_clauses.append("CONTRACT_ID IS NULL")

    # list owner departments (distinct)
    if RE_LIST_OWNER_DEPT.search(q):
        intent.select_columns = ["DISTINCT OWNER_DEPARTMENT"]
        intent.order_by = "OWNER_DEPARTMENT"
        intent.order_desc = False
        return intent

    # requested => REQUEST_DATE
    if RE_REQUESTED.search(q):
        intent.date_mode = "REQUEST"

    # expiring => END_ONLY + "in N days" (or next N days); default 30 if not provided
    if RE_EXPIRE.search(q):
        intent.date_mode = "END_ONLY"
        m_next = RE_NEXT_N_DAYS.search(q)
        m_last = RE_LAST_N_DAYS.search(q)
        n = 30
        if m_next:
            n = int(m_next.group(1))
            intent.date_start, intent.date_end = _next_n_days(today, n)
        elif m_last:
            # "expiring in last N days" is odd; treat as next N
            n = int(m_last.group(1))
            intent.date_start, intent.date_end = _next_n_days(today, n)
        else:
            intent.date_start, intent.date_end = _next_n_days(today, 30)

    # last month
    if RE_LAST_MONTH.search(q) and intent.date_start is None:
        intent.date_start = _first_day_of_last_month(today)
        intent.date_end = _last_day_of_last_month(today)

    # last N months (rolling)
    m = RE_LAST_N_MONTHS.search(q)
    if m and intent.date_start is None:
        n = int(m.group(1))
        intent.date_start, intent.date_end = _rolling_months(today, n)

    # year window ("in 2023")
    m = RE_YEAR.search(q)
    if m and "requested" in lower_q:
        y = int(m.group(1))
        intent.date_mode = "REQUEST"
        intent.date_start, intent.date_end = _year_window(y)

    # REQUEST TYPE = X
    m = RE_REQTYPE_EQ.search(q)
    if m:
        req_type = m.group(1).strip()
        intent.where_clauses.append("REQUEST_TYPE = :request_type")
        intent.binds["request_type"] = req_type

    # VAT null/zero and contract value > 0
    if "vat" in lower_q and ("null" in lower_q or "zero" in lower_q) and ("> 0" in lower_q or "value > 0" in lower_q):
        intent.where_clauses.append("NVL(VAT,0) = 0")
        intent.where_clauses.append("NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0")

    # monthly trend
    if RE_MONTHLY_TREND.search(q):
        intent.agg = "count"
        intent.group_by = "TRUNC(REQUEST_DATE,'MM')"
        intent.date_mode = "REQUEST"
        # default last 12 months rolling
        if intent.date_start is None:
            intent.date_start, intent.date_end = _rolling_months(today, 12)
        intent.order_by = "TRUNC(REQUEST_DATE,'MM')"
        intent.order_desc = False

    # default order: by measure when not aggregated, otherwise by measure desc
    if intent.order_by is None:
        if intent.group_by:
            intent.order_by = "MEASURE"
            intent.order_desc = True
        else:
            intent.order_by = intent.measure_sql
            intent.order_desc = True

    # Projection: if user specified columns in question "(...)" pattern
    # Example: "(contract id, owner, request date)" -> normalize to column names
    if "(" in q and ")" in q and "requested" in lower_q:
        # very light parser: split inside ()
        inside = q[q.find("(")+1:q.find(")")]
        cols = [c.strip().upper().replace(" ", "_") for c in inside.split(",") if c.strip()]
        # map friendly names
        mapping = {
            "CONTRACT_ID": "CONTRACT_ID",
            "OWNER": "CONTRACT_OWNER",
            "REQUEST_DATE": "REQUEST_DATE",
            "START_DATE": "START_DATE",
            "END_DATE": "END_DATE",
        }
        intent.select_columns = [mapping.get(c, c) for c in cols]

    return intent

def _ensure_dates(intent: NLIntent, today: Optional[dt.date]=None) -> None:
    if today is None:
        today = dt.date.today()
    # Provide defaults if missing
    if intent.date_start is None or intent.date_end is None:
        if intent.date_mode == "REQUEST":
            # default last full month if nothing explicit
            intent.date_start = _first_day_of_last_month(today)
            intent.date_end = _last_day_of_last_month(today)
        elif intent.date_mode == "END_ONLY":
            intent.date_start, intent.date_end = _next_n_days(today, 30)
        else:
            # OVERLAP default: last month
            intent.date_start = _first_day_of_last_month(today)
            intent.date_end = _last_day_of_last_month(today)

def build_sql_contract(intent: NLIntent, *, select_all_default: bool=True) -> Tuple[str, Dict[str, Any]]:
    """
    Build Oracle SQL and binds from intent. Date binds are datetime.date.
    """
    _ensure_dates(intent)
    binds = dict(intent.binds)
    binds["date_start"] = intent.date_start
    binds["date_end"] = intent.date_end
    if intent.top_n is not None:
        binds["top_n"] = int(intent.top_n)

    table_name = intent.table or "Contract"
    if table_name.startswith('"') or "." in table_name:
        table = table_name
    else:
        table = f'"{table_name}"'
    where = []

    # date filters
    if intent.date_mode == "REQUEST":
        where.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
    elif intent.date_mode == "END_ONLY":
        where.append("END_DATE BETWEEN :date_start AND :date_end")
    elif intent.date_mode == "START_ONLY":
        where.append("START_DATE BETWEEN :date_start AND :date_end")
    else:  # OVERLAP
        where.append("(START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= :date_end AND END_DATE >= :date_start)")

    # other filters
    where.extend(intent.where_clauses)
    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    # SELECT clause
    select_sql: str
    order_sql: str = ""
    fetch_sql: str = ""

    if intent.group_by and intent.agg:
        # aggregated with grouping
        gb = intent.group_by
        if intent.agg == "count":
            measure = "COUNT(*)"
            measure_alias = "CNT"
        elif intent.agg == "avg":
            measure = f"AVG({intent.measure_sql})"
            measure_alias = "MEASURE"
        else:
            measure = f"SUM({intent.measure_sql})"
            measure_alias = "MEASURE"
        select_sql = f"SELECT {gb} AS GROUP_KEY, {measure} AS {measure_alias}"
        group_sql = f" GROUP BY {gb}"
        order_expr = intent.order_by or measure_alias
        if intent.order_by == "MEASURE" and measure_alias != "MEASURE":
            order_expr = measure_alias
        order_dir = "DESC" if intent.order_desc else "ASC"
        order_sql = f" ORDER BY {order_expr} {order_dir}"
        if intent.top_n:
            fetch_sql = " FETCH FIRST :top_n ROWS ONLY"
        sql = f"{select_sql} FROM {table} {where_sql}{group_sql}{order_sql}{fetch_sql}"
        return sql, binds

    if intent.agg:
        if intent.agg == "count":
            measure = "COUNT(*)"
            measure_alias = "CNT"
        elif intent.agg == "avg":
            measure = f"AVG({intent.measure_sql})"
            measure_alias = "MEASURE"
        else:
            measure = f"SUM({intent.measure_sql})"
            measure_alias = "MEASURE"
        select_sql = f"SELECT {measure} AS {measure_alias}"
        order_sql = ""
        order_expr = intent.order_by
        if order_expr == "MEASURE" and measure_alias != "MEASURE":
            order_expr = measure_alias
        if order_expr:
            order_dir = "DESC" if intent.order_desc else "ASC"
            order_sql = f" ORDER BY {order_expr} {order_dir}"
        if intent.top_n:
            fetch_sql = " FETCH FIRST :top_n ROWS ONLY"
        sql = f"{select_sql} FROM {table} {where_sql}{order_sql}{fetch_sql}"
        return sql, binds

    if intent.select_columns:
        select_sql = "SELECT " + ", ".join(intent.select_columns)
    else:
        select_sql = "SELECT *" if select_all_default else "SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE"

    order_expr = intent.order_by or intent.measure_sql
    order_dir = "DESC" if intent.order_desc else "ASC"
    order_sql = f" ORDER BY {order_expr} {order_dir}"
    if intent.top_n:
        fetch_sql = " FETCH FIRST :top_n ROWS ONLY"
    sql = f"{select_sql} FROM {table} {where_sql}{order_sql}{fetch_sql}"
    return sql, binds

def explain_interpretation(intent: NLIntent) -> str:
    """
    Short human-readable explanation (English in code).
    """
    parts = []
    if intent.date_mode == "REQUEST":
        parts.append(f"Window on REQUEST_DATE: {intent.date_start} .. {intent.date_end}.")
    elif intent.date_mode == "END_ONLY":
        parts.append(f"Window on END_DATE: {intent.date_start} .. {intent.date_end}.")
    elif intent.date_mode == "START_ONLY":
        parts.append(f"Window on START_DATE: {intent.date_start} .. {intent.date_end}.")
    else:
        parts.append(f"Active-overlap window: START_DATE <= {intent.date_end} AND END_DATE >= {intent.date_start}.")
    if intent.group_by:
        parts.append(f"Grouping by {intent.group_by}.")
    if intent.agg:
        parts.append(f"Aggregation: {intent.agg.upper()} over {'GROSS' if intent.measure_sql!=expr_net() else 'NET'} value.")
    if intent.top_n:
        parts.append(f"Top {intent.top_n} rows requested.")
    if intent.full_text_search and intent.fts_tokens:
        parts.append(f"Full-text search on tokens: {', '.join(intent.fts_tokens)}.")
    return " ".join(parts)
