# Contracts planner & deterministic SQL builder
# All comments/strings inside code are English-only per project guideline.
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# ---- Gross and helpers -------------------------------------------------------
GROSS_SQL = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
    "ELSE NVL(VAT,0) END"
)

STATUS_NORMALIZE = {
    # left: normalized upper-case form
    "EXPIRE": ["EXPIRE", "EXPIRED", "ENDED", "FINISHED"],
    "ACTIVE": ["ACTIVE", "RUNNING"],
    "PENDING": ["PENDING", "WAITING"],
}

def _normalize_status(user_text: str) -> str | None:
    u = (user_text or "").strip().upper()
    for key, variants in STATUS_NORMALIZE.items():
        if u in variants:
            return key.capitalize()
    # If user passed an exact value, return as-is (capitalize first)
    if u:
        return u.capitalize()
    return None


def _month_bounds_last_n(n: int, today: date) -> tuple[date, date]:
    """Calendar window covering last n months inclusive to end of last full day."""
    # Example: last 1 month => first day of previous month .. last day of previous month
    end = (today.replace(day=1) - relativedelta(days=1))  # last day of previous month
    start = (end.replace(day=1) - relativedelta(months=(n - 1)))
    return start, end


def _last_month(today: date) -> tuple[date, date]:
    return _month_bounds_last_n(1, today)


def _last_quarter(today: date) -> tuple[date, date]:
    q = (today.month - 1) // 3 + 1
    last_q = q - 1 if q > 1 else 4
    last_q_year = today.year if q > 1 else today.year - 1
    first_month = 3 * last_q - 2
    start = date(last_q_year, first_month, 1)
    end = (start + relativedelta(months=3) - relativedelta(days=1))
    return start, end


def _next_n_days(n: int, today: date) -> tuple[date, date]:
    return today, today + timedelta(days=n)


def _to_date(val: date | datetime | str) -> date:
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    # val is str (we keep ISO like '2025-08-31' safe)
    return datetime.fromisoformat(val).date()


# ---- Intent ------------------------------------------------------------------
@dataclass
class Intent:
    question: str
    top_n: int | None = None
    order_by: str | None = None
    order_desc: bool = True
    group_by: str | None = None
    agg: str | None = None               # 'sum' | 'count' | 'avg' | None
    gross: bool = False
    window_kind: str | None = None       # 'REQUEST', 'END_ONLY', 'OVERLAP'
    window_start: date | None = None
    window_end: date | None = None
    explicit_columns: list[str] | None = None
    where_clauses: list[str] | None = None
    where_binds: dict | None = None
    explain_parts: list[str] | None = None


def parse_intent(q: str, today: date | None = None) -> Intent:
    """Heuristic deterministic intent parser for DW Contract questions."""
    t = today or date.today()
    qn = (q or "").strip()
    qi = qn.lower()

    intent = Intent(question=qn, top_n=None, order_by=None, order_desc=True,
                    group_by=None, agg=None, gross=False,
                    window_kind=None, window_start=None, window_end=None,
                    explicit_columns=None, where_clauses=[], where_binds={},
                    explain_parts=[])

    # ---- Specific direct where on explicit column (e.g., CONTRACT_STATUS) ----
    m_status = re.search(r"contract_status\s*=\s*([A-Za-z\-]+)", qi)
    if m_status:
        val = _normalize_status(m_status.group(1))
        if val:
            intent.where_clauses.append("UPPER(CONTRACT_STATUS) = UPPER(:status_val)")
            intent.where_binds["status_val"] = val
            intent.explain_parts.append("Filtering on CONTRACT_STATUS as explicitly requested.")
        # Listing all rows by latest request date
        intent.window_kind = None
        intent.order_by = "REQUEST_DATE"
        intent.order_desc = True
        return intent

    # ---- Expiring in X days (count) ------------------------------------------
    m_exp_cnt = re.search(r"expiring\s+in\s+(\d+)\s+days.*\(count\)", qi)
    if m_exp_cnt:
        days = int(m_exp_cnt.group(1))
        ds, de = _next_n_days(days, t)
        intent.agg = "count"
        intent.window_kind = "END_ONLY"
        intent.window_start, intent.window_end = ds, de
        intent.explain_parts.append(f"Counting contracts with END_DATE in next {days} days.")
        return intent

    # ---- END_DATE in next N days ---------------------------------------------
    m_end_next = re.search(r"end_date\s+in\s+the\s+next\s+(\d+)\s+days", qi)
    if m_end_next:
        days = int(m_end_next.group(1))
        ds, de = _next_n_days(days, t)
        intent.window_kind = "END_ONLY"
        intent.window_start, intent.window_end = ds, de
        intent.explain_parts.append(f"Filtering by END_DATE between {ds} and {de}.")
        return intent

    # ---- requested last month ------------------------------------------------
    if "requested last month" in qi or ("requested" in qi and "last month" in qi):
        ds, de = _last_month(t)
        intent.window_kind = "REQUEST"
        intent.window_start, intent.window_end = ds, de
        intent.explicit_columns = ["CONTRACT_ID", "CONTRACT_OWNER", "REQUEST_DATE"]
        intent.order_by = "REQUEST_DATE"
        intent.order_desc = True
        intent.explain_parts.append("Using REQUEST_DATE window for 'requested last month'.")
        return intent

    # ---- top N by gross / net last X months/month ----------------------------
    m_top_gross = re.search(r"top\s+(\d+)\s+.*gross.*last\s+(\d+)\s+months", qi)
    if m_top_gross:
        n = int(m_top_gross.group(1)); months = int(m_top_gross.group(2))
        ds = t - relativedelta(months=months)
        de = t
        intent.top_n = n
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = GROSS_SQL
        intent.order_desc = True
        intent.explain_parts.append(f"Top {n} by GROSS over last {months} months, using OVERLAP window.")
        return intent

    m_top_gross_lm = re.search(r"top\s+(\d+)\s+.*gross.*last\s+month", qi)
    if m_top_gross_lm:
        n = int(m_top_gross_lm.group(1))
        ds, de = _last_month(t)
        intent.top_n = n
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = GROSS_SQL
        intent.order_desc = True
        intent.explain_parts.append(f"Top {n} by GROSS in last month, using OVERLAP window.")
        return intent

    m_top_net_lm = re.search(r"top\s+(\d+)\s+contracts.*contract value.*last\s+month", qi)
    if m_top_net_lm:
        n = int(m_top_net_lm.group(1))
        ds, de = _last_month(t)
        intent.top_n = n
        intent.gross = False
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        intent.order_desc = True
        intent.explain_parts.append(f"Top {n} by NET in last month, using OVERLAP window.")
        return intent

    m_top_net_lmN = re.search(r"top\s+(\d+)\s+contracts.*contract value.*last\s+(\d+)\s+months", qi)
    if m_top_net_lmN:
        n = int(m_top_net_lmN.group(1)); months = int(m_top_net_lmN.group(2))
        ds = t - relativedelta(months=months); de = t
        intent.top_n = n
        intent.gross = False
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        intent.order_desc = True
        intent.explain_parts.append(f"Top {n} by NET over last {months} months, using OVERLAP window.")
        return intent

    # ---- totals per owner department last quarter ----------------------------
    if "total gross value of contracts per owner department last quarter" in qi:
        ds, de = _last_quarter(t)
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "sum"
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append("Gross SUM per OWNER_DEPARTMENT over last quarter (OVERLAP).")
        return intent

    # total gross per owner department (no window)
    if "total gross value of contracts per owner department" in qi:
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "sum"
        intent.gross = True
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append("Gross SUM per OWNER_DEPARTMENT (all time).")
        return intent

    # count by status all time
    if "count of contracts by status" in qi:
        intent.group_by = "CONTRACT_STATUS"
        intent.agg = "count"
        intent.order_by = "CNT"
        intent.order_desc = True
        intent.explain_parts.append("COUNT(*) per CONTRACT_STATUS (all time).")
        return intent

    # VAT zero but net > 0
    if "vat is null or zero" in qi and "value" in qi:
        intent.where_clauses.append("NVL(VAT,0) = 0 AND NVL(CONTRACT_VALUE_NET_OF_VAT,0) > 0")
        intent.order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        intent.order_desc = True
        intent.explain_parts.append("VAT=0 and NET>0 filter applied.")
        return intent

    # REQUEST TYPE = Renewal in 2023
    m_reqtype_year = re.search(r"request\s*type\s*=\s*([A-Za-z]+).*in\s*(\d{4})", qi)
    if m_reqtype_year:
        rtype = m_reqtype_year.group(1)
        yr = int(m_reqtype_year.group(2))
        ds, de = date(yr,1,1), date(yr,12,31)
        intent.where_clauses.append("UPPER(REQUEST_TYPE)=UPPER(:req_type)")
        intent.where_binds["req_type"] = rtype
        intent.window_kind = "REQUEST"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "REQUEST_DATE"
        intent.order_desc = True
        intent.explain_parts.append(f"Filter REQUEST_TYPE='{rtype}' and REQUEST_DATE in {yr}.")
        return intent

    # distinct ENTITY with counts
    if "distinct entity values" in qi and "counts" in qi:
        intent.group_by = "ENTITY"
        intent.agg = "count"
        intent.order_by = "CNT"
        intent.order_desc = True
        intent.explain_parts.append("COUNT(*) per ENTITY (all time).")
        return intent

    # list owner departments (use counts as default)
    if "list contracts owneres department" in qi or "list contracts owners department" in qi:
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "count"
        intent.order_by = "CNT"
        intent.order_desc = True
        intent.explain_parts.append("COUNT(*) per OWNER_DEPARTMENT (all time).")
        return intent

    # END_DATE in next 90 days (phrase without explicit "END_DATE" is handled above)
    if "contracts with end_date in the next 90 days" in qi:
        ds, de = _next_n_days(90, t)
        intent.window_kind = "END_ONLY"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "END_DATE"
        intent.order_desc = False
        intent.explain_parts.append("END_DATE between today and +90 days.")
        return intent

    # Fallback: list all ordered by REQUEST_DATE desc
    intent.order_by = "REQUEST_DATE"
    intent.order_desc = True
    intent.explain_parts.append("Fallback listing ordered by REQUEST_DATE DESC.")
    return intent


# ---- Build SQL ---------------------------------------------------------------
def build_sql(intent: Intent) -> tuple[str, dict, dict]:
    binds = {}
    parts = []
    explain = []

    # Window WHERE clause
    if intent.window_kind == "REQUEST":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
            explain.append(f"Window = REQUEST_DATE between {binds['date_start']} and {binds['date_end']}.")
    elif intent.window_kind == "END_ONLY":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("END_DATE BETWEEN :date_start AND :date_end")
            explain.append(f"Window = END_DATE between {binds['date_start']} and {binds['date_end']}.")
    elif intent.window_kind == "OVERLAP":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
                         "AND START_DATE <= :date_end AND END_DATE >= :date_start)")
            explain.append("Window = OVERLAP on START_DATE/END_DATE.")

    # Explicit where
    for wc in (intent.where_clauses or []):
        parts.append(wc)
    for k,v in (intent.where_binds or {}).items():
        binds[k] = v

    where_sql = ""
    if parts:
        where_sql = " WHERE " + " AND ".join(parts)

    # Projection and aggregation
    gross_expr = GROSS_SQL
    if intent.group_by and intent.agg:
        if intent.agg == "sum":
            measure = gross_expr if intent.gross else "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
            sql = (
                f"SELECT\n  {intent.group_by} AS GROUP_KEY,\n"
                f"  SUM({measure}) AS MEASURE\n"
                f'FROM "Contract"{where_sql}\n'
                f"GROUP BY {intent.group_by}"
            )
            order_col = intent.order_by or "MEASURE"
        elif intent.agg == "count":
            sql = (
                f"SELECT\n  {intent.group_by} AS GROUP_KEY,\n"
                f"  COUNT(*) AS CNT\n"
                f'FROM "Contract"{where_sql}\n'
                f"GROUP BY {intent.group_by}"
            )
            order_col = intent.order_by or "CNT"
        else:
            # default: count
            sql = (
                f"SELECT\n  {intent.group_by} AS GROUP_KEY,\n"
                f"  COUNT(*) AS CNT\n"
                f'FROM "Contract"{where_sql}\n'
                f"GROUP BY {intent.group_by}"
            )
            order_col = "CNT"
    elif intent.agg == "count" and not intent.group_by:
        sql = f'SELECT COUNT(*) AS CNT FROM "Contract"{where_sql}'
        order_col = None
    else:
        # Non-aggregated listing
        if intent.explicit_columns:
            cols = ", ".join(intent.explicit_columns)
        else:
            cols = "*"
        sql = f'SELECT {cols} FROM "Contract"{where_sql}'
        order_col = intent.order_by

    # ORDER BY
    if order_col:
        direction = "DESC" if intent.order_desc else "ASC"
        sql += f"\nORDER BY {order_col} {direction}"

    # LIMIT
    if intent.top_n:
        binds["top_n"] = intent.top_n
        sql += "\nFETCH FIRST :top_n ROWS ONLY"

    meta = {
        "explain": "; ".join(intent.explain_parts or []),
        "window_kind": intent.window_kind,
        "group_by": intent.group_by,
        "agg": intent.agg,
        "gross": intent.gross,
    }
    return sql, binds, meta


def plan_sql(question: str, today: date | None = None) -> tuple[str, dict, dict]:
    it = parse_intent(question, today=today)
    return build_sql(it)
