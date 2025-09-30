# Contracts planner & deterministic SQL builder
# All comments/strings inside code are English-only per project guideline.
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

from dateutil.relativedelta import relativedelta

_YTD_YEAR_RE = re.compile(
    r"(?:\b(?:ytd|year\s*to\s*date)\s*(\d{4})\b)|(?:\b(\d{4})\s*(?:ytd|year\s*to\s*date)\b)",
    re.IGNORECASE,
)

_LOWEST_RE = re.compile(r"\b(lowest|bottom|least|smallest|cheapest|min)\b", re.IGNORECASE)

# --- Request Type parsing & synonyms -----------------------------------------

REQTYPE_EQ_RE = re.compile(
    r"\bREQUEST\s*TYPE\s*=\s*([\"']?)([^\"';,]+)\1",
    re.IGNORECASE,
)

REQTYPE_COLON_OR_SPACE_RE = re.compile(
    r"\bREQUEST\s*TYPE\b\s*[: ]\s*([\"']?)([^\"';,]+)\1",
    re.IGNORECASE,
)


def _norm(value: str) -> str:
    return (value or "").strip().lower()


def _load_enum_synonyms(settings: Dict) -> Dict:
    """Return the DW_ENUM_SYNONYMS map from settings (if present)."""

    return (settings or {}).get("DW_ENUM_SYNONYMS", {})


def _syn_bucket_for(enum_map: Dict, field: str, value: str) -> Optional[Dict]:
    """Return the synonym bucket for the given field/value if available."""

    field_map = enum_map.get(field, {})
    val_key = _norm(value)
    if val_key in field_map:
        return field_map[val_key]
    for key in field_map.keys():
        if _norm(key) == val_key:
            return field_map[key]
    return None


def _build_reqtype_condition(value: str, bucket: Optional[Dict]) -> Tuple[str, Dict[str, str]]:
    """Build a SQL predicate for REQUEST_TYPE from synonyms."""

    parts: list[str] = []
    binds: Dict[str, str] = {}
    idx = 0

    equals_list = []
    prefix_list = []
    contains_list = []

    if bucket:
        equals_list = bucket.get("equals") or []
        prefix_list = bucket.get("prefix") or []
        contains_list = bucket.get("contains") or []

    if not (equals_list or prefix_list or contains_list):
        equals_list = [value]

    eq_terms: list[str] = []
    for candidate in equals_list:
        key = f"rt_eq_{idx}"
        idx += 1
        binds[key] = candidate
        eq_terms.append(f"UPPER(TRIM(REQUEST_TYPE)) = UPPER(:{key})")
    if eq_terms:
        parts.append("(" + " OR ".join(eq_terms) + ")")

    for candidate in prefix_list:
        key = f"rt_pre_{idx}"
        idx += 1
        binds[key] = candidate + "%"
        parts.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")

    for candidate in contains_list:
        key = f"rt_cont_{idx}"
        idx += 1
        binds[key] = "%" + candidate + "%"
        parts.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")

    if not bucket:
        key = f"rt_like_{idx}"
        idx += 1
        binds[key] = f"%{_norm(value)}%"
        parts.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")

    where_sql = "(" + " OR ".join(parts) + ")" if parts else "1=1"
    return where_sql, binds


def extract_request_type_filter(
    question: str, settings: Dict
) -> Optional[Tuple[str, Dict[str, str], str]]:
    """Detect REQUEST TYPE filters in the question and build SQL using synonyms."""

    text = question or ""
    match = REQTYPE_EQ_RE.search(text)
    if not match:
        match = REQTYPE_COLON_OR_SPACE_RE.search(text)

    if not match:
        return None

    raw_value = (match.group(2) or "").strip()
    enum_map = _load_enum_synonyms(settings)
    bucket = _syn_bucket_for(enum_map, "Contract.REQUEST_TYPE", raw_value)

    where_sql, binds = _build_reqtype_condition(raw_value, bucket)
    explain = f"Filtering on REQUEST_TYPE ~= '{raw_value}' using synonyms (equals/prefix/contains)."
    return where_sql, binds, explain

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

STAKEHOLDER_SLOTS = 8


def _gross_from_alias(net_alias: str = "NET", vat_alias: str = "VAT") -> str:
    return (
        f"{net_alias} + CASE WHEN {vat_alias} BETWEEN 0 AND 1 "
        f"THEN {net_alias} * {vat_alias} ELSE {vat_alias} END"
    )


def _overlap_condition(
    start_bind: str = ":date_start", end_bind: str = ":date_end", prefix: str = ""
) -> str:
    return (
        f"({prefix}START_DATE IS NOT NULL AND {prefix}END_DATE IS NOT NULL "
        f"AND {prefix}START_DATE <= {end_bind} AND {prefix}END_DATE >= {start_bind})"
    )


def _build_meta(intent: Intent, **overrides: object) -> dict:
    meta = {
        "explain": "; ".join(intent.explain_parts or []),
        "window_kind": intent.window_kind,
        "group_by": intent.group_by,
        "agg": intent.agg,
        "gross": intent.gross,
    }
    meta.update(overrides)
    return meta

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
    special: str | None = None
    special_params: dict | None = None


def parse_intent(q: str, today: date | None = None) -> Intent:
    """Heuristic deterministic intent parser for DW Contract questions."""
    t = today or date.today()
    qn = (q or "").strip()
    qi = qn.lower()

    lowest_hint = bool(_LOWEST_RE.search(qn))

    intent = Intent(question=qn, top_n=None, order_by=None, order_desc=True,
                    group_by=None, agg=None, gross=False,
                    window_kind=None, window_start=None, window_end=None,
                    explicit_columns=None, where_clauses=[], where_binds={},
                    explain_parts=[])

    if lowest_hint:
        intent.order_desc = False

    def _note_ordering(desc: bool) -> None:
        intent.explain_parts.append(
            "Ordering: descending (top/highest)." if desc else "Ordering: ascending (lowest/bottom)."
        )

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
        _note_ordering(True)
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
        _note_ordering(True)
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
        _note_ordering(True)
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
        _note_ordering(True)
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
        _note_ordering(True)
        return intent

    # ---- bottom N by net last month -----------------------------------------
    m_bottom_net_lm = re.search(r"bottom\s+(\d+)\s+contracts.*contract value.*last\s+month", qi)
    if m_bottom_net_lm:
        n = int(m_bottom_net_lm.group(1))
        ds, de = _last_month(t)
        intent.top_n = n
        intent.gross = False
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        intent.order_desc = False
        intent.explain_parts.append(f"Bottom {n} by NET in last month, using OVERLAP window.")
        _note_ordering(False)
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
        _note_ordering(True)
        return intent

    # total gross per owner department (no window)
    if "total gross value of contracts per owner department" in qi:
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "sum"
        intent.gross = True
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append("Gross SUM per OWNER_DEPARTMENT (all time).")
        _note_ordering(True)
        return intent

    # lowest owner department by gross last quarter
    if "lowest owner department" in qi and "gross" in qi and "last quarter" in qi:
        ds, de = _last_quarter(t)
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "sum"
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "MEASURE"
        intent.order_desc = False
        intent.explain_parts.append("Gross SUM per OWNER_DEPARTMENT over last quarter (OVERLAP).")
        _note_ordering(False)
        return intent

    # total gross per DEPARTMENT_OUL last quarter
    if "total gross value per department_oul last quarter" in qi or "total gross value per department oul last quarter" in qi:
        ds, de = _last_quarter(t)
        intent.group_by = "DEPARTMENT_OUL"
        intent.agg = "sum"
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append("Gross SUM per DEPARTMENT_OUL over last quarter (OVERLAP).")
        _note_ordering(True)
        return intent

    # count by status all time
    if "count of contracts by status" in qi:
        intent.group_by = "CONTRACT_STATUS"
        intent.agg = "count"
        intent.order_by = "CNT"
        intent.order_desc = True
        intent.explain_parts.append("COUNT(*) per CONTRACT_STATUS (all time).")
        _note_ordering(True)
        return intent

    if "count of contracts per entity_no" in qi or "count of contracts per entity no" in qi:
        intent.group_by = "ENTITY_NO"
        intent.agg = "count"
        intent.order_by = "CNT"
        intent.order_desc = True
        intent.explain_parts.append("COUNT(*) per ENTITY_NO (all time).")
        _note_ordering(True)
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
        _note_ordering(False)
        return intent

    # ---- Missing CONTRACT_ID -------------------------------------------------
    if "missing contract_id" in qi:
        intent.special = "missing_contract_id"
        intent.explain_parts.append("Rows with missing/blank CONTRACT_ID.")
        return intent

    # ---- Gross by stakeholder slots over last N days ------------------------
    if "gross" in qi and "stakeholder" in qi and "last" in qi and "days" in qi:
        m_days = re.search(r"last\s+(\d+)\s+days", qi)
        ndays = int(m_days.group(1)) if m_days else 90
        date_end = t
        date_start = t - timedelta(days=ndays)
        intent.special = "gross_by_stakeholder_slots_last_ndays"
        intent.special_params = {
            "ndays": ndays,
            "date_start": date_start,
            "date_end": date_end,
            "slots": STAKEHOLDER_SLOTS,
        }
        intent.explain_parts.append(f"Gross by stakeholder over last {ndays} days (OVERLAP window).")
        return intent

    # ---- YTD top gross -------------------------------------------------------
    if ("ytd" in qi or "year to date" in qi) and "gross" in qi and "top" in qi:
        m_top = re.search(r"top\s+(\d+)", qi)
        m_year = _YTD_YEAR_RE.search(qn)
        if m_year:
            year = int(m_year.group(1) or m_year.group(2))
        else:
            year = t.year
        top_n = int(m_top.group(1)) if m_top else 5
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        intent.top_n = top_n
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = start, end
        intent.order_by = GROSS_SQL
        intent.order_desc = True
        intent.explain_parts.append(f"Top {top_n} by GROSS for {year} YTD (OVERLAP window).")
        _note_ordering(True)
        return intent

    # ---- Average gross per REQUEST_TYPE last N months -----------------------
    if "average gross" in qi and "request_type" in qi:
        m_months = re.search(r"last\s+(\d+)\s+months", qi)
        months = int(m_months.group(1)) if m_months else 6
        ds, de = _month_bounds_last_n(months, t)
        intent.group_by = "REQUEST_TYPE"
        intent.agg = "avg"
        intent.gross = True
        intent.window_kind = "REQUEST"
        intent.window_start, intent.window_end = ds, de
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append(f"Average GROSS per REQUEST_TYPE over last {months} months (REQUEST window).")
        return intent

    # ---- Monthly trend by REQUEST_DATE last 12 months -----------------------
    if "monthly trend" in qi and "last" in qi and "12" in qi and "request_date" in qi:
        ds, de = _month_bounds_last_n(12, t)
        intent.special = "monthly_trend_last_12m"
        intent.special_params = {"date_start": ds, "date_end": de}
        intent.explain_parts.append("Monthly count over last 12 months (REQUEST_DATE window).")
        return intent

    # ---- Entity totals by status --------------------------------------------
    m_entity = re.search(r"entity_no\s*=\s*'([^']+)'", qi)
    if m_entity and "contract_status" in qi:
        intent.special = "entityno_totals_by_status"
        intent.special_params = {"entity_no": m_entity.group(1)}
        intent.explain_parts.append("Totals and counts by CONTRACT_STATUS for a specific ENTITY_NO.")
        return intent

    # ---- Expiring buckets 30/60/90 -----------------------------------------
    if "30/60/90" in qi and "expiring" in qi:
        intent.special = "expiring_buckets_30_60_90"
        intent.special_params = {"today": t}
        intent.explain_parts.append("Counts for END_DATE buckets at 30/60/90 days.")
        return intent

    # ---- Highest average gross owner department last quarter ---------------
    if "owner department" in qi and "highest average gross" in qi and "last quarter" in qi:
        ds, de = _last_quarter(t)
        intent.group_by = "OWNER_DEPARTMENT"
        intent.agg = "avg"
        intent.gross = True
        intent.window_kind = "OVERLAP"
        intent.window_start, intent.window_end = ds, de
        intent.top_n = 1
        intent.order_by = "MEASURE"
        intent.order_desc = True
        intent.explain_parts.append("Top OWNER_DEPARTMENT by average GROSS last quarter (OVERLAP window).")
        return intent

    # ---- Stakeholders with more than N contracts in 2024 -------------------
    if "stakeholders" in qi and "more than" in qi and "2024" in qi:
        m_more = re.search(r"more than\s+(\d+)", qi)
        min_n = int(m_more.group(1)) if m_more else 5
        intent.special = "stakeholders_more_than_n_2024"
        intent.special_params = {"min_n": min_n}
        intent.explain_parts.append(f"Stakeholders with more than {min_n} contracts in 2024.")
        return intent

    # ---- Representative email missing --------------------------------------
    if "representative_email" in qi and "missing" in qi:
        intent.special = "rep_email_missing"
        intent.explain_parts.append("Rows with missing representative_email.")
        return intent

    # ---- Requester totals by quarter ---------------------------------------
    m_requester = re.search(r"requester\s*=\s*'([^']+)'", qi)
    if m_requester and "quarter" in qi:
        intent.special = "requester_quarterly_totals"
        intent.special_params = {"requester": m_requester.group(1)}
        intent.explain_parts.append("Quarterly totals for a requester (REQUEST_DATE window).")
        return intent

    # ---- Stakeholder departments 2024 --------------------------------------
    if "stakeholder" in qi and "departments" in qi and "2024" in qi:
        intent.special = "stakeholder_departments_2024"
        intent.special_params = {"year": 2024}
        intent.explain_parts.append("Stakeholder departments touched in 2024 with totals.")
        return intent

    # ---- Top pairs by gross last 180 days ----------------------------------
    if "pairs" in qi and "last" in qi and "180" in qi:
        intent.special = "top_pairs_last_180d"
        intent.special_params = {"days": 180}
        intent.explain_parts.append("Top OWNER_DEPARTMENT / stakeholder pairs by GROSS last 180 days.")
        return intent

    # ---- Duplicate CONTRACT_IDs --------------------------------------------
    if "duplicate contract ids" in qi:
        intent.special = "duplicate_contract_ids"
        intent.explain_parts.append("Detecting duplicate CONTRACT_ID values.")
        return intent

    # ---- Median gross by owner department this year ------------------------
    if "median gross" in qi and "owner department" in qi and "this year" in qi:
        intent.special = "median_gross_by_owner_dept_this_year"
        intent.special_params = {"year": t.year}
        intent.explain_parts.append("Median GROSS per OWNER_DEPARTMENT for current year (OVERLAP).")
        return intent

    # ---- END_DATE < START_DATE ---------------------------------------------
    if "end_date < start_date" in qi:
        intent.special = "end_before_start"
        intent.explain_parts.append("Integrity check for END_DATE before START_DATE.")
        return intent

    # ---- Duration mismatch for ~12 months ----------------------------------
    if "duration" in qi and "12" in qi and "months" in qi:
        intent.special = "duration_12m_mismatch"
        intent.explain_parts.append("Duration text ~12 months but date diff mismatches.")
        return intent

    # ---- Year-over-year comparison -----------------------------------------
    if "year-over-year" in qi or "yoy" in qi:
        intent.special = "yoy_same_period"
        intent.special_params = {"today": t}
        intent.explain_parts.append("YoY gross totals for the same calendar window.")
        return intent

    # ---- Status threshold gross -------------------------------------------
    if "contract_status in" in qi and (">" in qi or "threshold" in qi):
        intent.special = "status_threshold_gross"
        intent.special_params = {"text": qi}
        intent.explain_parts.append("Filtering by CONTRACT_STATUS list with gross threshold.")
        return intent

    # ---- Top 3 per entity last 365 days ------------------------------------
    if ("top 3" in qi and "per entity" in qi) or ("each entity" in qi and "top 3" in qi):
        intent.special = "entity_top3_last365"
        intent.special_params = {"days": 365}
        intent.explain_parts.append("Top 3 contracts by GROSS per ENTITY over last 365 days.")
        return intent

    # ---- Owner vs OUL mismatch ---------------------------------------------
    owner_dept_hint = "owner_department" in qi or "owner department" in qi or "owner dept" in qi
    oul_hint = "department_oul" in qi or "department oul" in qi or "oul" in qi
    compare_hint = "mismatch" in qi or " vs " in qi or "compare" in qi or "versus" in qi
    if owner_dept_hint and oul_hint and compare_hint:
        intent.special = "owner_vs_oul_mismatch"
        intent.explain_parts.append("Comparing OWNER_DEPARTMENT vs DEPARTMENT_OUL.")
        return intent

    # Fallback: list all ordered by REQUEST_DATE desc
    intent.order_by = "REQUEST_DATE"
    intent.order_desc = not lowest_hint
    if lowest_hint:
        intent.explain_parts.append("Fallback listing ordered by REQUEST_DATE ASC due to lowest/bottom phrasing.")
    else:
        intent.explain_parts.append("Fallback listing ordered by REQUEST_DATE DESC.")
    return intent


# ---- Build SQL ---------------------------------------------------------------
def _build_special(intent: Intent) -> tuple[str, dict, dict]:
    params = intent.special_params or {}
    special = intent.special or ""
    explain_meta = "; ".join(intent.explain_parts or [])

    if special == "missing_contract_id":
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE CONTRACT_ID IS NULL OR TRIM(CONTRACT_ID) = ''\n"
            "ORDER BY REQUEST_DATE DESC"
        )
        return sql, {}, _build_meta(intent, explain=explain_meta, strategy="contract_deterministic")

    if special == "gross_by_stakeholder_slots_last_ndays":
        date_start = _to_date(params.get("date_start"))
        date_end = _to_date(params.get("date_end"))
        slots = int(params.get("slots", STAKEHOLDER_SLOTS))
        gross_alias = _gross_from_alias("NET", "VAT")
        overlap = _overlap_condition()
        union_parts = []
        for idx in range(1, slots + 1):
            union_parts.append(
                f"SELECT CONTRACT_STAKEHOLDER_{idx} AS STAKEHOLDER,\n"
                "       NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NET,\n"
                "       NVL(VAT,0) AS VAT\n"
                '  FROM "Contract"\n'
                f"  WHERE {overlap}"
            )
        cte = "\nUNION ALL\n".join(union_parts)
        sql = (
            "WITH S AS (\n"
            f"{cte}\n"
            ")\n"
            "SELECT STAKEHOLDER AS GROUP_KEY,\n"
            f"       SUM({gross_alias}) AS MEASURE\n"
            "FROM S\n"
            "WHERE STAKEHOLDER IS NOT NULL\n"
            "GROUP BY STAKEHOLDER\n"
            "ORDER BY MEASURE DESC"
        )
        binds = {"date_start": date_start, "date_end": date_end}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            gross=True,
            group_by="STAKEHOLDER",
            strategy="contract_deterministic",
        )

    if special == "monthly_trend_last_12m":
        date_start = _to_date(params.get("date_start"))
        date_end = _to_date(params.get("date_end"))
        sql = (
            "SELECT TRUNC(REQUEST_DATE, 'MM') AS MONTH,\n"
            "       COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "WHERE REQUEST_DATE BETWEEN :date_start AND :date_end\n"
            "GROUP BY TRUNC(REQUEST_DATE, 'MM')\n"
            "ORDER BY MONTH ASC"
        )
        binds = {"date_start": date_start, "date_end": date_end}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            agg="count",
            group_by="MONTH",
            strategy="contract_deterministic",
        )

    if special == "entityno_totals_by_status":
        entity_no = params.get("entity_no")
        sql = (
            "SELECT CONTRACT_STATUS AS GROUP_KEY,\n"
            f"       SUM({GROSS_SQL}) AS MEASURE,\n"
            "       COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "WHERE ENTITY_NO = :entity_no\n"
            "GROUP BY CONTRACT_STATUS\n"
            "ORDER BY MEASURE DESC"
        )
        binds = {"entity_no": entity_no}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            gross=True,
            group_by="CONTRACT_STATUS",
            strategy="contract_deterministic",
        )

    if special == "expiring_buckets_30_60_90":
        base = params.get("today") or date.today()
        d30s = _to_date(base)
        d30e = _to_date(base + timedelta(days=30))
        d60s = _to_date(base)
        d60e = _to_date(base + timedelta(days=60))
        d90s = _to_date(base)
        d90e = _to_date(base + timedelta(days=90))
        sql = (
            "SELECT 30 AS BUCKET_DAYS, COUNT(*) AS CNT FROM \"Contract\" WHERE END_DATE BETWEEN :d30s AND :d30e\n"
            "UNION ALL\n"
            "SELECT 60 AS BUCKET_DAYS, COUNT(*) AS CNT FROM \"Contract\" WHERE END_DATE BETWEEN :d60s AND :d60e\n"
            "UNION ALL\n"
            "SELECT 90 AS BUCKET_DAYS, COUNT(*) AS CNT FROM \"Contract\" WHERE END_DATE BETWEEN :d90s AND :d90e\n"
            "ORDER BY BUCKET_DAYS"
        )
        binds = {
            "d30s": d30s,
            "d30e": d30e,
            "d60s": d60s,
            "d60e": d60e,
            "d90s": d90s,
            "d90e": d90e,
        }
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            strategy="contract_deterministic",
            agg="count",
        )

    if special == "stakeholders_more_than_n_2024":
        min_n = int(params.get("min_n", 5))
        ds = _to_date(date(2024, 1, 1))
        de = _to_date(date(2024, 12, 31))
        union_parts = []
        for idx in range(1, STAKEHOLDER_SLOTS + 1):
            union_parts.append(
                f"SELECT CONTRACT_STAKEHOLDER_{idx} AS STAKEHOLDER\n"
                '  FROM "Contract"\n'
                "  WHERE REQUEST_DATE BETWEEN :date_start AND :date_end"
            )
        cte = "\nUNION ALL\n".join(union_parts)
        sql = (
            "WITH S AS (\n"
            f"{cte}\n"
            ")\n"
            "SELECT STAKEHOLDER AS GROUP_KEY, COUNT(*) AS CNT\n"
            "FROM S\n"
            "WHERE STAKEHOLDER IS NOT NULL\n"
            "GROUP BY STAKEHOLDER\n"
            "HAVING COUNT(*) > :min_n\n"
            "ORDER BY CNT DESC"
        )
        binds = {"date_start": ds, "date_end": de, "min_n": min_n}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            strategy="contract_deterministic",
            group_by="STAKEHOLDER",
            agg="count",
        )

    if special == "rep_email_missing":
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE representative_email IS NULL OR TRIM(representative_email) = ''\n"
            "ORDER BY REQUEST_DATE DESC"
        )
        return sql, {}, _build_meta(intent, explain=explain_meta, strategy="contract_deterministic")

    if special == "requester_quarterly_totals":
        requester = params.get("requester")
        sql = (
            "SELECT TRUNC(REQUEST_DATE,'Q') AS QUARTER,\n"
            f"       SUM({GROSS_SQL}) AS TOTAL_GROSS,\n"
            "       COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "WHERE UPPER(REQUESTER)=UPPER(:requester)\n"
            "GROUP BY TRUNC(REQUEST_DATE,'Q')\n"
            "ORDER BY QUARTER ASC"
        )
        binds = {"requester": requester}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            gross=True,
            strategy="contract_deterministic",
            group_by="QUARTER",
        )

    if special == "stakeholder_departments_2024":
        year = int(params.get("year", 2024))
        ds = _to_date(date(year, 1, 1))
        de = _to_date(date(year, 12, 31))
        overlap = _overlap_condition()
        union_parts = []
        for idx in range(1, STAKEHOLDER_SLOTS + 1):
            union_parts.append(
                f"SELECT CONTRACT_STAKEHOLDER_{idx} AS STAKEHOLDER,\n"
                "       OWNER_DEPARTMENT,\n"
                "       NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NET,\n"
                "       NVL(VAT,0) AS VAT\n"
                '  FROM "Contract"\n'
                f"  WHERE {overlap}"
            )
        cte = "\nUNION ALL\n".join(union_parts)
        gross_alias = _gross_from_alias("NET", "VAT")
        sql = (
            "WITH S AS (\n"
            f"{cte}\n"
            ")\n"
            "SELECT STAKEHOLDER AS GROUP_KEY,\n"
            "       LISTAGG(DISTINCT OWNER_DEPARTMENT, ', ') WITHIN GROUP (ORDER BY OWNER_DEPARTMENT) AS DEPARTMENTS,\n"
            f"       SUM({gross_alias}) AS MEASURE,\n"
            "       COUNT(*) AS CNT\n"
            "FROM S\n"
            "WHERE STAKEHOLDER IS NOT NULL\n"
            "GROUP BY STAKEHOLDER\n"
            "ORDER BY MEASURE DESC"
        )
        binds = {"date_start": ds, "date_end": de}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            gross=True,
            strategy="contract_deterministic",
            group_by="STAKEHOLDER",
        )

    if special == "top_pairs_last_180d":
        days = int(params.get("days", 180))
        end_date = params.get("today") or date.today()
        ds = _to_date(end_date - timedelta(days=days))
        de = _to_date(end_date)
        overlap = _overlap_condition()
        union_parts = []
        for idx in range(1, STAKEHOLDER_SLOTS + 1):
            union_parts.append(
                f"SELECT OWNER_DEPARTMENT, CONTRACT_STAKEHOLDER_{idx} AS STAKEHOLDER,\n"
                "       NVL(CONTRACT_VALUE_NET_OF_VAT,0) AS NET,\n"
                "       NVL(VAT,0) AS VAT\n"
                '  FROM "Contract"\n'
                f"  WHERE {overlap}"
            )
        cte = "\nUNION ALL\n".join(union_parts)
        gross_alias = _gross_from_alias("NET", "VAT")
        sql = (
            "WITH P AS (\n"
            f"{cte}\n"
            ")\n"
            "SELECT OWNER_DEPARTMENT, STAKEHOLDER,\n"
            f"       SUM({gross_alias}) AS MEASURE\n"
            "FROM P\n"
            "WHERE STAKEHOLDER IS NOT NULL\n"
            "GROUP BY OWNER_DEPARTMENT, STAKEHOLDER\n"
            "ORDER BY MEASURE DESC\n"
            "FETCH FIRST 10 ROWS ONLY"
        )
        binds = {"date_start": ds, "date_end": de}
        return sql, binds, _build_meta(
            intent,
            explain=explain_meta,
            gross=True,
            strategy="contract_deterministic",
            group_by="OWNER_DEPARTMENT,STAKEHOLDER",
        )

    if special == "duplicate_contract_ids":
        sql = (
            "SELECT CONTRACT_ID, COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "GROUP BY CONTRACT_ID\n"
            "HAVING COUNT(*) > 1\n"
            "ORDER BY CNT DESC"
        )
        return sql, {}, _build_meta(
            intent,
            explain=explain_meta,
            strategy="contract_deterministic",
            group_by="CONTRACT_ID",
            agg="count",
        )

    if special == "median_gross_by_owner_dept_this_year":
        year = int(params.get("year", date.today().year))
        ds = _to_date(date(year, 1, 1))
        de = _to_date(date(year, 12, 31))
        condition = _overlap_condition()
        sql = (
            "SELECT OWNER_DEPARTMENT AS GROUP_KEY, MEDIAN("
            f"{GROSS_SQL}"
            ") AS MEASURE\n"
            'FROM "Contract"\n'
            f"WHERE {condition}\n"
            "GROUP BY OWNER_DEPARTMENT\n"
            "ORDER BY MEASURE DESC"
        )
        binds = {"date_start": ds, "date_end": de}
        return sql, binds, _build_meta(intent, explain=explain_meta, gross=True, strategy="contract_deterministic")

    if special == "end_before_start":
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE END_DATE < START_DATE\n"
            "ORDER BY REQUEST_DATE DESC"
        )
        return sql, {}, _build_meta(intent, explain=explain_meta, strategy="contract_deterministic")

    if special == "duration_12m_mismatch":
        sql = (
            "SELECT * FROM \"Contract\"\n"
            "WHERE REGEXP_LIKE(NVL(DURATION,''), '12')\n"
            "  AND (START_DATE IS NOT NULL AND END_DATE IS NOT NULL)\n"
            "  AND ABS(MONTHS_BETWEEN(END_DATE, START_DATE) - 12) > 1\n"
            "ORDER BY REQUEST_DATE DESC"
        )
        return sql, {}, _build_meta(intent, explain=explain_meta, strategy="contract_deterministic")

    if special == "yoy_same_period":
        today_val = params.get("today") or date.today()
        current_start = date(today_val.year, 1, 1)
        current_end = date(today_val.year, 3, 31)
        prev_start = date(today_val.year - 1, 1, 1)
        prev_end = date(today_val.year - 1, 3, 31)
        binds = {
            "ds": _to_date(current_start),
            "de": _to_date(min(current_end, today_val)),
            "p_ds": _to_date(prev_start),
            "p_de": _to_date(prev_end),
        }
        # Overlap-based YoY: treat "contracts" as active when the contract window overlaps each period.
        current_overlap = _overlap_condition(":ds", ":de")
        previous_overlap = _overlap_condition(":p_ds", ":p_de")
        sql = (
            "SELECT 'CURRENT' AS PERIOD, SUM("
            f"{GROSS_SQL}"
            ") AS TOTAL_GROSS\n"
            'FROM "Contract"\n'
            f"WHERE {current_overlap}\n"
            "UNION ALL\n"
            "SELECT 'PREVIOUS' AS PERIOD, SUM("
            f"{GROSS_SQL}"
            ") AS TOTAL_GROSS\n"
            'FROM "Contract"\n'
            f"WHERE {previous_overlap}"
        )
        return sql, binds, _build_meta(intent, explain=explain_meta, gross=True, strategy="contract_deterministic")

    if special == "status_threshold_gross":
        raw = params.get("text", "")
        m_status = re.search(r"contract_status\s+in\s*\(([^)]+)\)", raw)
        statuses = []
        if m_status:
            statuses = [s.strip().strip("'\"") for s in m_status.group(1).split(",") if s.strip()]
        if not statuses:
            statuses = ["Active", "Pending"]
        m_thr = re.search(r">\s*([0-9][0-9,]*)", raw)
        threshold = int(m_thr.group(1).replace(",", "")) if m_thr else 1_000_000
        placeholders = ", ".join(f":s{i}" for i in range(len(statuses)))
        sql = (
            "SELECT * FROM \"Contract\"\n"
            f"WHERE CONTRACT_STATUS IN ({placeholders})\n"
            f"  AND ({GROSS_SQL}) > :thr\n"
            f"ORDER BY {GROSS_SQL} DESC"
        )
        binds = {f"s{i}": status for i, status in enumerate(statuses)}
        binds["thr"] = threshold
        return sql, binds, _build_meta(intent, explain=explain_meta, gross=True, strategy="contract_deterministic")

    if special == "entity_top3_last365":
        days = int(params.get("days", 365))
        end_date = params.get("today") or date.today()
        ds = _to_date(end_date - timedelta(days=days))
        de = _to_date(end_date)
        overlap = _overlap_condition(prefix="c.")
        sql = (
            "SELECT * FROM (\n"
            "  SELECT c.*, ROW_NUMBER() OVER (PARTITION BY ENTITY ORDER BY "
            f"{GROSS_SQL}"
            " DESC) AS rn\n"
            '  FROM "Contract" c\n'
            f"  WHERE {overlap}\n"
            ")\n"
            "WHERE rn <= 3\n"
            "ORDER BY ENTITY, rn"
        )
        binds = {"date_start": ds, "date_end": de}
        return sql, binds, _build_meta(intent, explain=explain_meta, gross=True, strategy="contract_deterministic")

    if special == "owner_vs_oul_mismatch":
        sql = (
            "SELECT OWNER_DEPARTMENT, DEPARTMENT_OUL, COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "WHERE DEPARTMENT_OUL IS NOT NULL\n"
            "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
            "GROUP BY OWNER_DEPARTMENT, DEPARTMENT_OUL\n"
            "ORDER BY CNT DESC"
        )
        return sql, {}, _build_meta(intent, explain=explain_meta, strategy="contract_deterministic")

    return "", {}, _build_meta(intent, explain=explain_meta)


def build_sql(intent: Intent, settings: Optional[Dict[str, object]] = None) -> tuple[str, dict, dict]:
    if intent.special:
        return _build_special(intent)

    binds = {}
    parts = []
    settings_map: Dict[str, object] = dict(settings or {})

    # Window WHERE clause
    if intent.window_kind == "REQUEST":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("REQUEST_DATE BETWEEN :date_start AND :date_end")
            intent.explain_parts.append(
                f"Window = REQUEST_DATE between {binds['date_start']} and {binds['date_end']}."
            )
    elif intent.window_kind == "END_ONLY":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("END_DATE BETWEEN :date_start AND :date_end")
            intent.explain_parts.append(
                f"Window = END_DATE between {binds['date_start']} and {binds['date_end']}."
            )
    elif intent.window_kind == "OVERLAP":
        if intent.window_start and intent.window_end:
            binds["date_start"] = _to_date(intent.window_start)
            binds["date_end"]   = _to_date(intent.window_end)
            parts.append("(START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
                         "AND START_DATE <= :date_end AND END_DATE >= :date_start)")

    # Explicit where
    for wc in (intent.where_clauses or []):
        parts.append(wc)
    for k,v in (intent.where_binds or {}).items():
        binds[k] = v

    # REQUEST_TYPE filter via synonyms (avoid duplicate clauses)
    existing_reqtype = any(
        "REQUEST_TYPE" in (wc or "").upper() for wc in (intent.where_clauses or [])
    )
    if not existing_reqtype:
        reqtype = extract_request_type_filter(intent.question, settings_map)
        if reqtype:
            where_sql, where_binds, note = reqtype
            parts.append(where_sql)
            binds.update(where_binds)
            intent.explain_parts.append(note)

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
        elif intent.agg == "avg":
            measure = gross_expr if intent.gross else "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
            sql = (
                f"SELECT\n  {intent.group_by} AS GROUP_KEY,\n"
                f"  AVG({measure}) AS MEASURE\n"
                f'FROM "Contract"{where_sql}\n'
                f"GROUP BY {intent.group_by}"
            )
            order_col = intent.order_by or "MEASURE"
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


def build_contract_sql(
    question: str,
    settings: Dict[str, object],
    *,
    today: date | None = None,
) -> tuple[str, dict, dict]:
    """Parse the question and build deterministic Contract SQL with settings."""

    intent = parse_intent(question, today=today)
    return build_sql(intent, settings=settings)


def plan_sql(
    question: str,
    today: date | None = None,
    settings: Optional[Dict[str, object]] = None,
) -> tuple[str, dict, dict]:
    intent = parse_intent(question, today=today)
    return build_sql(intent, settings=settings)
