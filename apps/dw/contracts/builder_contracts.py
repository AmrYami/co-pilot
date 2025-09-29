from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Sequence, Tuple

from core.nlu.schema import NLIntent

from .sql_templates import (
    BuiltSQL,
    sql_avg_gross_by_request_type,
    sql_counts_30_60_90,
    sql_duplicate_contract_ids,
    sql_duration_mismatch_12m,
    sql_end_before_start,
    sql_entity_top3_gross,
    sql_gross_by_stakeholder_slots,
    sql_missing_contract_id,
    sql_missing_rep_email,
    sql_monthly_trend_by_request_date,
    sql_owner_dept_highest_avg_gross,
    sql_owner_stakeholder_pairs_top,
    sql_owner_vs_oul_mismatch,
    sql_requester_quarter_totals,
    sql_stakeholder_dept_2024,
    sql_stakeholders_more_than_n_2024,
    sql_status_in_gross_threshold,
    sql_status_totals_for_entity_no,
    sql_top_gross_ytd,
    sql_yoy,
    sql_median_gross_by_owner_dept_this_year,
)


# ---------------------------------------------------------------------------
# Lightweight SQL builder helpers used by the new deterministic planner path.
# ---------------------------------------------------------------------------

# Re-export a common gross expression so we do not repeat it.
GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)

LOWEST_TOKENS = {"lowest", "bottom", "least", "smallest"}
HIGHEST_TOKENS = {"highest", "top", "largest", "biggest"}


def _has_token(q: str, tokens: set[str]) -> bool:
    t = q.lower()
    return any(tok in t for tok in tokens)


def infer_sort_desc_from_text(q: str, default_desc: bool = True) -> bool:
    """
    If the question contains 'lowest/bottom/least', return False (ASC).
    If it contains 'highest/top', return True (DESC).
    Else fall back to default_desc.
    """

    if _has_token(q, LOWEST_TOKENS):
        return False
    if _has_token(q, HIGHEST_TOKENS):
        return True
    return default_desc


def build_overlap_predicate(ds_bind: str = ":date_start", de_bind: str = ":date_end") -> str:
    """Active-window overlap predicate between binds."""

    return "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= {de} AND END_DATE >= {ds})".format(
        de=de_bind, ds=ds_bind
    )


def build_top_contracts_by_net(
    q: str,
    use_window: bool,
    top_n_bind: str = ":top_n",
    ds_bind: str = ":date_start",
    de_bind: str = ":date_end",
) -> Tuple[str, Dict[str, Any], str]:
    """
    Build 'Top/Bottom N contracts by NET'. When use_window is True, use OVERLAP window.
    Returns (sql, binds, explain).
    """

    order_desc = infer_sort_desc_from_text(q, default_desc=True)
    order_word = "DESC" if order_desc else "ASC"
    where_clause = ""
    explain = "Top contracts by NET"
    if use_window:
        where_clause = "WHERE " + build_overlap_predicate(ds_bind, de_bind) + "\n"
        explain += " in the requested window (OVERLAP)."
    sql = (
        'SELECT * FROM "Contract"\n'
        f"{where_clause}"
        f"ORDER BY NVL(CONTRACT_VALUE_NET_OF_VAT,0) {order_word}\n"
        f"FETCH FIRST {top_n_bind} ROWS ONLY"
    )
    return sql, {}, explain


def build_top_contracts_by_gross(
    q: str,
    use_window: bool,
    top_n_bind: str = ":top_n",
    ds_bind: str = ":date_start",
    de_bind: str = ":date_end",
) -> Tuple[str, Dict[str, Any], str]:
    """
    Build 'Top/Bottom N contracts by GROSS'. When use_window is True, use OVERLAP window.
    """

    order_desc = infer_sort_desc_from_text(q, default_desc=True)
    order_word = "DESC" if order_desc else "ASC"
    where_clause = ""
    explain = "Top contracts by GROSS"
    if use_window:
        where_clause = "WHERE " + build_overlap_predicate(ds_bind, de_bind) + "\n"
        explain += " in the requested window (OVERLAP)."
    sql = (
        'SELECT * FROM "Contract"\n'
        f"{where_clause}"
        f"ORDER BY {GROSS_EXPR} {order_word}\n"
        f"FETCH FIRST {top_n_bind} ROWS ONLY"
    )
    return sql, {}, explain


def build_grouped_gross_per_dim(
    dim: str,
    use_window: bool,
    ds_bind: str = ":date_start",
    de_bind: str = ":date_end",
    agg: str = "SUM",
) -> Tuple[str, Dict[str, Any], str]:
    """
    Build 'Gross per DIMENSION' (OWNER_DEPARTMENT / DEPARTMENT_OUL / ENTITY / ENTITY_NO / CONTRACT_STATUS / REQUEST_TYPE).
    """

    dim_expr = dim
    where_clause = ""
    explain = f"{agg} GROSS per {dim}"
    if use_window:
        where_clause = "WHERE " + build_overlap_predicate(ds_bind, de_bind) + "\n"
        explain += " in the requested window (OVERLAP)."
    sql = (
        f'SELECT\n  {dim_expr} AS GROUP_KEY,\n'
        f"  {agg}({GROSS_EXPR}) AS MEASURE\n"
        'FROM "Contract"\n'
        f"{where_clause}"
        f"GROUP BY {dim_expr}\n"
        "ORDER BY MEASURE DESC"
    )
    return sql, {}, explain


def build_owner_vs_oul_diff() -> Tuple[str, Dict[str, Any], str]:
    """
    Differences between OWNER_DEPARTMENT and DEPARTMENT_OUL with counts, ordered by CNT DESC.
    """

    sql = (
        'SELECT\n'
        "  NVL(TRIM(OWNER_DEPARTMENT),'(None)') AS OWNER_DEPARTMENT,\n"
        "  NVL(TRIM(DEPARTMENT_OUL),'(None)')  AS DEPARTMENT_OUL,\n"
        "  COUNT(*) AS CNT\n"
        'FROM "Contract"\n'
        "WHERE DEPARTMENT_OUL IS NOT NULL\n"
        "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
        "GROUP BY NVL(TRIM(OWNER_DEPARTMENT),'(None)'), NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
        "ORDER BY CNT DESC"
    )
    explain = "Owner vs OUL differences (non-null, non-equal), ordered by CNT DESC."
    return sql, {}, explain


def build_yoy_overlap(
    ds: str = ":ds",
    de: str = ":de",
    p_ds: str = ":p_ds",
    p_de: str = ":p_de",
) -> Tuple[str, Dict[str, Any], str]:
    """
    YoY gross totals for the same period using OVERLAP windows for 'active' contracts.
    """

    cur_overlap = build_overlap_predicate(ds_bind=ds, de_bind=de)
    prv_overlap = build_overlap_predicate(ds_bind=p_ds, de_bind=p_de)
    sql = (
        "SELECT 'CURRENT' AS PERIOD, SUM({gross}) AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {cur_overlap}\n"
        "UNION ALL\n"
        "SELECT 'PREVIOUS' AS PERIOD, SUM({gross}) AS TOTAL_GROSS\n"
        'FROM "Contract"\n'
        f"WHERE {prv_overlap}"
    ).format(gross=GROSS_EXPR)
    explain = "YoY gross totals using active OVERLAP windows (CURRENT vs PREVIOUS)."
    return sql, {}, explain


def _ensure_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def window_last_days(now: date, days: int) -> Tuple[date, date]:
    days = max(1, days)
    end = now
    start = now - timedelta(days=days - 1)
    return start, end


def window_last_months(now: date, months: int) -> Tuple[date, date]:
    months = max(1, months)
    end = now
    month = now.month
    year = now.year
    for _ in range(months - 1):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    start = date(year, month, 1)
    return start, end


def window_last_quarter(now: date) -> Tuple[date, date]:
    quarter = ((now.month - 1) // 3) + 1
    if quarter == 1:
        year = now.year - 1
        quarter = 4
    else:
        year = now.year
        quarter -= 1
    start_month = 3 * (quarter - 1) + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    if end_month > 12:
        end_month -= 12
        year += 1
    last_day = _month_last_day(year, end_month)
    end = date(year, end_month, last_day)
    return start, end


def _month_last_day(year: int, month: int) -> int:
    if month == 12:
        return 31
    next_month = date(year, month, 1) + timedelta(days=32)
    first_next = date(next_month.year, next_month.month, 1)
    return (first_next - timedelta(days=1)).day


def infer_yoy_period(now: date) -> Tuple[date, date]:
    start = date(now.year, 1, 1)
    end = now
    return start, end


_RE_QUOTED = re.compile(r"'([^']+)'")


def extract_quoted_value(question: Optional[str], *, key: Optional[str] = None) -> Optional[str]:
    if not question:
        return None
    text = question
    if key:
        pattern = re.compile(rf"{re.escape(key)}[^']*'([^']+)'", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            return match.group(1).strip()
    match = _RE_QUOTED.search(text)
    if match:
        return match.group(1).strip()
    return None


_RE_INT = re.compile(r"(-?\d+)")


def extract_integer(text: str) -> Optional[int]:
    if not text:
        return None
    match = _RE_INT.search(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


_RE_NUMBER = re.compile(r"(-?\d[\d,]*(?:\.\d+)?)")


def extract_number(text: str) -> Optional[float]:
    if not text:
        return None
    match = _RE_NUMBER.search(text)
    if not match:
        return None
    value = match.group(1).replace(",", "")
    try:
        return float(value)
    except ValueError:
        return None


def extract_statuses(text: str) -> Sequence[str]:
    if not text:
        return []
    pattern = re.compile(r"CONTRACT_STATUS\s+IN\s*\(([^)]+)\)", re.IGNORECASE)
    match = pattern.search(text)
    if not match:
        return []
    inner = match.group(1)
    candidates = re.findall(r"'([^']+)'", inner)
    cleaned = [cand.strip() for cand in candidates if cand.strip()]
    return cleaned


def _fallback_listing() -> BuiltSQL:
    sql = (
        'SELECT *\n'
        'FROM "Contract"\n'
        'ORDER BY REQUEST_DATE DESC\n'
        'FETCH FIRST 50 ROWS ONLY'
    )
    return BuiltSQL(sql=sql, binds={})


def build_sql(intent: NLIntent, now: date | datetime | None = None) -> BuiltSQL:
    now = _ensure_date(now or date.today())
    q_raw = getattr(intent, "notes", {}).get("q") if getattr(intent, "notes", None) else None
    if not q_raw and getattr(intent, "question", None):
        q_raw = intent.question
    q = (q_raw or getattr(intent, "raw_question", "") or "").lower()

    if "missing" in q and "contract_id" in q:
        return sql_missing_contract_id()

    if "last 90 days" in q and "gross" in q and "stakeholder" in q:
        ds, de = window_last_days(now, 90)
        return sql_gross_by_stakeholder_slots(ds, de)

    if "2024 ytd" in q and "top" in q and "gross" in q:
        top = extract_integer(q) or 5
        return sql_top_gross_ytd(2024, top, today=now)

    if "average" in q and "request_type" in q and "last 6 months" in q:
        ds, de = window_last_months(now, 6)
        return sql_avg_gross_by_request_type(ds, de)

    if "monthly trend" in q and "last 12 months" in q:
        ds, de = window_last_months(now, 12)
        return sql_monthly_trend_by_request_date(ds, de)

    if "for entity_no" in q and "status" in q:
        entity_no = extract_quoted_value(q_raw or intent.question, key="ENTITY_NO")
        if entity_no:
            return sql_status_totals_for_entity_no(entity_no)

    if "expiring in 30/60/90 days" in q:
        return sql_counts_30_60_90(now)

    if "highest average gross" in q and "last quarter" in q:
        ds, de = window_last_quarter(now)
        return sql_owner_dept_highest_avg_gross(ds, de)

    if "stakeholders involved in more than" in q and "in 2024" in q:
        n_min = extract_integer(q) or 1
        return sql_stakeholders_more_than_n_2024(n_min)

    if "representative_email" in q and ("missing" in q or "null" in q or "blank" in q):
        return sql_missing_rep_email()

    if "total gross & count by quarter" in q and "requester" in q:
        requester = extract_quoted_value(q_raw or intent.question, key="REQUESTER")
        if requester:
            return sql_requester_quarter_totals(requester)

    if "for each stakeholder" in q and "2024" in q and "distinct departments" in q:
        return sql_stakeholder_dept_2024()

    if "pairs" in q and "last 180 days" in q:
        ds, de = window_last_days(now, 180)
        return sql_owner_stakeholder_pairs_top(ds, de, top_n=10)

    if "duplicate" in q and "contract id" in q:
        return sql_duplicate_contract_ids()

    if "median" in q and "owner department" in q and ("this year" in q or "current year" in q):
        return sql_median_gross_by_owner_dept_this_year(now)

    if "end_date < start_date" in q or "integrity check" in q:
        return sql_end_before_start()

    if "duration" in q and "12" in q and "months" in q and ("!=" in q or "not" in q or "mismatch" in q):
        return sql_duration_mismatch_12m()

    if "year-over-year" in q or "yoy" in q:
        ds, de = infer_yoy_period(now)
        return sql_yoy(ds, de)

    if "for contract_status in (" in q and "exceeds a threshold" in q:
        statuses = extract_statuses(q_raw or intent.question or "")
        if statuses:
            gross_min = extract_number(q) or 1_000_000
            return sql_status_in_gross_threshold(statuses, gross_min)

    if "for each entity" in q and "top 3" in q and "last 365 days" in q:
        ds, de = window_last_days(now, 365)
        return sql_entity_top3_gross(ds, de)

    if "owner_department vs department_oul" in q:
        return sql_owner_vs_oul_mismatch()

    return _fallback_listing()

