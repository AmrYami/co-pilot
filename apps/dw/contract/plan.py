from __future__ import annotations

"""Planning logic to map Contract questions to SQL."""

from typing import Dict, Any, Tuple
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from .intent import parse_contract_intent, ContractIntent
from . import sqlgen


def _window_from_intent(ci: ContractIntent) -> Dict[str, Any]:
    """Compute date_start/date_end bindings and explanations from intent."""

    today = date.today()
    explain_parts = []
    binds: Dict[str, Any] = {}

    if ci.ytd:
        year = ci.ytd_year or today.year
        start = date(year, 1, 1)
        end = today if year == today.year else date(year, 12, 31)
        binds["date_start"] = start
        binds["date_end"] = end
        label = "year-to-date" if year == today.year else f"year-to-date {year}"
        explain_parts.append(
            f"Window: {label} ({start.isoformat()}..{end.isoformat()})."
        )
    elif ci.last_month:
        first_day_this_month = today.replace(day=1)
        end_last_month = first_day_this_month - timedelta(days=1)
        start_last_month = end_last_month.replace(day=1)
        binds["date_start"] = start_last_month
        binds["date_end"] = end_last_month
        explain_parts.append(
            f"Window: last month ({start_last_month.isoformat()}..{end_last_month.isoformat()})."
        )
    elif ci.last_n_months:
        start = (today - relativedelta(months=ci.last_n_months)).replace(day=1)
        binds["date_start"] = start
        binds["date_end"] = today
        explain_parts.append(
            f"Window: last {ci.last_n_months} months ({start.isoformat()}..{today.isoformat()})."
        )
    elif ci.next_n_days:
        start = today
        end = today + timedelta(days=ci.next_n_days)
        binds["date_start"] = start
        binds["date_end"] = end
        explain_parts.append(
            f"Window: next {ci.next_n_days} days ({start.isoformat()}..{end.isoformat()})."
        )
    elif ci.year_literal:
        start = date(ci.year_literal, 1, 1)
        end = date(ci.year_literal, 12, 31)
        binds["date_start"] = start
        binds["date_end"] = end
        explain_parts.append(
            f"Window: calendar year {ci.year_literal} ({start.isoformat()}..{end.isoformat()})."
        )

    return {"binds": binds, "explain": " ".join(explain_parts)}


def build_sql_for_question(q: str) -> Tuple[str, Dict[str, Any], str]:
    """Return SQL, binds, and explanation for a Contract question."""

    ci = parse_contract_intent(q)
    if not ci:
        return "", {}, ""

    window_info = _window_from_intent(ci)
    binds = {**window_info.get("binds", {})}
    explain_parts = []
    if window_info.get("explain"):
        explain_parts.append(window_info["explain"])

    if ci.top_n:
        binds["top_n"] = ci.top_n
        explain_parts.append(f"Top {ci.top_n} requested.")

    if ci.action == "top_net":
        explain_parts.append("Sorting by NET contract value.")
        return sqlgen.top_contracts_by_net(True), binds, " ".join(explain_parts)

    if ci.action == "top_gross":
        if "top_n" not in binds and ci.ytd:
            binds["top_n"] = 5
            explain_parts.append("Top 5 default for YTD request.")
        explain_parts.append("Sorting by GROSS contract value.")
        return sqlgen.top_contracts_by_gross(True), binds, " ".join(explain_parts)

    if ci.action == "list_requested_basic":
        explain_parts.append(
            "Requested window on REQUEST_DATE; projecting (CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE)."
        )
        return sqlgen.list_requested_basic_columns(), binds, " ".join(explain_parts)

    if ci.action == "group_gross_owner_dept_last_window":
        explain_parts.append(
            "Grouping by OWNER_DEPARTMENT; measuring GROSS; using overlap window."
        )
        return sqlgen.gross_per_owner_department_last_window(), binds, " ".join(explain_parts)

    if ci.action == "group_gross_owner_dept_all_time":
        explain_parts.append("Grouping by OWNER_DEPARTMENT; measuring GROSS; all-time.")
        return sqlgen.gross_per_owner_department_all_time(), binds, " ".join(explain_parts)

    if ci.action == "status_counts":
        explain_parts.append("Counting contracts per CONTRACT_STATUS; all-time.")
        return sqlgen.status_counts_all_time(), binds, " ".join(explain_parts)

    if ci.action == "expiring_count":
        explain_parts.append("Expiring window on END_DATE; returning count.")
        return sqlgen.expiring_count_30d(), binds, " ".join(explain_parts)

    if ci.action == "expiring_list":
        explain_parts.append("Expiring window on END_DATE; listing ascending by END_DATE.")
        return sqlgen.expiring_list_window(), binds, " ".join(explain_parts)

    if ci.action == "vat_zero_net_pos":
        explain_parts.append("Filter: VAT is null/zero AND NET value > 0.")
        return sqlgen.vat_zero_net_positive(), binds, " ".join(explain_parts)

    if ci.action == "reqtype_year":
        year = ci.year_literal or 2023
        explain_parts.append(f"Filter: REQUEST_TYPE = 'Renewal'; window = year {year}.")
        return sqlgen.requested_type_in_year("Renewal"), binds, " ".join(explain_parts)

    if ci.action == "entity_counts":
        explain_parts.append("Counting contracts per ENTITY; all-time.")
        return sqlgen.entity_counts(), binds, " ".join(explain_parts)

    if ci.action == "owner_dept_counts":
        explain_parts.append("Counting contracts per OWNER_DEPARTMENT; all-time.")
        return sqlgen.owner_department_counts(), binds, " ".join(explain_parts)

    if ci.action == "owner_vs_oul_mismatch":
        explain_parts.append("Comparing OWNER_DEPARTMENT against DEPARTMENT_OUL mismatches.")
        return sqlgen.owner_vs_oul_mismatch(), binds, " ".join(explain_parts)

    return "", {}, ""

