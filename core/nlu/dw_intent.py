from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import re
from dateutil.relativedelta import relativedelta

__all__ = ["DWIntent", "parse_intent"]


_NUM = re.compile(r"\b(\d+)\b")


@dataclass
class DWIntent:
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    agg: Optional[str] = None
    group_by: Optional[str] = None
    measure_sql: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    wants_all_columns: Optional[bool] = None
    notes: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _window_last_month(today: datetime) -> Dict[str, str]:
    first_curr = today.replace(day=1)
    last_prev = first_curr - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    return {
        "start": first_prev.strftime("%Y-%m-%d"),
        "end": last_prev.strftime("%Y-%m-%d"),
    }


def _window_last_n_months(today: datetime, n: int, calendar_full: bool = True) -> Dict[str, str]:
    if calendar_full:
        start_anchor = today.replace(day=1) - relativedelta(months=n)
        end_anchor = today.replace(day=1) - timedelta(days=1)
        return {
            "start": start_anchor.strftime("%Y-%m-%d"),
            "end": end_anchor.strftime("%Y-%m-%d"),
        }

    start = today - relativedelta(months=n)
    return {
        "start": start.strftime("%Y-%m-%d"),
        "end": today.strftime("%Y-%m-%d"),
    }


def _window_next_n_days(today: datetime, n: int) -> Dict[str, str]:
    return {
        "start": today.strftime("%Y-%m-%d"),
        "end": (today + timedelta(days=n)).strftime("%Y-%m-%d"),
    }


def parse_intent(
    question: str,
    defaults: Dict[str, Any] | None = None,
    now: datetime | None = None,
) -> DWIntent:
    text = (question or "").strip()
    lowered = text.lower()
    now = now or datetime.utcnow()
    defaults = defaults or {}

    intent = DWIntent(notes={"q": text})
    intent.wants_all_columns = bool(defaults.get("select_all_default", True))

    if "last month" in lowered:
        intent.has_time_window = True
        intent.explicit_dates = _window_last_month(now)
    elif "last quarter" in lowered:
        quarter_idx = (now.month - 1) // 3
        quarter_end_month = quarter_idx * 3
        if quarter_end_month == 0:
            year = now.year - 1
            start = datetime(year, 10, 1)
            end = datetime(year, 12, 31)
        else:
            year = now.year
            start = datetime(year, quarter_end_month - 2, 1)
            end = datetime(year, quarter_end_month, 1) + relativedelta(months=1) - timedelta(days=1)
        intent.has_time_window = True
        intent.explicit_dates = {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        }
    elif "last 3 months" in lowered:
        intent.has_time_window = True
        intent.explicit_dates = _window_last_n_months(now, 3, calendar_full=True)
    else:
        match_next_days = re.search(r"\bnext\s+(\d{1,3})\s+days?\b", lowered)
        if match_next_days:
            days = int(match_next_days.group(1))
            intent.has_time_window = True
            intent.explicit_dates = _window_next_n_days(now, days)

    if any(token in lowered for token in ["expiring", "expire", "expires"]):
        intent.date_column = "END_DATE"
    elif "requested" in lowered:
        intent.date_column = "REQUEST_DATE"

    if intent.date_column is None:
        intent.date_column = str(defaults.get("date_column", "REQUEST_DATE"))

    if "count" in lowered or "(count)" in lowered:
        intent.agg = "count"

    if " by " in lowered or " per " in lowered:
        if "department" in lowered:
            intent.group_by = "OWNER_DEPARTMENT"
        elif "status" in lowered:
            intent.group_by = "CONTRACT_STATUS"
        elif "entity" in lowered:
            intent.group_by = "ENTITY_NO"
        elif "owner" in lowered:
            intent.group_by = "CONTRACT_OWNER"
        elif "stakeholder" in lowered:
            intent.group_by = "CONTRACT_STAKEHOLDER_1"
    elif "stakeholder" in lowered:
        intent.group_by = "CONTRACT_STAKEHOLDER_1"

    if "gross" in lowered:
        intent.agg = intent.agg or "sum"
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
        )
        intent.sort_by = "GROSS_VALUE"
        intent.sort_desc = True
    elif any(token in lowered for token in ["contract value", "net value", "value"]):
        intent.agg = intent.agg or "sum"
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
        intent.sort_by = intent.sort_by or intent.measure_sql
        intent.sort_desc = True

    if "top" in lowered:
        match_num = _NUM.search(lowered)
        if match_num:
            intent.top_n = int(match_num.group(1))
            intent.user_requested_top_n = True

    if "list all" in lowered or lowered.startswith("list "):
        intent.wants_all_columns = False
        if "list all" in lowered:
            intent.wants_all_columns = True

    return intent
