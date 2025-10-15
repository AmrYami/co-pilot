"""Helpers for injecting time windows into the legacy ``/dw/rate`` builder."""

from __future__ import annotations

from datetime import date
from typing import List, Mapping, MutableMapping, Optional, Tuple


WindowTuple = Tuple[date, date]


def apply_time_windows(
    where_parts: List[str],
    binds: MutableMapping[str, date],
    windows: Mapping[str, Optional[WindowTuple]],
    flags: Mapping[str, int],
) -> None:
    """Inject SQL predicates and binds for detected time windows."""

    # requested:
    if windows.get("requested"):
        start, end = windows["requested"] or (None, None)
        if start and end:
            binds["req_start"] = start
            binds["req_end"] = end
            where_parts.append("REQUEST_DATE BETWEEN :req_start AND :req_end")

    # active (overlap):
    if windows.get("active"):
        start, end = windows["active"] or (None, None)
        if start and end:
            binds["act_start"] = start
            binds["act_end"] = end
            if flags.get("DW_OVERLAP_REQUIRE_BOTH_DATES", 1):
                where_parts.append("START_DATE IS NOT NULL AND END_DATE IS NOT NULL")
                where_parts.append("START_DATE <= :act_end")
                where_parts.append("END_DATE   >= :act_start")
            else:
                where_parts.append("NVL(START_DATE, DATE '0001-01-01') <= :act_end")
                where_parts.append("NVL(END_DATE,   DATE '9999-12-31') >= :act_start")

    # expiring (end-only):
    if windows.get("expiring"):
        start, end = windows["expiring"] or (None, None)
        if start and end:
            binds["exp_start"] = start
            binds["exp_end"] = end
            where_parts.append("END_DATE BETWEEN :exp_start AND :exp_end")


def choose_order_by(order_by_from_user: Optional[str], windows: Mapping[str, Optional[WindowTuple]]) -> str:
    """Return the appropriate ORDER BY clause for the detected windows."""

    if order_by_from_user:
        return order_by_from_user
    if windows.get("expiring"):
        return "ORDER BY END_DATE ASC"
    return "ORDER BY REQUEST_DATE DESC"


__all__ = ["apply_time_windows", "choose_order_by"]

