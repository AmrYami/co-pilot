from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal

DateCol = Literal["END_DATE", "REQUEST_DATE", "START_DATE"]


@dataclass
class TimeWindow:
    start: Optional[str] = None  # ISO YYYY-MM-DD
    end: Optional[str] = None
    inferred: bool = False
    column: Optional[DateCol] = None


@dataclass
class NLIntent:
    has_time_window: Optional[bool] = None
    time_window: Optional[TimeWindow] = None
    top_n: Optional[int] = None
    agg: Optional[Literal["count", "sum", "avg", "min", "max"]] = None
    group_by: Optional[str] = None  # e.g., OWNER_DEPARTMENT
    sort_by: Optional[str] = None
    sort_desc: bool = True
    wants_all_columns: bool = False
