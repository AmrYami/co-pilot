from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import date


@dataclass
class NLIntent:
    raw: str
    # temporal
    has_time_window: Optional[bool] = None
    explicit_dates: Optional[Dict[str, date]] = None  # {"start": date, "end": date}
    date_column: Optional[str] = None  # "OVERLAP" | "REQUEST_DATE" | "END_DATE"
    expire: Optional[bool] = None
    # scope & projection
    wants_all_columns: bool = True
    full_text_search: bool = False
    fts_tokens: Optional[List[str]] = None
    # ranking & grouping
    top_n: Optional[int] = None
    user_requested_top_n: Optional[bool] = None
    group_by: Optional[str] = None  # e.g., OWNER_DEPARTMENT | CONTRACT_STATUS | STAKEHOLDER_UNION
    # measures
    agg: Optional[str] = None       # "sum" | "count" | "avg"
    measure_sql: Optional[str] = None  # SQL expression (gross/net)
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    # notes (debug)
    notes: Optional[Dict] = None
