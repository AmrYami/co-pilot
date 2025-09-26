from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class NLIntent(BaseModel):
    question: str
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, str]] = None
    top_n: Optional[int] = None
    agg: Optional[str] = None
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: bool = True
    wants_all_columns: bool = True
    user_requested_top_n: Optional[bool] = None
    full_text_search: bool = False
    fts_tokens: List[str] = Field(default_factory=list)
    filters: Dict[str, str] = Field(default_factory=dict)
    expire: Optional[bool] = None
    measure_sql: Optional[str] = None
    explain_on: bool = True
    date_basis: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
