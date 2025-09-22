import re
from dataclasses import dataclass
from typing import Optional

DIMENSION_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "stakeholders": "CONTRACT_STAKEHOLDER_1",
}

WINDOW_HINTS = [
    (r"\blast\s+month\b", ("last_month", None)),
    (r"\blast\s+3\s+months?\b", ("last_3_months", None)),
    (r"\blast\s+quarter\b", ("last_quarter", None)),
    (r"\bnext\s+(\d+)\s+days\b", ("next_n_days", "END_DATE")),
    (r"\bexpir\w+\b", (None, "END_DATE")),
]


@dataclass
class DWIntent:
    agg: Optional[str] = None  # 'count' | 'sum' | None
    dimension: Optional[str] = None  # mapped DB column
    measure: str = "gross"  # 'gross'|'net'
    user_requested_top_n: bool = False
    top_n: Optional[int] = None
    date_column: Optional[str] = None  # REQUEST_DATE | END_DATE | START_DATE
    window_key: Optional[str] = None  # 'last_month' | 'last_3_months' | 'last_quarter' | 'next_n_days'
    wants_all_columns: bool = False
    window_param: Optional[int] = None  # e.g., number of days for next_n_days


def extract_intent(q: str) -> DWIntent:
    t = (q or "").strip().lower()
    intent = DWIntent()

    # count?
    if "count" in t or "(count)" in t:
        intent.agg = "count"

    # by/per <dimension>
    m = re.search(r"\b(?:by|per)\s+([a-z\s_]+)", t)
    if m:
        key = m.group(1).strip()
        for k, col in DIMENSION_MAP.items():
            if k in key:
                intent.dimension = col
                break

    # “top N …”
    m = re.search(r"\btop\s+(\d+)\b", t)
    if m:
        intent.user_requested_top_n = True
        intent.top_n = int(m.group(1))

    # gross vs net
    if "gross" in t:
        intent.measure = "gross"
    elif "net" in t:
        intent.measure = "net"

    # date hints
    for pat, (wkey, force_col) in WINDOW_HINTS:
        mm = re.search(pat, t)
        if mm:
            if wkey == "next_n_days":
                intent.window_key = "next_n_days"
                try:
                    intent.window_param = int(mm.group(1))
                except (ValueError, IndexError, TypeError):
                    intent.window_param = None
                intent.date_column = force_col or intent.date_column
            else:
                intent.window_key = wkey
            if force_col:
                intent.date_column = force_col
            break

    # default date column
    if intent.date_column is None:
        intent.date_column = "REQUEST_DATE"

    # wants all columns only if not aggregating and no dimension
    if intent.agg is None and intent.dimension is None:
        if any(w in t for w in ["list", "show", "contracts with", "all columns"]):
            intent.wants_all_columns = True

    return intent
