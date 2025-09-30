"""Intent parsing heuristics for DocuWare Contract questions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DWIntent:
    """Structured interpretation of a natural-language DW question."""

    question: str
    agg: Optional[str] = None
    group_by: Optional[str] = None
    sort_by: Optional[str] = None
    sort_desc: Optional[bool] = None
    top_n: Optional[int] = None
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None
    explicit_dates: Optional[Dict[str, date]] = None
    measure_sql: Optional[str] = None
    notes: Dict[str, Any] = field(default_factory=dict)
    wants_all_columns: bool = True
    # signals
    is_bottom: bool = False
    by_dimension_hint: Optional[str] = None
    extra_filters: List[Dict[str, Any]] = field(default_factory=list)


_BOTTOM_RE = re.compile(r"\b(bottom|lowest|least|smallest|min)\b", re.I)
_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.I)
_BOTTOM_TOP_RE = re.compile(r"\b(?:bottom|lowest|least|smallest|min)\s+(\d+)\b", re.I)
_GROUP_HINT_RE = re.compile(r"\b(?:per|by)\b", re.I)
_DIMENSION_PATTERNS: Dict[str, str] = {
    r"(owner\s*department|owner_department|owner\s+dept)": "OWNER_DEPARTMENT",
    r"(department_oul|oul\b)": "DEPARTMENT_OUL",
    r"(entity_no|entity\s*no|entityno)": "ENTITY_NO",
    r"\bentity\b": "ENTITY",
}

_DIRECT_EQ_PATTERNS: Tuple[Tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bREQUEST\s*TYPE\s*=\s*([\"']?)([A-Za-z0-9 _\-]+)\1", re.IGNORECASE), "REQUEST_TYPE"),
)


def _extract_direct_equality_filters(q: str) -> List[Tuple[str, str]]:
    """Return list of (column, value) for simple equality phrases in the question."""
    results: List[Tuple[str, str]] = []
    if not q:
        return results
    for pattern, column in _DIRECT_EQ_PATTERNS:
        match = pattern.search(q)
        if match:
            value = match.group(2).strip()
            if value:
                results.append((column, value))
    return results


def parse_intent(question: str) -> DWIntent:
    """Parse natural-language question into a :class:`DWIntent`."""

    q = (question or "").strip()
    intent = DWIntent(question=q)

    if not q:
        return intent

    lowered = q.lower()

    # Aggregation cues
    if re.search(r"\bcount\b", lowered):
        intent.agg = "count"
        intent.wants_all_columns = False
    elif re.search(r"\baverage|avg\b", lowered):
        intent.agg = "avg"
    elif re.search(r"\b(sum|total|amount)\b", lowered):
        intent.agg = "sum"

    # measure selection
    if re.search(r"\bgross\b", lowered):
        intent.measure_sql = (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) "
            "BETWEEN 0 AND 1 THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END"
        )
    elif re.search(r"\bnet\b", lowered):
        intent.measure_sql = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

    # detect top/bottom N
    m = _TOP_RE.search(q)
    if m:
        intent.top_n = int(m.group(1))
        intent.sort_desc = True

    if _BOTTOM_RE.search(q):
        m2 = _BOTTOM_TOP_RE.search(q)
        if m2:
            intent.top_n = int(m2.group(1))
        intent.is_bottom = True
        intent.sort_desc = False

    # GROUP BY hints
    if _GROUP_HINT_RE.search(q):
        for pattern, dim in _DIMENSION_PATTERNS.items():
            if re.search(pattern, q, re.I):
                intent.group_by = dim
                intent.by_dimension_hint = dim
                break

    # window hints
    if "requested" in lowered:
        intent.date_column = "REQUEST_DATE"
    elif re.search(r"\b(expir(?:e|y|ing)|end\s*date)\b", lowered):
        intent.date_column = "END_ONLY"
    else:
        intent.date_column = "OVERLAP"

    # Year-to-date handling
    if re.search(r"\b(ytd|year\s*to\s*date)\b", lowered):
        intent.notes["ytd"] = True
        intent.date_column = "OVERLAP"

    # simple cues for time windows
    if re.search(r"\blast\s+month\b", lowered):
        intent.has_time_window = True
    elif re.search(r"\bnext\s+\d+\s+days\b", lowered):
        intent.has_time_window = True
    elif re.search(r"\blast\s+\d+\s+months\b", lowered):
        intent.has_time_window = True

    direct_eq = _extract_direct_equality_filters(q)
    if direct_eq:
        for idx, (column, value) in enumerate(direct_eq, start=1):
            bind_name = f"eq_{column.lower()}_{idx}"
            intent.extra_filters.append(
                {
                    "col": column,
                    "op": "eq_ci",
                    "bind": bind_name,
                    "value": value,
                }
            )
    elif re.search(r"\brenew(al|ed)?\b", q, flags=re.IGNORECASE):
        intent.extra_filters.append(
            {
                "col": "REQUEST_TYPE",
                "op": "like_ci",
                "bind": "req_like",
                "value": "%renew%",
            }
        )

    return intent
