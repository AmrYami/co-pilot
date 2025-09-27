from __future__ import annotations
from typing import Dict, Any, List
from datetime import date, datetime
from dateutil.parser import isoparse


def ensure_oracle_date_binds(binds: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce :date_start / :date_end to datetime.date for Oracle."""
    out = dict(binds or {})
    for k in ("date_start", "date_end"):
        v = out.get(k)
        if v is None:
            continue
        if isinstance(v, date) and not isinstance(v, datetime):
            continue
        if isinstance(v, datetime):
            out[k] = v.date()
            continue
        if isinstance(v, str):
            # ISO-like → date
            try:
                out[k] = isoparse(v).date()
            except Exception:
                # fallback: YYYY-MM-DD
                y, m, d = v.split("-")
                out[k] = date(int(y), int(m), int(d))
    return out


def get_fts_columns(settings, table: str) -> List[str]:
    mapping = settings.get("DW_FTS_COLUMNS", {}) or {}
    if isinstance(mapping, dict):
        if table in mapping:
            return mapping[table]
        return mapping.get("*", [])
    return []


def compose_explain(intent, binds: Dict[str, Any]) -> str:
    """Human-readable short note (kept English-only inside code)."""
    parts: List[str] = []
    # time window
    if intent.explicit_dates:
        s = intent.explicit_dates.get("start")
        e = intent.explicit_dates.get("end")
        if s and e:
            parts.append(f"Window: {s} → {e}.")
    # date column mode
    if intent.date_column == "REQUEST_DATE":
        parts.append("Window applied on REQUEST_DATE.")
    elif intent.date_column == "END_DATE":
        parts.append("Window applied on END_DATE (expiry).")
    else:
        parts.append("Window interpreted as active-overlap (START_DATE..END_DATE).")
    # grouping / measure
    if intent.group_by:
        parts.append(f"Grouped by {intent.group_by}.")
    if intent.agg:
        parts.append(f"Aggregation: {intent.agg.upper()}.")
    # sorting & top
    if intent.top_n:
        parts.append(f"Returning top {intent.top_n} rows.")
    if intent.sort_by:
        parts.append(f"Sorted by {intent.sort_by} {'DESC' if intent.sort_desc else 'ASC'}.")
    # projection
    parts.append("Columns: all." if intent.wants_all_columns and not intent.group_by else "Columns: aggregated projection.")
    return " ".join(parts)
