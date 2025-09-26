from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, date


def _fmt_date(d: Any) -> str:
    """Format ISO 'YYYY-MM-DD', datetime/date, or passthrough."""
    if d is None:
        return "?"
    if isinstance(d, (datetime, date)):
        return d.strftime("%Y-%m-%d")
    s = str(d).strip()
    # accept 'YYYY-MM-DD' or any string, keep short
    if len(s) > 32:
        s = s[:32] + "…"
    return s


def build_explanation(
    intent: Dict[str, Any],
    binds: Dict[str, Any],
    fts_meta: Optional[Dict[str, Any]],
    table: str,
    cols_selected: Optional[List[str]],
    strategy: str,
    default_date_basis: str = "REQUEST_DATE",
) -> str:
    """
    Turn internal intent + execution metadata into a short, user-facing explanation.
    Safe to call with partial info: missing keys are handled gracefully.
    """
    parts: List[str] = []

    # --- Time window ---
    ds = binds.get("date_start") or (intent.get("explicit_dates") or {}).get("start")
    de = binds.get("date_end") or (intent.get("explicit_dates") or {}).get("end")
    date_col = intent.get("date_column")
    if ds and de:
        if (date_col or "").upper() == "OVERLAP":
            parts.append(
                f"Interpreting time window as {_fmt_date(ds)} → {_fmt_date(de)}; "
                "selecting contracts active by overlap "
                "(START_DATE ≤ end AND END_DATE ≥ start)."
            )
        elif date_col:
            parts.append(
                f"Filtering {date_col} between {_fmt_date(ds)} and {_fmt_date(de)}."
            )
        else:
            parts.append(
                f"Using default date basis {default_date_basis} between "
                f"{_fmt_date(ds)} and {_fmt_date(de)}."
            )
    else:
        # No dates detected
        if date_col:
            parts.append(f"No explicit dates; date basis is {date_col}.")
        else:
            parts.append("No time window filter detected.")

    # --- Grouping & measure ---
    group_by = intent.get("group_by")
    agg = (intent.get("agg") or "").lower() or None
    measure = intent.get("measure_sql")
    if group_by:
        parts.append(f"Grouping by {group_by}.")
    if agg == "count":
        parts.append("Measuring COUNT(*).")
    elif agg and measure:
        parts.append(f"Measuring {agg.upper()} of {measure}.")
    elif agg:
        parts.append(f"Applying {agg.upper()} aggregation.")
    else:
        parts.append("No aggregation requested.")

    # --- Sorting & Top N ---
    sort_by = intent.get("sort_by")
    sort_desc = bool(intent.get("sort_desc"))
    top_n = intent.get("top_n")
    if sort_by:
        parts.append(f"Sorting by {sort_by} {'descending' if sort_desc else 'ascending'}.")
    if top_n:
        parts.append(f"Returning top {top_n} rows.")

    # --- Projection ---
    if intent.get("wants_all_columns"):
        parts.append("Returning all columns.")
    elif cols_selected:
        shown = ", ".join(cols_selected[:8]) + ("…" if len(cols_selected) > 8 else "")
        parts.append(f"Returning selected columns: {shown}.")

    # --- Full-text search ---
    if intent.get("full_text_search"):
        tokens = intent.get("fts_tokens") or []
        cols = (fts_meta or {}).get("columns") or []
        t_str = ", ".join(tokens[:5]) + ("…" if len(tokens) > 5 else "")
        c_str = ", ".join(cols[:8]) + ("…" if len(cols) > 8 else "")
        if tokens and cols:
            parts.append(f"Full‑text search enabled ({t_str}); scanning: {c_str}.")
        elif tokens:
            parts.append(f"Full‑text search enabled ({t_str}).")
        else:
            parts.append("Full‑text search enabled.")

    # --- Planner strategy ---
    if strategy:
        parts.append(f"Planner: {strategy}.")

    # --- Table mention (only if useful) ---
    if table:
        parts.append(f"Table: {table}.")

    # Final sentence.
    return " ".join(parts)
