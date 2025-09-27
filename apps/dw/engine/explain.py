from __future__ import annotations

from .models import NLIntent


def build_explain(intent: NLIntent) -> str:
    parts = []
    if intent.explicit_dates:
        start = intent.explicit_dates.get("start")
        end = intent.explicit_dates.get("end")
        if start and end:
            parts.append(f"Interpreted time window as {start} → {end}.")
    if intent.date_column == "OVERLAP":
        parts.append("Treated contracts as active when START_DATE/END_DATE overlap the requested window.")
    elif intent.date_column:
        parts.append(f"Used {intent.date_column} as the date basis.")
    if intent.group_by:
        parts.append(f"Grouped by {intent.group_by}.")
    if intent.agg:
        parts.append(f"Aggregation: {intent.agg}.")
    if intent.sort_by:
        order_word = "descending" if intent.sort_desc else "ascending"
        parts.append(f"Sorted by {intent.sort_by} in {order_word} order.")
    if intent.full_text_search:
        parts.append("Enabled full-text search across the configured columns.")
    if intent.wants_all_columns and not intent.group_by and not intent.agg:
        parts.append("Returned all columns because none were specified.")
    return " ".join(parts)
