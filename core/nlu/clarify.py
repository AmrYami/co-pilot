"""Compose a lightweight NL intent from raw user text."""

from __future__ import annotations

from .dates import parse_time_window
from .number import extract_top_n
from .slots import extract_group_by, wants_count
from .schema import NLIntent, TimeWindow


def infer_intent(
    text: str,
    default_date_col: str = "REQUEST_DATE",
    all_columns_default: bool = True,
) -> NLIntent:
    top_n = extract_top_n(text)
    time_window_data, inferred = parse_time_window(text, default_col=default_date_col)
    group_by = extract_group_by(text)

    has_window = bool(time_window_data.get("start") and time_window_data.get("end"))

    explicit = None
    if any([time_window_data.get("start"), time_window_data.get("end")]):
        explicit = TimeWindow(
            start=time_window_data.get("start"),
            end=time_window_data.get("end"),
        )

    intent = NLIntent(
        has_time_window=has_window if inferred else None,
        explicit_dates=explicit,
        top_n=top_n,
        group_by=group_by,
        wants_all_columns=all_columns_default,
        date_column=time_window_data.get("column") or default_date_col,
    )

    if wants_count(text):
        intent.agg = "count"

    return intent
