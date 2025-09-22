"""Compose a lightweight NL intent from raw user text."""

from __future__ import annotations

from .dates import parse_time_window
from .number import extract_top_n
from .slots import extract_group_by, wants_count
from .types import NLIntent, TimeWindow


def infer_intent(
    text: str,
    default_date_col: str = "REQUEST_DATE",
    all_columns_default: bool = True,
) -> NLIntent:
    top_n = extract_top_n(text)
    time_window_data, inferred = parse_time_window(text, default_col=default_date_col)
    group_by = extract_group_by(text)

    has_window = bool(time_window_data.get("start") and time_window_data.get("end"))

    time_window = TimeWindow(
        start=time_window_data.get("start"),
        end=time_window_data.get("end"),
        inferred=inferred,
        column=time_window_data.get("column"),
    )

    intent = NLIntent(
        has_time_window=has_window,
        time_window=time_window if any([time_window.start, time_window.end]) else None,
        top_n=top_n,
        group_by=group_by,
        wants_all_columns=all_columns_default,
    )

    if wants_count(text):
        intent.agg = "count"

    return intent
