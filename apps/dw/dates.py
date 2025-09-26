"""Utilities for coercing inbound values into proper Oracle bind types."""
from __future__ import annotations

from datetime import date, datetime

# Try a few common formats; fall back to strict ISO.
_CANDIDATE_FMTS = ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y")


def coerce_oracle_date(val) -> date | None:
    """Return a Python date for a given value (str/date/datetime), or None."""
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    for fmt in _CANDIDATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # Last resort: ISO parse (YYYY-MM-DD)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unrecognized date literal for Oracle bind: {val!r}") from exc
