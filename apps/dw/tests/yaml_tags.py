from __future__ import annotations
import os
from datetime import date, datetime, timedelta
from typing import Any, Union

import yaml
try:
    from dateutil.relativedelta import relativedelta
except Exception as ex:
    raise RuntimeError(
        "python-dateutil is required for golden YAML tags. "
        "pip install python-dateutil"
    ) from ex


def _parse_int(node_value: Any, default: int = 0) -> int:
    if isinstance(node_value, int):
        return node_value
    if isinstance(node_value, str):
        s = node_value.strip()
        # allow forms like "N=90"
        if "=" in s:
            _, v = s.split("=", 1)
            s = v.strip()
        return int(s)
    raise ValueError(f"Expected integer-like YAML scalar, got: {node_value!r}")

def _today() -> date:
    """Allow freezing 'today' for stable tests via GOLDEN_TODAY=YYYY-MM-DD."""
    env_v = os.getenv("GOLDEN_TODAY")
    if env_v:
        return date.fromisoformat(env_v)
    return date.today()

def _first_day_of_quarter(d: date) -> date:
    q = (d.month - 1) // 3  # 0..3
    first_month = q * 3 + 1
    return d.replace(month=first_month, day=1)


def _maybe_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return _iso_date(value)
    except Exception:
        return None


def _start_of_month(value: Union[int, str, date, datetime, None]) -> date:
    base = _today().replace(day=1)
    if value is None:
        return base
    candidate = _maybe_date(value)
    if candidate:
        return candidate.replace(day=1)
    try:
        offset = _parse_int(value, 0)
    except Exception:
        offset = 0
    return base + relativedelta(months=offset)


def _end_of_month(value: Union[int, str, date, datetime, None]) -> date:
    start = _start_of_month(value)
    next_month = start + relativedelta(months=1)
    return next_month - timedelta(days=1)


def _start_of_last_month() -> date:
    return _start_of_month(-1)


def _end_of_last_month() -> date:
    return _end_of_month(-1)

def _start_of_year(offset_or_year: Union[int, str, date, datetime, None]) -> date:
    t = _today()
    if offset_or_year is None:
        y = t.year
    else:
        date_candidate = _maybe_date(offset_or_year)
        if date_candidate:
            y = date_candidate.year
        else:
            try:
                val = int(str(offset_or_year).strip())
            except Exception:
                val = 0
            if val >= 1900:
                y = val
            else:
                y = (t + relativedelta(years=val)).year
    return date(y, 1, 1)

def _end_of_year(offset_or_year: Union[int, str, None]) -> date:
    s = _start_of_year(offset_or_year)
    return s.replace(month=12, day=31)

def _start_of_quarter(value: Union[int, str, date, datetime, None]) -> date:
    base = _first_day_of_quarter(_today())
    if value is None:
        return base
    candidate = _maybe_date(value)
    if candidate:
        return _first_day_of_quarter(candidate)
    try:
        k = _parse_int(value, 0)
    except Exception:
        k = 0
    return base + relativedelta(months=3 * k)

def _end_of_quarter(value: Union[int, str, date, datetime, None]) -> date:
    s = _start_of_quarter(value)
    # end = start_of_next_quarter - 1 day
    nxt = s + relativedelta(months=3)
    return nxt - timedelta(days=1)

def _days_ago(n: Union[int, str]) -> date:
    return _today() - timedelta(days=_parse_int(n))

def _days_ahead(n: Union[int, str]) -> date:
    return _today() + timedelta(days=_parse_int(n))

def _quarter_ago(n: Union[int, str]) -> date:
    base = _today().replace(day=1)
    current_q_start_month = ((base.month - 1) // 3) * 3 + 1
    current_q_start = date(base.year, current_q_start_month, 1)
    return current_q_start - relativedelta(months=3 * _parse_int(n))


def _months_ago(n: Union[int, str]) -> date:
    return _today() - relativedelta(months=_parse_int(n))


def _months_ahead(n: Union[int, str]) -> date:
    return _today() + relativedelta(months=_parse_int(n))


def _iso_date(value: Union[str, date]) -> date:
    if isinstance(value, date):
        return value
    s = str(value).strip()
    try:
        return date.fromisoformat(s)
    except ValueError:
        return datetime.fromisoformat(s).date()


def _parse_optional_int(value: Any, default: int = 1) -> int:
    if value is None:
        return default
    try:
        return _parse_int(value, default)
    except Exception:
        return default


# --- YAML constructors ---
class GoldenLoader(yaml.SafeLoader):
    """Custom loader to support temporal tags for golden tests."""

def _construct_scalar(loader: GoldenLoader, node: yaml.Node) -> Any:
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    return loader.construct_object(node)


def _normalise_optional(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def construct_today(loader: GoldenLoader, node: yaml.Node) -> date:
    _ = _construct_scalar(loader, node)  # ignore payload if any
    return _today()


def construct_start_of_month(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _normalise_optional(_construct_scalar(loader, node))
    return _start_of_month(val)


def construct_end_of_month(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _normalise_optional(_construct_scalar(loader, node))
    return _end_of_month(val)


def construct_start_of_last_month(loader: GoldenLoader, node: yaml.Node) -> date:
    _ = _construct_scalar(loader, node)
    return _start_of_last_month()


def construct_end_of_last_month(loader: GoldenLoader, node: yaml.Node) -> date:
    _ = _construct_scalar(loader, node)
    return _end_of_last_month()


def construct_days_ago(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _days_ago(val)

def construct_days_ahead(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _days_ahead(val)

def construct_start_of_year(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _start_of_year(val)

def construct_end_of_year(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _end_of_year(val)

def construct_start_of_quarter(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _start_of_quarter(val)

def construct_end_of_quarter(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _end_of_quarter(val)

def construct_quarter_ago(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _quarter_ago(val)


def construct_months_ago(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _months_ago(val)


def construct_months_ahead(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _months_ahead(val)


def construct_iso(loader: GoldenLoader, node: yaml.Node) -> date:
    val = _construct_scalar(loader, node)
    return _iso_date(val)


def _multi_start_dispatch(suffix: str, payload: Any) -> date:
    s = (suffix or "").strip().lower().replace("-", "_")
    if s in ("", "month", "months", "this_month"):
        return _start_of_month(payload)
    if s in ("last_month", "previous_month"):
        return _start_of_last_month()
    if s in ("year", "years"):
        return _start_of_year(payload)
    if s in ("last_year", "previous_year"):
        return _start_of_year(-1)
    if s in ("quarter", "quarters"):
        return _start_of_quarter(payload)
    if s in ("last_quarter", "previous_quarter"):
        return _start_of_quarter(-1)
    if s in ("prev_months", "previous_months", "months_ago", "month_ago"):
        return _start_of_month(-_parse_optional_int(payload, 1))
    if s in ("prev_years", "previous_years", "years_ago", "year_ago"):
        return _start_of_year(-_parse_optional_int(payload, 1))
    if s in ("prev_quarters", "previous_quarters", "quarters_ago", "quarter_ago"):
        return _start_of_quarter(-_parse_optional_int(payload, 1))
    # Fallback: interpret payload as explicit base date or default to current month start.
    try:
        return _start_of_month(payload)
    except Exception:
        return _start_of_month(None)


def _multi_end_dispatch(suffix: str, payload: Any) -> date:
    s = (suffix or "").strip().lower().replace("-", "_")
    if s in ("", "month", "months", "this_month"):
        return _end_of_month(payload)
    if s in ("last_month", "previous_month"):
        return _end_of_last_month()
    if s in ("year", "years"):
        return _end_of_year(payload)
    if s in ("last_year", "previous_year"):
        return _end_of_year(-1)
    if s in ("quarter", "quarters"):
        return _end_of_quarter(payload)
    if s in ("last_quarter", "previous_quarter"):
        return _end_of_quarter(-1)
    if s in ("prev_months", "previous_months", "months_ago", "month_ago"):
        return _end_of_month(-_parse_optional_int(payload, 1))
    if s in ("prev_years", "previous_years", "years_ago", "year_ago"):
        return _end_of_year(-_parse_optional_int(payload, 1))
    if s in ("prev_quarters", "previous_quarters", "quarters_ago", "quarter_ago"):
        return _end_of_quarter(-_parse_optional_int(payload, 1))
    try:
        return _end_of_month(payload)
    except Exception:
        return _end_of_month(None)


def construct_start_multi(loader: GoldenLoader, tag_suffix: str, node: yaml.Node) -> date:
    payload = _normalise_optional(_construct_scalar(loader, node))
    return _multi_start_dispatch(tag_suffix, payload)


def construct_end_multi(loader: GoldenLoader, tag_suffix: str, node: yaml.Node) -> date:
    payload = _normalise_optional(_construct_scalar(loader, node))
    return _multi_end_dispatch(tag_suffix, payload)


def register_yaml_tags() -> None:
    """Register all custom YAML tags on the GoldenLoader."""
    yaml.add_constructor("!today",               construct_today,              Loader=GoldenLoader)
    yaml.add_constructor("!start_of_month",      construct_start_of_month,      Loader=GoldenLoader)
    yaml.add_constructor("!end_of_month",        construct_end_of_month,        Loader=GoldenLoader)
    yaml.add_constructor("!start_of_last_month", construct_start_of_last_month, Loader=GoldenLoader)
    yaml.add_constructor("!end_of_last_month",   construct_end_of_last_month,   Loader=GoldenLoader)
    yaml.add_constructor("!days_ago",            construct_days_ago,            Loader=GoldenLoader)
    yaml.add_constructor("!days_ahead",          construct_days_ahead,          Loader=GoldenLoader)
    yaml.add_constructor("!months_ago",          construct_months_ago,          Loader=GoldenLoader)
    yaml.add_constructor("!months_ahead",        construct_months_ahead,        Loader=GoldenLoader)
    yaml.add_constructor("!start_of_year",       construct_start_of_year,       Loader=GoldenLoader)
    yaml.add_constructor("!end_of_year",         construct_end_of_year,         Loader=GoldenLoader)
    yaml.add_constructor("!start_of_quarter",    construct_start_of_quarter,    Loader=GoldenLoader)
    yaml.add_constructor("!end_of_quarter",      construct_end_of_quarter,      Loader=GoldenLoader)
    yaml.add_constructor("!quarter_ago",         construct_quarter_ago,         Loader=GoldenLoader)
    yaml.add_constructor("!iso",                 construct_iso,                 Loader=GoldenLoader)
    yaml.add_multi_constructor("!start_of_",      construct_start_multi,         Loader=GoldenLoader)
    yaml.add_multi_constructor("!end_of_",        construct_end_multi,           Loader=GoldenLoader)
