from __future__ import annotations
import os
from datetime import date, timedelta
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

def _start_of_year(offset_or_year: Union[int, str, None]) -> date:
    t = _today()
    if offset_or_year is None:
        y = t.year
    else:
        # If explicit year like "2023", use as absolute. Otherwise treat as offset.
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

def _start_of_quarter(offset: Union[int, str, None]) -> date:
    t = _today()
    base = _first_day_of_quarter(t)
    k = 0 if offset is None else _parse_int(offset, 0)
    return base + relativedelta(months=3 * k)

def _end_of_quarter(offset: Union[int, str, None]) -> date:
    s = _start_of_quarter(offset)
    # end = start_of_next_quarter - 1 day
    nxt = s + relativedelta(months=3)
    return nxt - timedelta(days=1)

def _days_ago(n: Union[int, str]) -> date:
    return _today() - timedelta(days=_parse_int(n))

def _days_ahead(n: Union[int, str]) -> date:
    return _today() + timedelta(days=_parse_int(n))

def _quarter_ago(n: Union[int, str]) -> date:
    return _today() - relativedelta(months=3 * _parse_int(n))


# --- YAML constructors ---
class GoldenLoader(yaml.SafeLoader):
    """Custom loader to support temporal tags for golden tests."""

def _construct_scalar(loader: GoldenLoader, node: yaml.Node) -> Any:
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    return loader.construct_object(node)

def construct_today(loader: GoldenLoader, node: yaml.Node) -> date:
    _ = _construct_scalar(loader, node)  # ignore payload if any
    return _today()

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


def register_yaml_tags() -> None:
    """Register all custom YAML tags on the GoldenLoader."""
    yaml.add_constructor("!today",             construct_today,            Loader=GoldenLoader)
    yaml.add_constructor("!days_ago",          construct_days_ago,         Loader=GoldenLoader)
    yaml.add_constructor("!days_ahead",        construct_days_ahead,       Loader=GoldenLoader)
    yaml.add_constructor("!start_of_year",     construct_start_of_year,    Loader=GoldenLoader)
    yaml.add_constructor("!end_of_year",       construct_end_of_year,      Loader=GoldenLoader)
    yaml.add_constructor("!start_of_quarter",  construct_start_of_quarter, Loader=GoldenLoader)
    yaml.add_constructor("!end_of_quarter",    construct_end_of_quarter,   Loader=GoldenLoader)
    yaml.add_constructor("!quarter_ago",       construct_quarter_ago,      Loader=GoldenLoader)
