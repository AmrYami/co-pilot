from __future__ import annotations

from typing import Dict, Literal, Optional

try:  # pragma: no cover - optional dependency
    from pydantic import BaseModel, Field  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when pydantic missing
    class _FieldSpec:
        def __init__(self, *, default_factory=None):
            self.default_factory = default_factory

    def Field(default_factory=None):  # type: ignore
        return _FieldSpec(default_factory=default_factory)

    class BaseModel:  # minimal stub implementing what we need
        def __init__(self, **data):
            for name, annotation in getattr(self, "__annotations__", {}).items():
                default = getattr(self.__class__, name, None)
                if isinstance(default, _FieldSpec):
                    value = default.default_factory() if default.default_factory else None
                else:
                    value = default
                object.__setattr__(self, name, value)
            for key, value in data.items():
                object.__setattr__(self, key, value)

        def model_dump(self, exclude_none: bool = False):
            result = {}
            for name in getattr(self, "__annotations__", {}):
                value = getattr(self, name, None)
                if exclude_none and value is None:
                    continue
                if isinstance(value, BaseModel):
                    result[name] = value.model_dump(exclude_none=exclude_none)
                else:
                    result[name] = value
            return result

        def __setattr__(self, key, value):
            object.__setattr__(self, key, value)

Agg = Literal["count", "sum", "avg", "min", "max"]


class TimeWindow(BaseModel):
    start: Optional[str] = None  # ISO-8601 date
    end: Optional[str] = None


class NLIntent(BaseModel):
    # time / filters
    has_time_window: Optional[bool] = None
    date_column: Optional[str] = None  # e.g. END_DATE | REQUEST_DATE | START_DATE
    explicit_dates: Optional[TimeWindow] = None

    # aggregation / shape
    agg: Optional[Agg] = None
    group_by: Optional[str] = None  # resolved column name
    wants_all_columns: Optional[bool] = None

    # ranking / bounds
    top_n: Optional[int] = None
    sort_by: Optional[str] = None  # resolved column or expression
    sort_desc: Optional[bool] = None
    user_requested_top_n: Optional[bool] = None  # true only if user explicitly asked

    # measures / semantics
    measure_sql: Optional[str] = None  # e.g. NVL(CONTRACT_VALUE_NET_OF_VAT,0) ...
    notes: Dict[str, str] = Field(default_factory=dict)
