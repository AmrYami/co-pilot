from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any


@dataclass
class QuestionSpec:
    """Structured representation of a user's question."""
    intent: str = "sql_request"  # sql_request | raw_sql | smalltalk | help | ambiguous
    datasource: Optional[str] = None
    prefixes: List[str] = field(default_factory=list)

    # canonical analysis fields (domain-agnostic)
    date_column: Optional[str] = None
    date_range: Optional[str] = None
    entity: Optional[str] = None
    tables: List[str] = field(default_factory=list)
    metric_key: Optional[str] = None
    metric_expr: Optional[str] = None
    group_by: List[str] = field(default_factory=list)
    top_k: Optional[int] = None
    filters: List[str] = field(default_factory=list)

    # LLM confidence or notes
    notes: List[str] = field(default_factory=list)

    def missing_fields(self) -> List[str]:
        missing: List[str] = []
        if not self.date_range:
            missing.append("date_range")
        if not self.entity and (self.top_k or (self.group_by and len(self.group_by) == 1)):
            missing.append("entity")
        if not self.metric_key and not self.metric_expr:
            missing.append("metric")
        if not self.tables:
            missing.append("tables")
        return missing

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def merge_specs(base: QuestionSpec, patch: QuestionSpec) -> QuestionSpec:
    """Merge two QuestionSpec objects, preferring explicit values from `patch`."""
    out = QuestionSpec(**base.as_dict())
    for fld in (
        "intent",
        "datasource",
        "date_column",
        "date_range",
        "entity",
        "metric_key",
        "metric_expr",
        "top_k",
    ):
        val = getattr(patch, fld)
        if val:
            setattr(out, fld, val)
    for lf in ("prefixes", "tables", "group_by", "filters", "notes"):
        lst = list(getattr(out, lf) or [])
        for v in getattr(patch, lf) or []:
            if v and v not in lst:
                lst.append(v)
        setattr(out, lf, lst)
    return out
