from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import re

from apps.dw.sql_utils import resolve_group_by


@dataclass
class EqFilter:
    col: str
    val: str
    ci: bool = True
    trim: bool = True
    op: str = "eq"


@dataclass
class RateHints:
    fts_tokens: List[str] = field(default_factory=list)
    fts_operator: str = "OR"  # OR | AND
    order_by: Optional[Tuple[str, bool]] = None  # (column, desc)
    eq_filters: List[EqFilter] = field(default_factory=list)
    group_by: Optional[str] = None
    gross: Optional[bool] = None


_RE_ORDER_BY = re.compile(r"order_by\s*:\s*([A-Za-z0-9_]+)\s+(asc|desc)", re.I)
_RE_FTS = re.compile(r"fts?\s*:\s*(.+)$", re.I)
_RE_EQ = re.compile(
    r"filter\s*:\s*([A-Za-z0-9_]+)\s*=\s*'?([^';\)]+?)'?\s*(\((.*?)\))?",
    re.I,
)
_RE_GROUP_BY = re.compile(r"group_by\s*:\s*([A-Za-z0-9_\- ]+)", re.I)
_RE_GROSS = re.compile(r"gross\s*:\s*(true|false)", re.I)

def _split_fts_tokens(s: str) -> Tuple[List[str], str]:
    """
    Split a free-form token string into a clean list.
    Accepts separators: '|', ',', ';', ' or ', ' OR ', ' and ' (rare).
    """
    # Normalize common textual separators to '|'
    has_and = bool(re.search(r"\band\b", s, flags=re.I))
    s = re.sub(r"\s+or\s+", "|", s, flags=re.I)
    s = re.sub(r"\s+and\s+", "|", s, flags=re.I)
    # Replace commas/semicolons with '|'
    s = s.replace(",", "|").replace(";", "|")
    # Remove quotes if user added any
    s = s.replace('"', '').replace("'", "")
    # Detect AND vs OR
    op = "AND" if has_and else "OR"
    parts = [p.strip() for p in re.split(r"[|]", s) if p.strip()]
    return parts, op


def parse_rate_comment(comment: str) -> RateHints:
    """
    Parse a free-form 'comment' coming from /dw/rate into a structured object.
    Supported hints (case-insensitive):
      - fts: token1 | token2
      - order_by: COLUMN asc|desc
      - filter: COL = VALUE (ci, trim)
    Multiple instructions can be separated by ';'
    """
    hints = RateHints()
    if not comment:
        return hints

    # Split comment by ';' into clauses for easier parsing
    clauses = [c.strip() for c in comment.split(";") if c.strip()]
    for clause in clauses:
        # ORDER BY
        m = _RE_ORDER_BY.search(clause)
        if m:
            hints.order_by = (m.group(1).upper(), m.group(2).lower() == "desc")
            continue

        # FTS
        m = _RE_FTS.search(clause)
        if m:
            tokens_str = m.group(1).strip()
            tokens, op = _split_fts_tokens(tokens_str)
            if tokens:
                hints.fts_tokens = tokens
                hints.fts_operator = op
            continue

        # Equality filter
        m = _RE_EQ.search(clause)
        if m:
            col = m.group(1).upper()
            val = m.group(2).strip()
            flags = (m.group(4) or "").lower()
            ci = "ci" in flags or "case_insensitive" in flags
            trim = "trim" in flags or "t" in flags
            hints.eq_filters.append(EqFilter(col=col, val=val, ci=ci, trim=trim))

        # Group by
        m = _RE_GROUP_BY.search(clause)
        if m:
            resolved = resolve_group_by(m.group(1))
            if resolved:
                hints.group_by = resolved

        # Gross toggle
        m = _RE_GROSS.search(clause)
        if m:
            hints.gross = m.group(1).lower() == "true"

    # Deduplicate eq filters by (col, normalized val, ci, trim)
    seen = set()
    deduped: List[EqFilter] = []
    for f in hints.eq_filters:
        key = (f.col, f.val.strip().lower(), f.ci, f.trim)
        if key not in seen:
            seen.add(key)
            deduped.append(f)
    hints.eq_filters = deduped
    return hints

