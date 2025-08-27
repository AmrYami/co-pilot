# apps/fa/hints.py
"""
FA-specific hint extraction: date ranges, equality filters, dimensions (1..4),
item filters, and 'category' → transaction type mapping.

All lookups use mem_* metadata and the live prefixed tables; nothing FA-specific
is put under core/.
"""
from __future__ import annotations
import re, json
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine

# --- light date parsers (day-to-day & month ranges) ---

LAST_MONTH   = re.compile(r"\blast\s+month\b", re.I)
YTD          = re.compile(r"\bYTD\b|\byear\s*to\s*date\b", re.I)
RANGE_YMD    = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", re.I)
BETWEEN_YMD  = re.compile(r"\bbetween\s+(\d{4})-(\d{2})-(\d{2})\s+and\s+(\d{4})-(\d{2})-(\d{2})\b", re.I)
RANGE_YM     = re.compile(r"\bfrom\s+(\d{4})[-/](\d{1,2})\s+to\s+(\d{4})[-/](\d{1,2})\b", re.I)

def _month_bounds(y: int, m: int) -> tuple[date, date]:
    from calendar import monthrange
    s = date(y, m, 1)
    e = date(y, m, monthrange(y, m)[1])
    return s, e

def _infer_date_range(text: str, today: Optional[date] = None) -> Optional[tuple[date, date, str]]:
    """Extract an explicit or implicit date range; grain can be month/day."""
    t = text.strip()
    d = today or date.today()

    m = RANGE_YMD.search(t) or BETWEEN_YMD.search(t)
    if m:
        y1,m1,d1,y2,m2,d2 = map(int, m.groups())
        return date(y1,m1,d1), date(y2,m2,d2), "day"

    m = RANGE_YM.search(t)
    if m:
        y1,m1,y2,m2 = map(int, m.groups())
        s, _ = _month_bounds(y1, m1)
        _, e = _month_bounds(y2, m2)
        return s, e, "month"

    if LAST_MONTH.search(t):
        y = d.year
        m = d.month - 1 or 12
        if d.month == 1: y -= 1
        s, e = _month_bounds(y, m)
        return s, e, "month"

    if YTD.search(t):
        s = date(d.year, 1, 1)
        return s, d, "day"

    return None

# --- simple k=v filter parser (user may type many variants) ---

KV_COLON = re.compile(r"\b([A-Za-z0-9_.]+)\s*:\s*([^\s,;]+)")
KV_EQ    = re.compile(r"\b([A-Za-z0-9_.]+)\s*=\s*([^\s,;]+)")

def _parse_eq_filters(text: str) -> dict[str, str]:
    """Parse k:v and k=v filters from free text."""
    eq: dict[str, str] = {}
    for m in KV_COLON.finditer(text):
        k, v = m.group(1).strip(), m.group(2).strip().strip('"\'')
        eq[k] = v
    for m in KV_EQ.finditer(text):
        k, v = m.group(1).strip(), m.group(2).strip().strip('"\'')
        eq[k] = v
    return eq

# --- categories → FA types (override via mem_settings key: FA_CATEGORY_MAP) ---

_DEFAULT_CATEGORY_MAP = {
    # keep soft assumptions; planner will skip unknowns
    "sales invoice": {"table": "debtor_trans", "types": [10]},
    "customer receipt": {"table": "debtor_trans", "types": [12]},
    "supplier payment": {"table": "supp_trans",   "types": [22]},
    "expenses invoice": {"table": "supp_trans",   "types": [20]},
    "fund transfer":   {"table": "bank_trans",   "types": [4]},
}

def _load_category_map(settings) -> dict:
    """Load category map from mem_settings (FA_CATEGORY_MAP) or fallback."""
    try:
        m = settings.get("FA_CATEGORY_MAP")
        if isinstance(m, str):
            return json.loads(m)
        if isinstance(m, dict):
            return m
    except Exception:
        pass
    return _DEFAULT_CATEGORY_MAP

# --- dimension & item resolvers ---

def _resolve_dimensions(mem_engine: Engine, prefix: str, tokens: Iterable[str]) -> dict[str, List[int]]:
    """
    Resolve dimension names to IDs for up to 4 dimensions.
    We try to match tokens (case-insensitive) against columns commonly present
    on the <prefix>dimensions table (name/reference/description).
    """
    dim_ids: dict[str, List[int]] = {}
    if not tokens:
        return dim_ids

    like_terms = [t for t in tokens if t and len(t) >= 2]
    if not like_terms:
        return dim_ids

    # guess common columns
    candidates = ["name", "reference", "ref", "description", "title"]
    cols_expr = " || ' ' || ".join([f"COALESCE({c},'')" for c in candidates])

    sql = text(f"""
        SELECT id
        FROM {prefix}dimensions
        WHERE (
            {" OR ".join([f"{cols_expr} ILIKE :t{i}" for i,_ in enumerate(like_terms)])}
        )
        LIMIT 100
    """)
    params = {f"t{i}": f"%{tok}%" for i, tok in enumerate(like_terms)}

    try:
        with mem_engine.connect() as c:
            rows = c.execute(sql, params).fetchall()
            ids = [int(r[0]) for r in rows]
    except Exception:
        ids = []

    # We don’t know which dim column (1..4) the user meant; give the planner options.
    if ids:
        dim_ids = {"dimension1_id": ids, "dimension2_id": ids, "dimension3_id": ids, "dimension4_id": ids}
    return dim_ids

def _resolve_items(mem_engine: Engine, prefix: str, tokens: Iterable[str]) -> List[str]:
    """
    Resolve item tokens (codes/names) → stock_id list via <prefix>stock_master.
    """
    like_terms = [t for t in tokens if t and len(t) >= 2]
    if not like_terms:
        return []
    sql = text(f"""
        SELECT stock_id
        FROM {prefix}stock_master
        WHERE (
            {" OR ".join([f"stock_id ILIKE :t{i} OR COALESCE(description,'') ILIKE :t{i}" for i,_ in enumerate(like_terms)])}
        )
        LIMIT 200
    """)
    params = {f"t{i}": f"%{tok}%" for i, tok in enumerate(like_terms)}
    try:
        with mem_engine.connect() as c:
            rows = c.execute(sql, params).fetchall()
            return [str(r[0]) for r in rows]
    except Exception:
        return []

# --- public API ---

def make_fa_hints(settings, mem_engine: Engine, prefixes: List[str], question: str) -> Dict[str, Any]:
    """
    Build FA hints:
      - date_range: {start, end, grain}
      - eq_filters: {"k":"v", ...}  (user-specified equals)
      - categories: [{"table":"debtor_trans","types":[10]}, ...]
      - dimensions: {"dimension1_id":[..], "dimension2_id":[..], ...} by resolving names
      - items: ["STK-001", ...] resolved by code/name
    """
    out: Dict[str, Any] = {}
    # 1) date range
    dr = _infer_date_range(question)
    if dr:
        s, e, g = dr
        out["date_range"] = {"start": str(s), "end": str(e), "grain": g}
    # 2) eq filters
    out["eq_filters"] = _parse_eq_filters(question)

    # 3) categories
    cat_map = _load_category_map(settings)
    cats = []
    lower_q = question.lower()
    for label, spec in cat_map.items():
        if label in lower_q:
            cats.append({"table": spec.get("table"), "types": list(spec.get("types") or [])})
    if cats:
        out["categories"] = cats

    # 4) dimensions & 5) items — resolve against the FIRST prefix (tenant-local)
    if prefixes:
        prefix = prefixes[0]
        # naive token capture for dimension phrases
        dim_tokens = []
        for m in re.finditer(r"\bdimension[ _-]?(?:[1-4])?\s*[:=]\s*([A-Za-z0-9 _-]{2,})", question, re.I):
            dim_tokens.append(m.group(1).strip())
        # also pick terms like "dim3=Retail"
        for m in re.finditer(r"\bdim(?:ension)?\s*([1-4])\s*=\s*([A-Za-z0-9 _-]{2,})", question, re.I):
            dim_tokens.append(m.group(2).strip())
        if dim_tokens:
            out["dimensions"] = _resolve_dimensions(mem_engine, prefix, dim_tokens)

        # items
        item_tokens = []
        for m in re.finditer(r"\bitem\s*[:=]\s*([A-Za-z0-9._-]{2,})", question, re.I):
            item_tokens.append(m.group(1).strip())
        for m in re.finditer(r"\bstock\s*[:=]\s*([A-Za-z0-9._-]{2,})", question, re.I):
            item_tokens.append(m.group(1).strip())
        items = _resolve_items(mem_engine, prefix, item_tokens)
        if items:
            out["items"] = items

    return out
