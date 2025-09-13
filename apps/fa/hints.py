# apps/fa/hints.py
"""FA-specific hint helpers."""

from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
import re


# Questions to ask when specific fields are missing from the structured spec
MISSING_FIELD_QUESTIONS = {
    "date_range": "What date range should we use (e.g., last month, between 2025-08-01 and 2025-08-31)?",
    "tables": "Which tables should we use (e.g., debtor_trans, debtors_master, gl_trans)?",
    "metric": "Which metric should we compute (e.g., sum of net sales, count of invoices)?",
    "entity": "Top by what entity (customer, supplier, item, account, or a dimension)?",
}

# Lightweight domain hints handed to the clarifier
DOMAIN_HINTS = {
    "entities": ["customer", "supplier", "item", "account", "dimension"],
    "table_aliases": [
        "debtor_trans",
        "debtors_master",
        "supp_trans",
        "gl_trans",
        "bank_trans",
        "stock_moves",
        "item_codes",
    ],
    "metric_registry": {"net_sales": "sum(quantity * price * (1-discount))"},
}


def _last_month_bounds() -> tuple[str, str]:
    today = date.today()
    first_this = today.replace(day=1)
    last_day_prev = first_this - timedelta(days=1)
    first_day_prev = last_day_prev.replace(day=1)
    return first_day_prev.isoformat(), last_day_prev.isoformat()


def parse_admin_answer(answer: str) -> Dict[str, Any]:
    """
    Minimal heuristics:
      - if mentions 'invoice' or 'tran_date' -> prefer debtor_trans.tran_date
      - 'last month' -> concrete YYYY-MM-DD range
    Returns a dict that make_fa_hints can merge into its output.
    """
    a = (answer or "").lower()
    out: Dict[str, Any] = {}

    if "tran_date" in a or "invoice" in a:
        out["date_column"] = "debtor_trans.tran_date"

    if "last month" in a:
        start, end = _last_month_bounds()
        out["date_filter"] = {
            "column": out.get("date_column", "tran_date"),
            "op": "between",
            "start": start,
            "end": end,
        }
        out["time_grain"] = "month"

    if "top 10" in a:
        out["limit"] = 10

    return out


def _build(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build FA-specific hints from a normalized payload."""
    from core.hints import make_hints as core_make_hints
    from apps.fa.adapters import expand_keywords

    q = (payload.get("question") or "").strip()
    prefixes = list(payload.get("prefixes") or [])
    clarifications: Optional[Dict[str, Any]] = payload.get("clarifications") or None
    admin_overrides: Optional[Dict[str, Any]] = payload.get("admin_overrides") or None

    # App-agnostic, lightweight hints (date range, simple eq filters)
    base = core_make_hints(q)

    # FA-specific keyword expansion (customers, invoices, etc.)
    base["keywords"] = expand_keywords(q.split())

    # Apply clarifications when provided (date range, date column, etc.)
    if clarifications:
        if dr := clarifications.get("date_range"):
            # support either dict(start/end) or string alias
            if isinstance(dr, dict):
                base["date_range"] = dr
            elif isinstance(dr, str):
                from core.hints import make_hints as _mh
                dr_parsed = _mh(dr).get("date_range")
                if dr_parsed:
                    base["date_range"] = dr_parsed
        if dc := clarifications.get("date_column"):
            base["date_column"] = dc

    # Always pass-through prefixes to downstream planner
    base["prefixes"] = prefixes

    # Apply admin overrides last
    if admin_overrides:
        base.update(admin_overrides)

    # You can add more FA-specific nudges here later (dimensions, ST codes, etc.)
    return base


def make_fa_hints(*args, **kwargs) -> Dict[str, Any]:
    """Compatible entry point supporting legacy and new call styles."""

    admin_reply: Optional[str] = None

    # New-style: single dict positional
    if args and len(args) == 1 and isinstance(args[0], dict):
        payload = dict(args[0])
        admin_reply = payload.get("admin_reply")
        hints = _build(payload)
    # Legacy: 3 positional args -> (mem_engine, prefixes, question[, clarifications])
    elif len(args) >= 3:
        mem_engine, prefixes, question = args[:3]
        clar = args[3] if len(args) > 3 else None
        admin_overrides = args[4] if len(args) > 4 else None
        admin_reply = args[5] if len(args) > 5 else kwargs.get("admin_reply")
        hints = _build({
            "mem_engine": mem_engine,
            "prefixes": prefixes,
            "question": question,
            "clarifications": clar,
            "admin_overrides": admin_overrides,
        })
    # Named kwargs (accept either shape)
    elif "payload" in kwargs and isinstance(kwargs["payload"], dict):
        payload = dict(kwargs["payload"])
        admin_reply = kwargs.get("admin_reply") or payload.get("admin_reply")
        hints = _build(payload)
    else:
        admin_reply = kwargs.get("admin_reply")
        hints = _build({
            "mem_engine": kwargs.get("mem_engine"),
            "prefixes": kwargs.get("prefixes") or [],
            "question": kwargs.get("question") or "",
            "clarifications": kwargs.get("clarifications"),
            "admin_overrides": kwargs.get("admin_overrides"),
        })

    if admin_reply:
        hints["admin_structured"] = parse_admin_reply_to_hints(admin_reply)

    return hints


def parse_admin_reply_to_hints(text: str) -> Dict[str, Any]:
    """
    Parse free-form admin reply into a light structure the planner can use.
    Accepts simple 'key: value' lines and common patterns like:
      tables: dt=debtor_trans, dtd=debtor_trans_details, dm=debtors_master
      joins:
        - dtd.debtor_trans_no = dt.trans_no
        - dtd.debtor_trans_type = dt.type
        - dm.debtor_no = dt.debtor_no
      date: dt.tran_date last_month
      filters: dt.type in (1,11)
      metric:
        key: net_sales
        expr: sum((case when dt.type=11 then -1 else 1 end) * dtd.unit_price * (1 - dtd.discount_percent) * dtd.quantity)
      group_by: dm.name
      order_by: net_sales desc
      limit: 10
    Returns a best-effort dict; any missing pieces are simply omitted.
    """
    t = (text or "").strip()
    out: Dict[str, Any] = {
        "tables": {},
        "joins": [],
        "filters": [],
        "date": {},
        "metric": {},
        "group_by": [],
        "order_by": [],
        "limit": None,
        "raw": t,
    }

    lines = [ln.strip() for ln in re.split(r"[\r\n]+", t) if ln.strip()]
    buf_key: Optional[str] = None
    buf: List[str] = []

    def flush_buf() -> None:
        nonlocal buf_key, buf, out
        if not buf_key:
            return
        body = "\n".join(buf).strip()
        if buf_key == "joins":
            for b in re.split(r"^\s*-\s*", body, flags=re.M):
                b = b.strip()
                if not b:
                    continue
                if "\n" in b:
                    for sub in [x.strip() for x in b.splitlines() if x.strip()]:
                        if sub.startswith("-"):
                            sub = sub[1:].strip()
                        if sub:
                            out["joins"].append(sub)
                else:
                    out["joins"].append(b)
        elif buf_key == "metric":
            mkey = re.search(r"\bkey\s*:\s*([A-Za-z0-9_]+)", body, re.I)
            mexp = re.search(r"\bexpr\s*:\s*(.+)$", body, re.I | re.S)
            if mkey:
                out["metric"]["key"] = mkey.group(1).strip()
            if mexp:
                out["metric"]["expr"] = mexp.group(1).strip()
        else:
            parts = re.split(r"[,\n]+", body)
            parts = [p.strip() for p in parts if p.strip()]
            if buf_key == "filters":
                out["filters"].extend(parts)
            elif buf_key == "group_by":
                out["group_by"].extend(parts)
            elif buf_key == "order_by":
                out["order_by"].extend(parts)
        buf_key = None
        buf = []

    for ln in lines:
        m = re.match(r"^(tables|joins|date|filters|metric|group_by|order_by|limit)\s*:\s*(.*)$", ln, re.I)
        if m:
            flush_buf()
            key = m.group(1).lower()
            rest = m.group(2).strip()
            if key == "tables":
                for part in re.split(r"[,\s]+", rest):
                    if "=" in part:
                        a, b = part.split("=", 1)
                        a, b = a.strip(), b.strip().strip(",")
                        if a and b:
                            out["tables"][a] = b
            elif key == "date":
                mcol = re.match(r"^([A-Za-z0-9_\.]+)\s+(.+)$", rest)
                if mcol:
                    out["date"]["column"] = mcol.group(1).strip()
                    out["date"]["range"] = mcol.group(2).strip()
                else:
                    out["date"]["range"] = rest
            elif key == "limit":
                try:
                    out["limit"] = int(re.findall(r"\d+", rest)[0])
                except Exception:
                    pass
            else:
                buf_key = key
                if rest:
                    buf.append(rest)
        else:
            if buf_key:
                buf.append(ln)
    flush_buf()

    if "metric" in out and out["metric"] and "expr" not in out["metric"]:
        body = out["metric"]
        if isinstance(body, str):
            out["metric"] = {"expr": body}

    return out


def _mysql_last_month_range(col: str) -> str:
    return (
        f"{col} >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01') "
        f"AND {col} <  DATE_FORMAT(CURDATE(), '%Y-%m-01')"
    )


def try_build_sql_from_hints(hints: Dict[str, Any], prefixes: List[str]) -> Optional[str]:
    """
    Deterministic SQL builder for common FA patterns.
    Returns a SQL string if we have enough structure (tables + joins + metric + group/order),
    otherwise returns None so the planner can try.
    """
    if not hints:
        return None

    h = hints.get("admin_structured") or hints

    tables = h.get("tables") or {}
    joins = h.get("joins") or []
    metric = h.get("metric") or {}
    gby = h.get("group_by") or []
    oby = h.get("order_by") or []
    filters = h.get("filters") or []
    date = h.get("date") or {}
    limit = h.get("limit")

    if not tables or not joins or not metric or not metric.get("expr"):
        return None

    pfx = (prefixes or [""])[0]

    def qtbl(t: str) -> str:
        return f"`{pfx}{t}`"

    select_cols: List[str] = []
    if gby:
        label = gby[0]
        alias = "group_key"
        if re.search(r"\bdm\.name\b", label):
            alias = "customer_name"
        select_cols.append(f"{label} AS {alias}")

    m_alias = metric.get("key", "metric_value")
    select_cols.append(f"{metric['expr']} AS {m_alias}")

    anchor_alias = list(tables.keys())[0]
    anchor_table = tables[anchor_alias]
    sql: List[Optional[str]] = [
        f"SELECT {', '.join(select_cols)}",
        f"FROM {qtbl(anchor_table)} AS {anchor_alias}",
    ]

    for j in joins:
        sql.append(f"JOIN {j}")
        if j.lower().startswith("`") or " join " in j.lower():
            continue
        m = re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z0-9_]+)\b", j)
        rhs_alias = None
        for a, _ in m:
            if a in tables and a != anchor_alias:
                rhs_alias = a
                break
        if rhs_alias:
            rhs_table = tables[rhs_alias]
            sql[-1] = f"JOIN {qtbl(rhs_table)} AS {rhs_alias} ON {j}"
        else:
            sql[-1] = None
            filters.append(j)

    sql = [s for s in sql if s]

    where_parts: List[str] = []
    if date.get("column") and date.get("range"):
        rng = date["range"].strip().lower()
        if "last month" in rng or "last_month" in rng:
            where_parts.append(_mysql_last_month_range(date["column"]))
        elif re.search(r"\bbetween\b", rng):
            where_parts.append(f"{date['column']} {date['range']}")
        else:
            where_parts.append(f"{date['column']} {date['range']}")

    for f in filters:
        if f:
            where_parts.append(f)

    if where_parts:
        sql.append("WHERE " + "\n  AND ".join(where_parts))

    if gby:
        sql.append("GROUP BY " + ", ".join(gby))

    if oby:
        sql.append("ORDER BY " + ", ".join(oby))

    if limit:
        sql.append(f"LIMIT {int(limit)}")

    return "\n".join(sql)

