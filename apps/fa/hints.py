# apps/fa/hints.py
"""FA-specific hint helpers."""

from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple
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


def parse_admin_reply(text: str) -> Dict:
    """Parse a free-text admin reply into a structured hint object."""
    t = (text or "").strip()
    if not t:
        return {}

    tables = {}
    for key in (
        "debtor_trans",
        "debtor_trans_details",
        "debtors_master",
        "gl_trans",
        "supp_trans",
        "bank_trans",
        "stock_moves",
    ):
        if re.search(rf"\b{key}\b", t, re.I):
            alias = {
                "debtor_trans": "dt",
                "debtor_trans_details": "dtd",
                "debtors_master": "dm",
            }.get(key, key[:2])
            tables[alias] = key

    date_col = None
    m = re.search(r"(date column|column)\s*[:=]\s*([a-zA-Z0-9_\.]+)", t)
    if m:
        date_col = m.group(2)
    period = "last_month" if re.search(r"\blast\s+month\b", t, re.I) else None

    metric = None
    if re.search(r"\bnet\s+sales\b", t, re.I):
        metric = {
            "key": "net_sales",
            "expr": "SUM((CASE WHEN dt.type=11 THEN -1 ELSE 1 END) * dtd.unit_price * (1 - COALESCE(dtd.discount_percent,0)) * dtd.quantity)",
        }

    group_by = []
    if re.search(r"\bcustomer\s+name\b", t, re.I):
        group_by.append("dm.name")
    order_by = ["net_sales DESC"] if metric and metric.get("key") == "net_sales" else []
    limit = 10 if re.search(r"\btop\s*10\b", t, re.I) else None

    joins: List[str] = []
    if {"dt", "dtd"}.issubset(tables.keys()):
        joins += [
            "dtd.debtor_trans_no = dt.trans_no",
            "dtd.debtor_trans_type = dt.type",
        ]
    if {"dt", "dm"}.issubset(tables.keys()):
        joins += ["dm.debtor_no = dt.debtor_no"]

    filters = []
    if "dt" in tables:
        filters.append("dt.type IN (1,11)")

    return {
        "tables": tables,
        "joins": joins,
        "date": {"column": date_col, "period": period},
        "filters": filters,
        "metric": metric,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit,
    }


def try_parse_admin_text(text: str) -> dict:
    """Parse loose admin notes into a structured hint dict."""
    d = {"tables": {}, "joins": [], "filters": [], "metric": {}, "date": {}}
    if not text:
        return d
    t = text.lower()
    for key, rx in {
        "dt": r"debtor[_\s]?trans",
        "dtd": r"debtor[_\s]?trans[_\s]?details",
        "dm": r"debtors[_\s]?master",
    }.items():
        m = re.search(rx, t)
        if m:
            d["tables"][key] = {"name": m.group(0).replace(" ", "_")}
    if "tran_date" in t or "date column dt.tran_date" in t:
        d["date"]["column"] = "dt.tran_date"
    if "last month" in t or "period last month" in t:
        d["date"]["period"] = "last_month"
    if "joined to customers" in t:
        d["joins"] += [
            "dtd.debtor_trans_no = dt.trans_no",
            "dtd.debtor_trans_type = dt.type",
            "dm.debtor_no = dt.debtor_no",
        ]
    if "net sales" in t:
        d["metric"] = {
            "key": "net_sales",
            "expr": ("SUM((CASE WHEN dt.type=11 THEN -1 ELSE 1 END) "
                     "* dtd.unit_price * (1 - COALESCE(dtd.discount_percent, 0)) * dtd.quantity)"),
        }
    if "type in (1,11)" in t or "invoice" in t or "credit note" in t:
        d["filters"].append("dt.type IN (1,11)")
    return d


def derive_sql_from_hints(prefixes: List[str], h: Dict) -> str:
    """Turn structured hints into a concrete MySQL SQL string with FA prefixes."""
    if not h or "tables" not in h or not h["tables"]:
        raise ValueError("insufficient_hints")

    pfx = (prefixes or [""]).pop(0)

    def T(name: str) -> str:
        return f"`{pfx}{name}`"

    tables = h["tables"]
    if not {"dt", "dtd", "dm"}.issubset(tables.keys()):
        raise ValueError("unsupported_hint_combo")

    date_col = h.get("date", {}).get("column") or "dt.tran_date"
    period = h.get("date", {}).get("period") or "last_month"

    date_filter = ""
    if period == "last_month":
        date_filter = "AND DATE_FORMAT(dt.tran_date, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')"

    metric_expr = h.get("metric", {}).get("expr") or "SUM((CASE WHEN dt.type=11 THEN -1 ELSE 1 END) * dtd.unit_price * (1 - COALESCE(dtd.discount_percent,0)) * dtd.quantity)"

    group_cols = h.get("group_by") or ["dm.name"]
    order_cols = h.get("order_by") or ["net_sales DESC"]
    limit = h.get("limit") or 10

    sql = f"""
SELECT dm.name AS customer,
       {metric_expr} AS net_sales
FROM {T('debtor_trans')} AS dt
JOIN {T('debtor_trans_details')} AS dtd
  ON dtd.debtor_trans_no = dt.trans_no
 AND dtd.debtor_trans_type = dt.type
JOIN {T('debtors_master')} AS dm
  ON dm.debtor_no = dt.debtor_no
WHERE dt.type IN (1, 11)
  {date_filter}
GROUP BY {', '.join(group_cols)}
ORDER BY {', '.join(order_cols)}
LIMIT {int(limit)};
""".strip()
    return sql


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
    """Compatible entry point supporting legacy and new call styles.

    Always normalize question (`q`) and prefixes to avoid NameError on old code.
    """

    admin_reply: Optional[str] = None
    prefixes: List[str] = []
    q: str = ""

    # New-style: single dict positional
    if args and len(args) == 1 and isinstance(args[0], dict):
        payload = dict(args[0])
        admin_reply = payload.get("admin_reply")
        prefixes = payload.get("prefixes") or []
        q = (payload.get("question") or "").strip()
        hints = _build(payload)
    # Legacy: 3 positional args -> (mem_engine, prefixes, question[, clarifications])
    elif len(args) >= 3:
        mem_engine, prefixes, question = args[:3]
        clar = args[3] if len(args) > 3 else None
        admin_overrides = args[4] if len(args) > 4 else None
        admin_reply = args[5] if len(args) > 5 else kwargs.get("admin_reply")
        q = (question or "").strip()
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
        prefixes = payload.get("prefixes") or []
        q = (payload.get("question") or "").strip()
        hints = _build(payload)
    else:
        admin_reply = kwargs.get("admin_reply")
        prefixes = kwargs.get("prefixes") or []
        q = (kwargs.get("question") or "").strip()
        hints = _build({
            "mem_engine": kwargs.get("mem_engine"),
            "prefixes": prefixes,
            "question": q,
            "clarifications": kwargs.get("clarifications"),
            "admin_overrides": kwargs.get("admin_overrides"),
        })

    if admin_reply:
        hints["admin_structured"] = try_parse_admin_text(admin_reply)

    hints["question"] = q
    hints["prefixes"] = prefixes
    return hints


def parse_admin_reply_to_hints(text: str, prefixes: List[str], question: str) -> Dict[str, Any]:
    t = (text or "").strip().lower()

    hints: Dict[str, Any] = {
        "prefixes": prefixes or [],
        "question": question or "",
        "tables": {},
        "joins": [],
        "filters": [],
        "metric": {},
        "date": {},
        "group_by": [],
        "order_by": [],
        "limit": None,
        "__needs": [],
    }

    tables = {}
    for key, pat in {
        "dt": r"\bdebtor[_\s]?trans\b",
        "dtd": r"\bdebtor[_\s]?trans[_\s]?details\b",
        "dm": r"\bdebtors[_\s]?master\b",
        "gl": r"\bgl[_\s]?trans\b",
        "bt": r"\bbank[_\s]?trans\b",
    }.items():
        if re.search(pat, t):
            if key == "dt":
                tables["dt"] = "debtor_trans"
            if key == "dtd":
                tables["dtd"] = "debtor_trans_details"
            if key == "dm":
                tables["dm"] = "debtors_master"
            if key == "gl":
                tables["gl"] = "gl_trans"
            if key == "bt":
                tables["bt"] = "bank_trans"

    if tables:
        hints["tables"] = tables

    if "debtor_trans_details" in tables.values() and "debtor_trans" in tables.values():
        hints["joins"].append("dtd.debtor_trans_no = dt.trans_no")
        hints["joins"].append("dtd.debtor_trans_type = dt.type")
    if "debtors_master" in tables.values() and "debtor_trans" in tables.values():
        hints["joins"].append("dm.debtor_no = dt.debtor_no")

    if "last month" in t or "last_month" in t:
        hints["date"] = {"column": "dt.tran_date", "period": "last_month"}
    elif "today" in t:
        hints["date"] = {"column": "dt.tran_date", "period": "today"}
    elif "yesterday" in t:
        hints["date"] = {"column": "dt.tran_date", "period": "yesterday"}
    m = re.findall(r"(\d{4}-\d{2}-\d{2})", t)
    if len(m) >= 2:
        hints["date"] = {"start": m[0], "end": m[1], "grain": "day", "column": hints.get("date", {}).get("column", "dt.tran_date")}

    if re.search(r"\bcredit\s*note\b", t):
        hints["filters"].append("dt.type IN (1,11)")
    elif re.search(r"\binvoice\b", t):
        hints["filters"].append("dt.type IN (1)")

    if "net" in t and "sales" in t:
        hints["metric"] = {
            "key": "net_sales",
            "expr": "SUM((CASE WHEN dt.type = 11 THEN -1 ELSE 1 END) * dtd.unit_price * (1 - COALESCE(dtd.discount_percent, 0)) * dtd.quantity)",
        }
    elif "count" in t:
        hints["metric"] = {"key": "cnt", "expr": "COUNT(*)"}

    if "top" in t and "customer" in t:
        hints["group_by"].append("dm.name")
        hints["order_by"].append("net_sales DESC")
        hints["limit"] = 10

    if not hints["tables"]:
        hints["__needs"].append("Which tables should we use (e.g., debtor_trans, debtors_master, gl_trans)?")
    if not hints["date"]:
        hints["__needs"].append("What date range should we use (e.g., last month, 2025-08-01 .. 2025-08-31)?")
    if not hints["metric"]:
        hints["__needs"].append("Which metric should we compute (e.g., sum of net sales, count of invoices)?")

    return hints






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


def parse_admin_reply_freeform(text: str, *, question: str = "") -> Dict[str, Any]:
    """Very permissive parser for free-text admin replies."""
    out: Dict[str, Any] = {"raw": text}
    t = (text or "").lower()
    if not t:
        return out

    tables = {}
    if "debtor_trans_details" in t or "dtd" in t:
        tables["dtd"] = "debtor_trans_details"
    if "debtor_trans" in t or " dt" in t or "dt " in t:
        tables["dt"] = "debtor_trans"
    if "debtors_master" in t or " dm" in t or "dm " in t:
        tables["dm"] = "debtors_master"
    if tables:
        out["tables"] = tables

    joins: List[str] = []
    if "debtor_trans_no" in t or "trans_no" in t:
        joins.append("dtd.debtor_trans_no = dt.trans_no")
    if "debtor_trans_type" in t or "dt.type" in t:
        joins.append("dtd.debtor_trans_type = dt.type")
    if "dm.debtor_no" in t or "debtors_master" in t:
        joins.append("dm.debtor_no = dt.debtor_no")
    if joins:
        out["joins"] = joins

    if "tran_date" in t:
        out["date"] = {"column": "dt.tran_date"}
    if "last month" in t or "last_month" in t:
        out.setdefault("date", {})["period"] = "last_month"

    if "type in (1,11" in t or ("invoice" in t and "credit" in t):
        out.setdefault("filters", []).append("dt.type IN (1,11)")

    if "net sales" in t or "sum net" in t:
        out["metric"] = {
            "key": "net_sales",
            "expr": "SUM((CASE WHEN dt.type=11 THEN -1 ELSE 1 END) * dtd.unit_price * (1 - COALESCE(dtd.discount_percent,0)) * dtd.quantity)",
        }
    if "top 10" in t or "limit 10" in t:
        out["limit"] = 10
    if "by customer" in t or "by customer name" in t:
        out["group_by"] = ["dm.name"]
        out["order_by"] = ["net_sales DESC"]

    return out

