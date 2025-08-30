"""
adapters.py — FrontAccounting helpers
- canonical↔physical table mapping (prefix handling)
- simple SQL rewriter for prefixes (regex-based; swap with sqlglot later)
- join-graph / metrics loaders (YAML) with prefix expansion
"""
from __future__ import annotations

import re
from typing import Dict, Optional, Any, Iterable, List, Tuple
from datetime import date, timedelta

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

def expand_keywords(words: List[str]) -> List[str]:
    base = [w.lower() for w in words]
    syn = {
        "customer": ["debtors_master", "debtor", "debtors", "customers"],
        "customers": ["debtors_master", "debtor", "debtors", "customer"],
        "client": ["debtors_master", "customer", "customers"],
        "clients": ["debtors_master", "customer", "customers"],

        "sales": ["debtor_trans", "sales_orders", "invoices", "invoice"],
        "sale": ["debtor_trans", "sales_orders", "invoices", "invoice"],
        "orders": ["sales_orders", "sales_order_details"],
        "order": ["sales_orders", "sales_order_details"],
        "invoice": ["debtor_trans", "invoices"],
        "invoices": ["debtor_trans", "invoice"],

        "receipt": ["bank_trans"],
        "receipts": ["bank_trans"],
        "stock": ["stock_moves", "stock_master"],
        "grn": ["grn_batch", "grn_items"],
    }
    out = set(base)
    for w in base:
        for alt in syn.get(w, []):
            out.add(alt)
    return list(out)

def canonical_to_physical(table: str, prefix: str) -> str:
    return f"{prefix}{table}"


def physical_to_canonical(raw: str, prefix: str) -> str:
    return raw[len(prefix):] if raw.startswith(prefix) else raw


# ---------------- SQL Rewriting (lightweight) ----------------
from_join_pat = re.compile(r"\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)(?!\.)\b", re.IGNORECASE)


def prepend_prefix(sql: str, prefix: str) -> str:
    def repl(m):
        kw, name = m.group(1), m.group(2)
        if name.startswith("(") or "." in name:  # already qualified or subquery
            return m.group(0)
        return f"{kw} {prefix}{name}"
    return from_join_pat.sub(repl, sql)


def union_for_prefixes(canonical_sql: str, prefixes: Iterable[str]) -> str:
    ps = list(prefixes)
    if len(ps) == 1:
        return prepend_prefix(canonical_sql, ps[0])
    parts = []
    for p in ps:
        sql_p = prepend_prefix(canonical_sql, p)
        parts.append(f"SELECT '{p}' AS tenant, t.* FROM ( {sql_p} ) t")
    return "\nUNION ALL\n".join(parts)


# ---------------- YAML helpers ----------------

def load_join_graph(path: str) -> List[Dict]:
    if not yaml:
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("joins", [])


def expand_join_graph_for_prefix(joins: List[Dict], prefix: str) -> List[Dict]:
    out = []
    for j in joins:
        out.append({
            **j,
            "from": canonical_to_physical(j["from"], prefix),
            "to": canonical_to_physical(j["to"], prefix),
        })
    return out


def load_metrics(path: str) -> Dict:
    if not yaml:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

_BUILTIN_ALIASES = {
    "sales_amount": {"revenue", "sales", "sales total", "sales amount", "turnover"},
    "ar_outstanding": {"ar outstanding", "outstanding receivables", "unpaid invoices", "receivables balance"},
}

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def build_alias_index(metrics: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Return a dict {alias -> metric_key} using YAML 'aliases' if present,
    and merge builtin aliases for robustness.
    """
    idx: Dict[str, str] = {}
    for key, meta in (metrics or {}).items():
        idx[_normalize(key)] = key
        label = _normalize(str(meta.get("label") or ""))
        if label:
            idx[label] = key
        for a in meta.get("aliases", []) or []:
            alias = _normalize(str(a))
            if alias:
                idx[alias] = key
    # merge builtins without overwriting explicit YAML
    for k, aliases in _BUILTIN_ALIASES.items():
        for a in aliases:
            idx.setdefault(_normalize(a), k)
    return idx

def match_metric(question: str, metrics: Dict[str, Dict[str, Any]]) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Heuristic text match: exact alias containment win.
    """
    if not metrics:
        return None
    q = _normalize(question)
    idx = build_alias_index(metrics)

    # try longest alias first (prefer multi-word phrases)
    aliases_sorted = sorted(idx.keys(), key=len, reverse=True)
    for a in aliases_sorted:
        if a and a in q:
            key = idx[a]
            meta = metrics.get(key)
            if meta:
                return key, meta
    return None

def parse_date_range(question: str) -> Optional[Dict[str, str]]:
    """
    Return a dict with 'label' and 'sql_predicate' for MySQL (FA) when a common
    phrase is detected. Else None.
    """
    q = _normalize(question)
    today = date.today()
    # helpers for month boundaries
    def first_of_month(d: date) -> date:
        return date(d.year, d.month, 1)
    def last_of_month(d: date) -> date:
        from calendar import monthrange
        return date(d.year, d.month, monthrange(d.year, d.month)[1])

    if "last month" in q:
        lm = (first_of_month(today) - timedelta(days=1))
        start = first_of_month(lm)
        end = last_of_month(lm)
        return {
            "label": "last_month",
            "sql_predicate": f"DATE(dt.tran_date) BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'"
        }

    if "this month" in q or "current month" in q:
        start = first_of_month(today)
        return {
            "label": "this_month",
            "sql_predicate": f"DATE(dt.tran_date) >= '{start.isoformat()}'"
        }

    if "last 7 days" in q or "past 7 days" in q:
        start = today - timedelta(days=7)
        return {
            "label": "last_7d",
            "sql_predicate": f"DATE(dt.tran_date) >= '{start.isoformat()}'"
        }

    if "ytd" in q or "year to date" in q:
        start = date(today.year, 1, 1)
        return {
            "label": "ytd",
            "sql_predicate": f"DATE(dt.tran_date) >= '{start.isoformat()}'"
        }

    return None

def inject_date_filter(sql: str, predicate: str) -> str:
    """
    Inject a date predicate into existing SQL. If there's a WHERE, add AND; else add WHERE.
    Keeps it simple/safe for SELECT statements.
    """
    # strip trailing semicolon for easier edits
    s = sql.strip().rstrip(";")
    if re.search(r"(?is)\bwhere\b", s):
        return f"{s} AND {predicate}"
    return f"{s} WHERE {predicate}"

def similar_inquiry_hints(mem_engine, namespace: str, question: str, limit: int = 20) -> str:
    from sqlalchemy import text
    toks = set(re.findall(r"[A-Za-z0-9_]+", question.lower()))
    with mem_engine.connect() as c:
        rows = c.execute(text("""
            SELECT id, question, admin_reply
            FROM mem_inquiries
            WHERE namespace=:ns AND status='answered'
            ORDER BY created_at DESC
            LIMIT :lim
        """), {"ns": namespace, "lim": limit}).mappings().all()
    best = None; best_j = 0.0
    for r in rows:
        t2 = set(re.findall(r"[A-Za-z0-9_]+", (r["question"] or "").lower()))
        inter = len(toks & t2); union = len(toks | t2) or 1
        j = inter/union
        if j > best_j:
            best, best_j = r, j
    if best and best_j >= 0.6:
        return f"Past similar answer (Jaccard={best_j:.2f}) notes: {best.get('admin_reply') or '(none)'}"
    return ""
