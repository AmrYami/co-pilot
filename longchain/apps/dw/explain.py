from typing import Dict, List, Any


def _fmt_eq_filters(eq_filters: List[Dict[str, Any]]) -> str:
    if not eq_filters:
        return "None"
    parts = []
    for f in eq_filters:
        col = f.get("col")
        val = f.get("val")
        ci = f.get("ci", False)
        trim = f.get("trim", False)
        parts.append(f"{col} = {val} (ci={ci}, trim={trim})")
    return "; ".join(parts)


def _fmt_fts(fts: Dict[str, Any]) -> str:
    if not fts or not fts.get("enabled"):
        return "Disabled"
    cols = fts.get("columns") or []
    tokens = fts.get("tokens") or []
    operator = fts.get("operator") or "OR"
    engine = fts.get("engine") or "like"
    if isinstance(tokens, list) and tokens and isinstance(tokens[0], list):
        tok_str = " OR ".join([" AND ".join(grp) for grp in tokens])
    else:
        tok_str = " | ".join(tokens)
    return f"FTS({engine}) on {len(cols)} cols; operator={operator}; tokens=[{tok_str}]"


def build_user_explain(payload: Dict[str, Any]) -> str:
    """
    Compact English rationale for end users.
    Summarizes filters, FTS mode/tokens, grouping, ordering, and measure.
    """
    intent = payload.get("intent", {})
    fts = payload.get("fts", {})
    gb = intent.get("group_by")
    sort_by = intent.get("sort_by")
    sort_desc = intent.get("sort_desc")
    measure_sql = intent.get("measure_sql")
    eq_filters = intent.get("eq_filters") or []

    bits: List[str] = []
    bits.append(f"Applied equality filters: {_fmt_eq_filters(eq_filters)}.")
    bits.append(_fmt_fts(fts) + ".")
    if gb:
        bits.append(f"Grouped by {gb}.")
    if sort_by:
        bits.append(f"Ordered by {sort_by} {'DESC' if sort_desc else 'ASC'}.")
    if measure_sql:
        bits.append("Gross measure applied." if "VAT" in str(measure_sql).upper() else "Simple measure.")
    return " ".join(bits)
