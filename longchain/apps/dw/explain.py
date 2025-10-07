from typing import Any, Dict, List, Tuple


def _fmt_bool(flag: Any) -> str:
    return "true" if flag else "false"


def _safe_upper(value: Any) -> str:
    try:
        return str(value).upper()
    except Exception:
        return str(value)


def _list_to_inline(items: List[str], limit: int = 8) -> str:
    values = [str(item) for item in items if str(item).strip()]
    if not values:
        return "—"
    if len(values) <= limit:
        return ", ".join(values)
    head = ", ".join(values[:limit])
    remaining = len(values) - limit
    return f"{head} …(+{remaining})"


def build_explain_struct(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize DW answer payload into a compact explain struct."""

    payload = payload or {}
    debug = payload.get("debug", {}) or {}
    meta = payload.get("meta", {}) or {}

    intent = (
        debug.get("intent")
        or debug.get("clarifier_intent")
        or meta.get("clarifier_intent")
        or {}
    )
    fts_dbg = debug.get("fts") or meta.get("fts") or {}
    binds = meta.get("binds") or {}
    sql_text = payload.get("sql", "") or ""

    eq_filters = intent.get("eq_filters") or []
    fts_tokens = intent.get("fts_tokens") or fts_dbg.get("tokens") or []
    fts_columns = intent.get("fts_columns") or fts_dbg.get("columns") or []
    fts_operator = intent.get("fts_operator") or fts_dbg.get("operator") or "OR"
    full_text_search = bool(intent.get("full_text_search")) or bool(fts_dbg.get("enabled"))

    struct = {
        "eq_filters": eq_filters,
        "fts": {
            "enabled": bool(full_text_search) or bool(fts_tokens),
            "operator": fts_operator,
            "tokens": fts_tokens,
            "columns": fts_columns,
        },
        "group_by": intent.get("group_by"),
        "order_by": {
            "column": intent.get("sort_by"),
            "desc": bool(intent.get("sort_desc")),
        },
        "gross": {
            "enabled": bool(intent.get("gross")),
            "expr": intent.get("measure_sql"),
        },
        "binds_count": len(binds),
        "strategy": meta.get("strategy"),
        "sql_len": len(sql_text),
    }
    return struct


def build_explain_text(struct: Dict[str, Any]) -> str:
    """Turn explain struct into a concise English rationale."""

    struct = struct or {}
    parts: List[str] = []

    eq_filters = struct.get("eq_filters") or []
    if eq_filters:
        fragments: List[str] = []
        for filt in eq_filters:
            column = filt.get("col") or filt.get("column")
            value = filt.get("val") or filt.get("value")
            if not column:
                continue
            chunk = f"{column} = {value}"
            flags: List[str] = []
            if filt.get("ci"):
                flags.append("ci")
            if filt.get("trim"):
                flags.append("trim")
            if flags:
                chunk += f" ({'/'.join(flags)})"
            fragments.append(chunk)
        if fragments:
            parts.append(
                "Applied equality filters: " + _list_to_inline(fragments, limit=6) + "."
            )

    fts = struct.get("fts") or {}
    if fts.get("enabled"):
        tokens = fts.get("tokens") or []
        columns = fts.get("columns") or []
        operator = fts.get("operator") or "OR"
        if tokens:
            parts.append(
                f"Full‑text search using {operator} over tokens: "
                f"{_list_to_inline(tokens, limit=6)}."
            )
        if columns:
            parts.append(
                f"Search columns: {_list_to_inline([_safe_upper(c) for c in columns], limit=6)}."
            )

    group_by = struct.get("group_by")
    if group_by:
        parts.append(f"Grouped by: {group_by}.")

    order_by = struct.get("order_by") or {}
    if order_by.get("column"):
        direction = "DESC" if order_by.get("desc") else "ASC"
        parts.append(f"Ordered by {order_by.get('column')} {direction}.")

    gross = struct.get("gross") or {}
    if gross.get("enabled"):
        expr = gross.get("expr") or "custom gross measure"
        parts.append(f"Used gross measure: {expr}.")

    if not parts:
        parts.append("Default listing strategy with no explicit filters detected.")
    return " ".join(parts)


def build_explain(payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Convenience wrapper returning both text and struct."""

    struct = build_explain_struct(payload)
    text = build_explain_text(struct)
    return text, struct


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
    """Backward-compatible rationale used by legacy UI pieces."""

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
        bits.append(
            "Gross measure applied." if "VAT" in str(measure_sql).upper() else "Simple measure."
        )
    return " ".join(bits)
