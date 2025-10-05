from typing import Any, Dict, List


def get_fts_columns(settings) -> List[str]:
    """
    Resolve FTS columns list using settings:
      - DW_FTS_COLUMNS["Contract"] if available
      - Else DW_FTS_COLUMNS["*"] if available
      - Else fall back to DW_EXPLICIT_FILTER_COLUMNS
    """
    cols: List[str] = []
    try:
        fts_cfg = settings.get("DW_FTS_COLUMNS", {}) or {}
        if isinstance(fts_cfg, dict):
            contract_cols = fts_cfg.get("Contract")
            if isinstance(contract_cols, list):
                cols = list(contract_cols)
            elif isinstance(fts_cfg.get("*"), list):
                cols = list(fts_cfg["*"])
    except Exception:
        cols = []
    if not cols:
        try:
            cols = list(settings.get("DW_EXPLICIT_FILTER_COLUMNS", []) or [])
        except Exception:
            cols = []
    # Ensure uniqueness and uppercase for SQL symbols
    return [c.upper() for c in dict.fromkeys(cols)]


def apply_rate_hints_to_intent(intent: Dict[str, Any], hints, settings) -> None:
    """
    Mutate the intent dict according to structured hints.
    Keys updated:
      - fts_tokens, fts_operator, fts_columns, full_text_search
      - sort_by, sort_desc
      - eq_filters (merged & deduped)
    """
    # Order by
    if hints.order_by:
        intent["sort_by"] = hints.order_by[0].upper()
        intent["sort_desc"] = bool(hints.order_by[1])

    # FTS
    if hints.fts_tokens:
        intent["fts_tokens"] = hints.fts_tokens
        intent["fts_operator"] = hints.fts_operator
        intent["fts_columns"] = get_fts_columns(settings)
        intent["full_text_search"] = True

    # Equality filters
    eq_filters: List[Dict[str, Any]] = []
    if "eq_filters" in intent and isinstance(intent["eq_filters"], list):
        eq_filters.extend(intent["eq_filters"])
    for f in getattr(hints, "eq_filters", []):
        eq_filters.append(
            {
                "col": f.col.upper(),
                "val": f.val,
                "op": f.op,
                "ci": bool(f.ci),
                "trim": bool(f.trim),
            }
        )
    # Deduplicate by (col,val,op,ci,trim)
    dedup = []
    seen = set()
    for f in eq_filters:
        key = (
            f.get("col", "").upper(),
            str(f.get("val", "")).strip().lower(),
            f.get("op", "eq"),
            bool(f.get("ci", True)),
            bool(f.get("trim", True)),
        )
        if key not in seen:
            seen.add(key)
            dedup.append(f)
    intent["eq_filters"] = dedup

