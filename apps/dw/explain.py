from typing import Dict, Any


def explain_interpretation(intent: Dict[str, Any], binds: Dict[str, Any], table: str = "Contract") -> str:
    parts = []
    q = (intent.get("notes") or {}).get("q", "")
    date_col = intent.get("date_column") or "OVERLAP"
    exp = intent.get("explicit_dates") or {}
    gb = intent.get("group_by")
    agg = intent.get("agg")
    top_n = intent.get("top_n")
    measure = intent.get("measure_sql") or "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"
    fts = intent.get("full_text_search")
    tokens = intent.get("fts_tokens") or []

    if exp:
        parts.append(f"Interpreting time window as {exp.get('start')} → {exp.get('end')}.")
    if date_col == "OVERLAP":
        parts.append("Treating “contracts” as active by overlap (START_DATE ≤ end AND END_DATE ≥ start).")
    elif date_col == "REQUEST_DATE":
        parts.append("Using REQUEST_DATE because the question mentions “requested”.")
    elif date_col == "END_DATE" and intent.get("expire"):
        parts.append(f"Using END_DATE for expiry in next {intent['expire']} days.")
    if gb:
        parts.append(f"Grouping by {gb}.")
    if agg == "count":
        parts.append("Returning counts.")
    elif measure:
        parts.append(f"Measuring by {('GROSS' if 'VAT' in measure and ' + ' in measure else 'NET')} contract value.")
    if top_n:
        parts.append(f"Limiting to Top {top_n} by sort.")
    if fts:
        parts.append(f"Full‑text search enabled across configured columns (tokens: {', '.join(tokens)})")
    return " ".join(parts) or "Default interpretation applied."


def build_explanation(*, intent: Dict[str, Any], binds: Dict[str, Any], fts_meta: Dict[str, Any], table: str,
                       cols_selected: Any, strategy: str, default_date_basis: str) -> str:
    # Legacy shim for existing callers; delegate to explain_interpretation.
    return explain_interpretation(intent, binds, table=table)
