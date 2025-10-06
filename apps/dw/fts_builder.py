from typing import Dict, List, Tuple


def build_like_predicates(columns: List[str], bind_name: str) -> str:
    """Build an OR chain of ``UPPER(NVL(col,'')) LIKE UPPER(:bind)`` predicates."""

    safe_columns = [col.strip() for col in columns or [] if col and str(col).strip()]
    or_terms = [f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in safe_columns]
    return "(" + " OR ".join(or_terms) + ")"


def build_fts_where(
    tokens_groups: List[List[str]],
    columns: List[str],
    operator_between_groups: str,
    binds_out: Dict[str, str],
) -> Tuple[str, Dict[str, str]]:
    """Return a WHERE fragment for LIKE-based full-text search.

    ``tokens_groups`` is a list of token collections. Tokens within the same
    collection are OR'd together; collections are combined using
    ``operator_between_groups`` ("AND" or "OR"). ``binds_out`` is mutated in
    place with generated bind variables (``fts_0``, ``fts_1`` ...).
    """

    if not tokens_groups or not columns:
        return "", binds_out

    group_sql: List[str] = []
    bind_idx = len([k for k in binds_out if str(k).startswith("fts_")])

    for group in tokens_groups:
        tokens = [tok.strip() for tok in group if tok and tok.strip()]
        if not tokens:
            continue
        subterms: List[str] = []
        for token in tokens:
            bind_name = f"fts_{bind_idx}"
            bind_idx += 1
            binds_out[bind_name] = f"%{token}%"
            subterms.append(build_like_predicates(columns, bind_name))
        if subterms:
            group_sql.append("(" + " OR ".join(subterms) + ")")

    if not group_sql:
        return "", binds_out

    op = "AND" if operator_between_groups.upper() == "AND" else "OR"
    return "(" + f" {op} ".join(group_sql) + ")", binds_out
