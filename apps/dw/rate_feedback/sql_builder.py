from typing import Any, Dict, List, Tuple


def _make_like_bind(val: str) -> str:
    """Wrap a token with %wildcards% suitable for LIKE."""
    return f"%{val}%"


def _fts_condition_sql(
    tokens: List[str], columns: List[str], operator: str, bind_prefix: str, binds: Dict[str, Any]
) -> str:
    """
    Build a composable SQL condition for FTS-like search:
      (UPPER(NVL(col,'')) LIKE UPPER(:b0) OR ... OR UPPER(NVL(colN,'')) LIKE UPPER(:b0))
      <OP> (same for next token) ...
    Returns the SQL string and fills 'binds' in-place with bidx -> %token%
    """
    op = "AND" if str(operator).upper() == "AND" else "OR"
    token_groups: List[str] = []
    for idx, token in enumerate(tokens):
        bname = f"{bind_prefix}{idx}"
        binds[bname] = _make_like_bind(token)
        per_token = " OR ".join(
            [f"UPPER(NVL({col},'')) LIKE UPPER(:{bname})" for col in columns]
        )
        token_groups.append(f"({per_token})")
    if not token_groups:
        return ""
    return "(" + f" {op} ".join(token_groups) + ")"


def build_contract_sql(intent: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Compose a SELECT against the Contract table honoring:
      - eq_filters (exact matches with optional ci/trim)
      - fts_tokens across configured FTS columns (LIKE)
      - requested order_by
    """
    sql = 'SELECT * FROM "Contract"'
    where_clauses: List[str] = []
    binds: Dict[str, Any] = {}

    # Equality filters
    for i, f in enumerate(intent.get("eq_filters", []) or []):
        col = f.get("col", "").upper()
        val = f.get("val", "")
        ci = bool(f.get("ci", True))
        trim = bool(f.get("trim", True))
        bname = f"eq_{i}"
        binds[bname] = val
        lhs = col
        if trim:
            lhs = f"TRIM({lhs})"
        rhs = f":{bname}"
        if ci:
            lhs = f"UPPER({lhs})"
            rhs = f"UPPER({rhs})"
        where_clauses.append(f"{lhs} = {rhs}")

    # FTS LIKE filters
    tokens: List[str] = intent.get("fts_tokens") or []
    if intent.get("full_text_search") and tokens:
        columns: List[str] = intent.get("fts_columns") or []
        if not columns:
            columns = ["CONTRACT_SUBJECT", "CONTRACT_PURPOSE"]
        cond = _fts_condition_sql(tokens, columns, intent.get("fts_operator", "OR"), "fts_", binds)
        if cond:
            where_clauses.append(cond)

    if where_clauses:
        sql += "\nWHERE " + "\n  AND ".join(where_clauses)

    # ORDER BY
    sort_by = intent.get("sort_by")
    sort_desc = bool(intent.get("sort_desc"))
    if sort_by:
        sql += f"\nORDER BY {sort_by} {'DESC' if sort_desc else 'ASC'}"
    else:
        sql += "\nORDER BY REQUEST_DATE DESC"

    return sql, binds

