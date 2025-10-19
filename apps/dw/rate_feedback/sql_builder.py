from typing import Any, Dict, List, Optional, Tuple

from apps.dw.sql_utils import pick_measure_sql, resolve_group_by


def _make_like_bind(val: str) -> str:
    """Wrap a token with %wildcards% suitable for LIKE."""

    return f"%{val}%"


def _fts_condition_sql(
    tokens: List[str], columns: List[str], operator: str, bind_prefix: str, binds: Dict[str, Any]
) -> str:
    """
    Build a composable SQL condition for FTS-like search:
      (UPPER(NVL(col,'')) LIKE UPPER(:b0) OR ...)
      <OP> (same for next token).
    Populates ``binds`` with wildcard-wrapped values.
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


def _append_where_clauses(where_clauses: List[str]) -> str:
    if not where_clauses:
        return ""
    return "\nWHERE " + "\n  AND ".join(where_clauses)


def _parse_sort_hint(value: Any) -> Tuple[str, Optional[bool]]:
    """Return (column, desc?) extracted from *value* if it carries direction."""

    if not isinstance(value, str):
        return "", None
    token = value.strip()
    if not token:
        return "", None
    parts = token.split()
    if len(parts) >= 2:
        direction = parts[1].upper()
        if direction in {"ASC", "DESC"}:
            return parts[0], direction == "DESC"
    return token, None


def _normalize_sort(sort_by: str, measure_alias: str, group_alias: str) -> str:
    if not sort_by:
        return measure_alias
    key = sort_by.upper()
    if key in {"TOTAL_GROSS", "TOTAL", "VALUE", "MEASURE"}:
        return measure_alias
    if key in {"GROUP", "GROUP_KEY"}:
        return group_alias
    return sort_by


def build_contract_sql(intent: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Compose a SELECT against the Contract table honoring rate hints:
      - eq_filters (exact matches with optional ci/trim)
      - fts_tokens across configured FTS columns (LIKE)
      - optional group_by/gross hints
      - requested ordering without duplicate ORDER BY clauses
    """

    table = '"Contract"'
    where_clauses: List[str] = []
    binds: Dict[str, Any] = {}

    # Equality filters
    for i, f in enumerate(intent.get("eq_filters", []) or []):
        col = f.get("col", "").upper()
        val = f.get("val", "")
        if not col:
            continue
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

    where_sql = _append_where_clauses(where_clauses)

    group_col = resolve_group_by(intent.get("group_by"))
    gross_flag = bool(intent.get("gross"))
    aggregate = bool(group_col)
    measure_sql, measure_alias = pick_measure_sql(gross_flag, aggregate=aggregate)

    if group_col:
        lines: List[str] = [
            f"SELECT {group_col} AS GROUP_KEY",
            f"       {measure_sql} AS {measure_alias}",
            "       COUNT(*) AS CNT",
            f"FROM {table}",
        ]
        if where_sql:
            lines.append(where_sql.strip())
        lines.append(f"GROUP BY {group_col}")

        sort_by_hint, dir_hint = _parse_sort_hint(intent.get("sort_by"))
        sort_by = sort_by_hint or "MEASURE"
        sort_desc = intent.get("sort_desc")
        if sort_desc is None:
            sort_desc = dir_hint if dir_hint is not None else True
        else:
            sort_desc = bool(sort_desc)
        if dir_hint is not None:
            sort_desc = dir_hint
        order_expr = _normalize_sort(sort_by, measure_alias, "GROUP_KEY")
        lines.append(f"ORDER BY {order_expr} {'DESC' if sort_desc else 'ASC'}")
        return "\n".join(lines), binds

    # Non-grouped listing
    lines = [f"SELECT * FROM {table}"]
    if where_sql:
        lines.append(where_sql.strip())
    sort_by_hint, dir_hint = _parse_sort_hint(intent.get("sort_by"))
    sort_by = sort_by_hint or "REQUEST_DATE"
    sort_desc = intent.get("sort_desc")
    if sort_desc is None:
        sort_desc = dir_hint if dir_hint is not None else True
    else:
        sort_desc = bool(sort_desc)
    if dir_hint is not None:
        sort_desc = dir_hint
    lines.append(f"ORDER BY {sort_by} {'DESC' if sort_desc else 'ASC'}")
    return "\n".join(lines), binds

