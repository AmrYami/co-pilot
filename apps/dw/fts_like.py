from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

LIKE_WRAP = "UPPER(NVL({col},'')) LIKE UPPER(:{b})"


def _group_sql_for_token(token_bind: str, columns: Sequence[str]) -> str:
    ors = []
    for col in columns:
        ors.append(LIKE_WRAP.format(col=col, b=token_bind))
    return "(" + " OR ".join(ors) + ")"


def build_fts_where(
    tokens_groups: List[List[str]],
    columns: Sequence[str],
    op_between_groups: str = "OR",
    bind_prefix: str = "fts",
) -> Tuple[str, Dict[str, str]]:
    if not tokens_groups or not columns:
        return "", {}

    sql_groups: List[str] = []
    binds: Dict[str, str] = {}
    idx = 0
    for group in tokens_groups:
        ors = []
        for tok in group:
            token = (tok or "").strip()
            if not token:
                continue
            bind_name = f"{bind_prefix}_{idx}"
            binds[bind_name] = f"%{token}%"
            ors.append(_group_sql_for_token(bind_name, columns))
            idx += 1
        if not ors:
            continue
        if len(ors) == 1:
            sql_groups.append(ors[0])
        else:
            sql_groups.append("(" + " OR ".join(ors) + ")")

    if not sql_groups:
        return "", {}

    joiner = " AND " if (op_between_groups or "OR").upper() == "AND" else " OR "
    return "(" + joiner.join(sql_groups) + ")", binds


def build_fts_like_where(
    columns: Sequence[str],
    tokens: Sequence[str],
    operator: str = "OR",
) -> Tuple[str, Dict[str, str]]:
    token_groups = [[tok] for tok in tokens or []]
    return build_fts_where(token_groups, columns, operator)
