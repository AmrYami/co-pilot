from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _ident(col: str) -> str:
    return col.strip().upper()


def _bind_name(prefix: str, idx: int) -> str:
    return f"{prefix}_{idx}"


def build_where(intent: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    binds: Dict[str, Any] = {}
    parts: List[str] = []

    eq_idx = 0
    for col, vals in intent.get("eq", {}).items():
        colu = _ident(col)
        ors = []
        for v in vals:
            bn = _bind_name("eq", eq_idx)
            eq_idx += 1
            binds[bn] = v.strip().upper()
            ors.append(f"UPPER(TRIM({colu})) = UPPER(:{bn})")
        if ors:
            parts.append("(" + " OR ".join(ors) + ")")

    for col, vals in intent.get("neq", {}).items():
        colu = _ident(col)
        ands = []
        for v in vals:
            bn = _bind_name("neq", eq_idx)
            eq_idx += 1
            binds[bn] = v.strip().upper()
            ands.append(f"UPPER(TRIM({colu})) <> UPPER(:{bn})")
        if ands:
            parts.append("(" + " AND ".join(ands) + ")")

    like_idx = 0
    for col, vals in intent.get("contains", {}).items():
        colu = _ident(col)
        ors = []
        for v in vals:
            bn = _bind_name("like", like_idx)
            like_idx += 1
            binds[bn] = f"%{v.strip().upper()}%"
            ors.append(f"UPPER(NVL({colu},'')) LIKE UPPER(:{bn})")
        if ors:
            parts.append("(" + " OR ".join(ors) + ")")

    for col, vals in intent.get("not_contains", {}).items():
        colu = _ident(col)
        ands = []
        for v in vals:
            bn = _bind_name("nlike", like_idx)
            like_idx += 1
            binds[bn] = f"%{v.strip().upper()}%"
            ands.append(f"UPPER(NVL({colu},'')) NOT LIKE UPPER(:{bn})")
        if ands:
            parts.append("(" + " AND ".join(ands) + ")")

    for grp in intent.get("empty", []):
        conds = [f"TRIM(NVL({_ident(c)},'')) = ''" for c in grp]
        parts.append("(" + " OR ".join(conds) + ")")

    for grp in intent.get("not_empty", []):
        conds = [f"TRIM(NVL({_ident(c)},'')) <> ''" for c in grp]
        parts.append("(" + " AND ".join(conds) + ")")

    for grp in intent.get("empty_any", []):
        cond = "(" + " OR ".join([f"TRIM(NVL({_ident(c)},'')) = ''" for c in grp]) + ")"
        parts.append(cond)

    for grp in intent.get("empty_all", []):
        cond = "(" + " AND ".join([f"TRIM(NVL({_ident(c)},'')) = ''" for c in grp]) + ")"
        parts.append(cond)

    fts_cols: List[str] = intent.get("_fts_columns", [])
    fts_idx = 0
    for tok_group in intent.get("fts_groups", []):
        if not fts_cols:
            continue
        ors = []
        for tok in tok_group:
            bn = _bind_name("fts", fts_idx)
            fts_idx += 1
            binds[bn] = f"%{tok.strip().upper()}%"
            ors.append(
                "(" + " OR ".join([f"UPPER(NVL({_ident(c)},'')) LIKE UPPER(:{bn})" for c in fts_cols]) + ")"
            )
        if ors:
            parts.append("(" + " OR ".join(ors) + ")")

    where_sql = ""
    if parts:
        where_sql = "WHERE (" + ") AND (".join(parts) + ")"
    return where_sql, binds
