from typing import Dict, List, Tuple


def qt(col: str) -> str:
    # استخدم TRIM + UPPER لنصوص equality
    return f"UPPER(TRIM({col}))"


def qn(col: str) -> str:
    # استخدم NVL + UPPER لنصوص LIKE/FTS
    return f"UPPER(NVL({col},''))"


def build_in_any_alias(
    col_key: str,
    values: List[str],
    eq_alias_map: Dict[str, List[str]],
    bind_prefix: str,
    bind_seq: List[Tuple[str, str]],
) -> str:
    """
    OR عبر كل أعمدة الـ alias لمجموعة قيم واحدة:
    (UPPER(TRIM(col1)) IN (UPPER(:b0), UPPER(:b1)) OR UPPER(TRIM(col2)) IN (...))
    """
    aliases = eq_alias_map.get(col_key.upper())
    cols = aliases if aliases else [col_key]

    # نفس الـ binds تُستخدم عبر كل الأعمدة
    bind_names = []
    for v in values:
        name = f"{bind_prefix}_{len(bind_seq)}"
        bind_seq.append((name, v))
        bind_names.append(f":{name}")

    inlist = ", ".join([f"UPPER({bn})" for bn in bind_names])
    parts = [f"{qt(c)} IN ({inlist})" for c in cols]
    return "(" + " OR ".join(parts) + ")"


def build_neq_all(col: str, values: List[str], bind_prefix: str, bind_seq: List[Tuple[str, str]]) -> str:
    # (UPPER(TRIM(col)) <> UPPER(:b0) AND UPPER(TRIM(col)) <> UPPER(:b1))
    parts = []
    for v in values:
        name = f"{bind_prefix}_{len(bind_seq)}"
        bind_seq.append((name, v))
        parts.append(f"{qt(col)} <> UPPER(:{name})")
    return "(" + " AND ".join(parts) + ")"


def build_not_like_all(col: str, tokens: List[str], bind_prefix: str, bind_seq: List[Tuple[str, str]]) -> str:
    # (UPPER(NVL(col,'')) NOT LIKE UPPER(:b0) AND ... )
    parts = []
    for t in tokens:
        name = f"{bind_prefix}_{len(bind_seq)}"
        bind_seq.append((name, f"%{t}%"))
        parts.append(f"{qn(col)} NOT LIKE UPPER(:{name})")
    return "(" + " AND ".join(parts) + ")"


def build_empty_any(cols: List[str]) -> str:
    return "(" + " OR ".join([f"TRIM(NVL({c},'')) = ''" for c in cols]) + ")"


def build_empty_all(cols: List[str]) -> str:
    return "(" + " AND ".join([f"TRIM(NVL({c},'')) = ''" for c in cols]) + ")"


def build_not_empty_all(cols: List[str]) -> str:
    return "(" + " AND ".join([f"TRIM(NVL({c},'')) <> ''" for c in cols]) + ")"


def build_fts_like(
    groups: List[List[str]],
    columns: List[str],
    bind_prefix: str,
    bind_seq: List[Tuple[str, str]],
    groups_op: str = "OR",   # << مهم: OR بين المجموعات
) -> str:
    group_sqls = []
    for g in groups:
        # كل مجموعة g عبارة عن كلمات يجب دمجها بـ AND داخل المجموعة (لو أكتر من كلمة)
        per_group_parts = []
        for token in g:
            name = f"{bind_prefix}_{len(bind_seq)}"
            bind_seq.append((name, f"%{token}%"))
            like_any_col = "(" + " OR ".join([f"{qn(c)} LIKE UPPER(:{name})" for c in columns]) + ")"
            per_group_parts.append(like_any_col)
        # AND داخل المجموعة الواحدة
        group_sqls.append("(" + " AND ".join(per_group_parts) + ")")
    # OR بين المجموعات
    op = " OR " if groups_op.upper() == "OR" else " AND "
    return "(" + op.join(group_sqls) + ")"
