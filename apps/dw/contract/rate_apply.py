from typing import Dict, List

from apps.dw.rate_grammar import RateHints


def apply_rate_hints_to_contract(hints: RateHints,
                                 where_parts: List[str],
                                 binds: Dict[str, object],
                                 bind_maker):
    def txt_expr(col: str, ci: bool, trim: bool) -> str:
        expr = col.upper()
        if trim:
            expr = f"TRIM({expr})"
        if ci:
            expr = f"UPPER({expr})"
        return expr

    # equality filters
    for i, f in enumerate(hints.eq_filters):
        b = bind_maker(f"rh_eq_{i}")
        binds[b] = f.value
        lhs = txt_expr(f.col, f.ci, f.trim)
        rhs = f":{b}"
        if f.ci:
            rhs = f"UPPER({rhs})"
        if f.trim:
            rhs = f"TRIM({rhs})"
        where_parts.append(f"{lhs} = {rhs}")

    # like filters
    for i, f in enumerate(hints.like_filters):
        b = bind_maker(f"rh_like_{i}")
        binds[b] = f"%{f.pattern}%"
        lhs = txt_expr(f.col, f.ci, f.trim)
        op = "LIKE"
        where_parts.append(f"{lhs} {op} :{b}")
