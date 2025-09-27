from __future__ import annotations

from typing import List


def expr_net() -> str:
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def expr_gross() -> str:
    # VAT can be absolute or ratio [0..1]
    return ("NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
            "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
            "ELSE NVL(VAT,0) END")


def pred_request_window() -> str:
    return "REQUEST_DATE BETWEEN :date_start AND :date_end"


def pred_overlap_strict() -> str:
    # Only contracts with valid span
    return "(START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= :date_end AND END_DATE >= :date_start)"


def pred_expiring_window() -> str:
    return "END_DATE BETWEEN :date_start AND :date_end"


def window_predicate(date_mode: str) -> str:
    if date_mode == "REQUEST_DATE":
        return pred_request_window()
    if date_mode == "END_DATE":
        return pred_expiring_window()
    # default overlap
    return pred_overlap_strict()


def select_all() -> str:
    return "*"


def select_basic_contract() -> str:
    return "CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE, START_DATE, END_DATE"


def order_clause(expr: str, desc: bool = True) -> str:
    return f"ORDER BY {expr} {'DESC' if desc else 'ASC'}"


def limit_clause() -> str:
    # Oracle 12c+
    return "FETCH FIRST :top_n ROWS ONLY"


def union_stakeholders(slots: int, measure_expr: str, where_pred: str | None = None) -> str:
    # Flatten stakeholder_1..stakeholder_N to one column and reuse measure_expr
    parts = []
    for i in range(1, slots + 1):
        col = f"CONTRACT_STAKEHOLDER_{i}"
        w = f"WHERE {where_pred}" if where_pred else ""
        parts.append(f"SELECT {col} AS STAKEHOLDER, {measure_expr} AS MEASURE FROM \"Contract\" {w}")
    return "\nUNION ALL\n".join(parts)
