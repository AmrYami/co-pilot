"""Common SQL helpers for deterministic DW queries."""

from __future__ import annotations

from typing import Optional, Tuple


GROSS_SQL = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
    "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0)*NVL(VAT,0) ELSE NVL(VAT,0) END"
)

NET_SQL = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"

_SAFE_GROUP_BY = {
    "OWNER_DEPARTMENT": "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL": "DEPARTMENT_OUL",
    "ENTITY": "ENTITY",
    "ENTITY_NO": "ENTITY_NO",
    "CONTRACT_STATUS": "CONTRACT_STATUS",
    "REQUEST_TYPE": "REQUEST_TYPE",
    "CONTRACT_OWNER": "CONTRACT_OWNER",
    "REQUESTER": "REQUESTER",
    "YEAR": "YEAR",
}

_GROUP_BY_SYNONYMS = {
    "department": "OWNER_DEPARTMENT",
    "owner department": "OWNER_DEPARTMENT",
    "oul": "DEPARTMENT_OUL",
    "entity no": "ENTITY_NO",
    "entity number": "ENTITY_NO",
    "status": "CONTRACT_STATUS",
    "request type": "REQUEST_TYPE",
    "owner": "CONTRACT_OWNER",
}


def resolve_group_by(value: Optional[str]) -> Optional[str]:
    """Return a safe group-by column or ``None`` if unsupported."""

    if not value:
        return None
    text = str(value).strip().strip('"')
    if not text:
        return None
    upper = text.upper()
    if upper in _SAFE_GROUP_BY:
        return _SAFE_GROUP_BY[upper]
    synonym = _GROUP_BY_SYNONYMS.get(text.lower())
    if synonym:
        return synonym
    return None


def pick_measure_sql(gross: bool, *, aggregate: bool = False) -> Tuple[str, str]:
    """Return ``(sql_expression, alias)`` for the requested value basis."""

    expr = GROSS_SQL if gross else NET_SQL
    if aggregate:
        return f"SUM({expr})", "MEASURE"
    return expr, "MEASURE"

