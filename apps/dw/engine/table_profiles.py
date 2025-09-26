from __future__ import annotations

from typing import Dict, List

CONTRACT_TABLE = "Contract"

CONTRACT_COLS = {
    "CONTRACT_ID": "CONTRACT_ID",
    "CONTRACT_OWNER": "CONTRACT_OWNER",
    "OWNER_DEPARTMENT": "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL": "DEPARTMENT_OUL",
    "ENTITY": "ENTITY",
    "ENTITY_NO": "ENTITY_NO",
    "REQUEST_DATE": "REQUEST_DATE",
    "START_DATE": "START_DATE",
    "END_DATE": "END_DATE",
    "VALUE_NET": "CONTRACT_VALUE_NET_OF_VAT",
    "VAT": "VAT",
    "CONTRACT_STATUS": "CONTRACT_STATUS",
}

STAKEHOLDER_COLS: List[str] = [f"CONTRACT_STAKEHOLDER_{i}" for i in range(1, 9)]

DIM_SYNONYMS: Dict[str, str] = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "dept": "OWNER_DEPARTMENT",
    "department_oul": "DEPARTMENT_OUL",
    "manager": "DEPARTMENT_OUL",
    "entity": "ENTITY",
    "entity no": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "status": "CONTRACT_STATUS",
}


def net_sql() -> str:
    return "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def gross_sql() -> str:
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
        "ELSE NVL(VAT,0) END"
    )


def fts_columns(settings, table_name: str = CONTRACT_TABLE) -> List[str]:
    cfg = settings.get("DW_FTS_COLUMNS") or {}
    if isinstance(cfg, str):
        import json

        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}
    return cfg.get(table_name) or cfg.get("*") or []
