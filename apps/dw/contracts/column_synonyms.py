# -*- coding: utf-8 -*-
"""
Column synonyms and helpers.
Everything here is table-specific to Contract → keep in contracts/ namespace.
"""

from typing import Optional

# Canonical column names for Contract table
CONTRACT_STAKEHOLDER_COLS = [
    "CONTRACT_STAKEHOLDER_1", "CONTRACT_STAKEHOLDER_2", "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4", "CONTRACT_STAKEHOLDER_5", "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7", "CONTRACT_STAKEHOLDER_8",
]

# Human → column canonical name (lowercased keys for robust matching)
_COLUMN_SYNONYMS = {
    # departments / owner department
    "department": "OWNER_DEPARTMENT",
    "departments": "OWNER_DEPARTMENT",
    "owner department": "OWNER_DEPARTMENT",
    "owner_department": "OWNER_DEPARTMENT",
    "owner-department": "OWNER_DEPARTMENT",

    # department OUL
    "department_oul": "DEPARTMENT_OUL",
    "department oul": "DEPARTMENT_OUL",
    "oul": "DEPARTMENT_OUL",

    # request type / status / requester
    "request type": "REQUEST_TYPE",
    "request_type": "REQUEST_TYPE",
    "status": "CONTRACT_STATUS",
    "contract status": "CONTRACT_STATUS",
    "contract_status": "CONTRACT_STATUS",
    "requester": "REQUESTER",

    # entity
    "entity": "ENTITY",
    "entity_no": "ENTITY_NO",

    # contract owner / id
    "contract owner": "CONTRACT_OWNER",
    "contract_owner": "CONTRACT_OWNER",
    "contract id": "CONTRACT_ID",
    "contract_id": "CONTRACT_ID",

    # stakeholders (with common misspells)
    "stakeholder": "STAKEHOLDER*",     # special token → expands to the 1..8 columns
    "stakeholders": "STAKEHOLDER*",
    "stackholder": "STAKEHOLDER*",
    "stackholders": "STAKEHOLDER*",
}

def normalize_column_name(human_name: str) -> Optional[str]:
    """
    Normalize a human-friendly column reference to canonical column name.
    Returns None if not recognized.
    Special return "STAKEHOLDER*" means expand to CONTRACT_STAKEHOLDER_1..8 elsewhere.
    """
    if not human_name:
        return None
    key = human_name.strip().lower()
    return _COLUMN_SYNONYMS.get(key)
