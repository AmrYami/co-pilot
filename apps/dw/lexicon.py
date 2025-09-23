from __future__ import annotations

import re

# Dimension synonyms → column names (DW "Contract" table)
DIMENSION_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
    "status": "CONTRACT_STATUS",
}

# Natural language → selectable columns
PROJECTION_MAP = {
    "contract id": "CONTRACT_ID",
    "id": "CONTRACT_ID",
    "owner": "CONTRACT_OWNER",
    "owner department": "OWNER_DEPARTMENT",
    "request date": "REQUEST_DATE",
    "start date": "START_DATE",
    "end date": "END_DATE",
    "status": "CONTRACT_STATUS",
    "net value": "CONTRACT_VALUE_NET_OF_VAT",
    "contract value": "CONTRACT_VALUE_NET_OF_VAT",
    "vat": "VAT",
    # special token; expanded into expression when building SQL
    "gross value": "__GROSS__",
}

EXPIRE_WORDS = re.compile(r"\b(expire|expiring)\b", re.I)
REQUESTED_WORDS = re.compile(r"\b(requested|request date)\b", re.I)
