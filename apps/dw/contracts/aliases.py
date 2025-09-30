from __future__ import annotations

# Canonical mapping from user-facing labels to DB column names.
ALIAS_TO_COLUMN = {
    # Request type variants
    "request type": "REQUEST_TYPE",
    "request_type": "REQUEST_TYPE",
    "requesttype": "REQUEST_TYPE",
    "request-type": "REQUEST_TYPE",

    # Common other columns (extend as needed)
    "contract status": "CONTRACT_STATUS",
    "contract_status": "CONTRACT_STATUS",
    "contractstatus": "CONTRACT_STATUS",

    "entity no": "ENTITY_NO",
    "entity_no": "ENTITY_NO",
    "entityno": "ENTITY_NO",

    "owner department": "OWNER_DEPARTMENT",
    "owner_department": "OWNER_DEPARTMENT",
    "ownerdepartment": "OWNER_DEPARTMENT",
}

def canonicalize_column(raw: str) -> str | None:
    """Normalize a raw column phrase to the DB column name."""
    # Keep only alphanumerics -> spaces, then collapse multiple spaces
    key = "".join(ch.lower() if ch.isalnum() else " " for ch in raw or "")
    key = " ".join(key.split())
    return ALIAS_TO_COLUMN.get(key)
