from __future__ import annotations

from typing import List

# Default allow-list for columns that can be filtered explicitly from feedback.
DEFAULT_EXPLICIT_FILTER_COLUMNS: List[str] = [
    "CONTRACT_STATUS",
    "REQUEST_TYPE",
    "ENTITY",
    "ENTITY_NO",
    "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL",
    "CONTRACT_OWNER",
    "CONTRACT_ID",
]
