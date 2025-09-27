from __future__ import annotations

from typing import Dict, List

# All comments/strings in code are English only.

# Canonical status synonyms (uppercased compare)
STATUS_SYNONYMS: Dict[str, List[str]] = {
    "EXPIRE":   ["EXPIRE", "EXPIRED", "EXPIRING", "ENDED", "END", "TERMINATED"],
    "ACTIVE":   ["ACTIVE", "RUNNING", "IN PROGRESS", "ONGOING"],
    "RENEWAL":  ["RENEWAL", "RENEW", "EXTENDED", "EXTENSION"],
}


def expand_status(value: str) -> List[str]:
    v = (value or "").strip().upper()
    for key, alts in STATUS_SYNONYMS.items():
        if v == key or v in alts:
            return list(dict.fromkeys([key] + alts))
    return [v]
