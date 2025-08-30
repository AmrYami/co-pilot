# core/research.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple

class Researcher:
    """No-op base. Apps can subclass and plug in (web, docs, kb)."""
    def search(self, question: str, context: Dict[str, Any]) -> Tuple[str, List[int]]:
        """
        Return (summary_text, source_ids). Implementations should write sources
        to mem_sources and return their IDs; core will store IDs in mem_inquiries.
        """
        return "", []
