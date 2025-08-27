# core/research.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import importlib

class Researcher:
    def search(self, question: str, context: Dict[str, Any]) -> Tuple[str, List[int]]:
        return "", []

class NullResearcher(Researcher):
    pass

def _load_class(path: str):
    mod, cls = path.split(":")
    return getattr(importlib.import_module(mod), cls)

def build_researcher(settings) -> Optional[Any]:
    try:
        if not bool(settings.get("RESEARCH_MODE", False)):
            return None
    except Exception:
        return None

    # Highest precedence: explicit class
    class_path = settings.get("RESEARCHER_CLASS")
    if class_path:
        try:
            return _load_class(class_path)(settings)
        except Exception:
            return NullResearcher()

    # Otherwise provider key
    provider = (settings.get("RESEARCH_PROVIDER", "disabled") or "disabled").lower()
    if provider in ("disabled", "none"):
        return None
    if provider in ("null", "stub"):
        return NullResearcher()

    # Future: add real providers here
    return NullResearcher()
