# core/research.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import importlib

from .settings import Settings, get_research_policy

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


def maybe_research(settings: Settings, question: str, datasource_name: str | None = None) -> str | None:
    if not settings.get("RESEARCH_MODE"):
        return None
    policy = get_research_policy(settings)
    if datasource_name and policy:
        if not policy.get(datasource_name, False):
            return None
    # load researcher class if provided
    cls_path = settings.get("RESEARCHER_CLASS")
    if not cls_path:
        return None
    mod, _, cls = cls_path.rpartition(".")
    if not mod or not cls:
        return None
    C = getattr(importlib.import_module(mod), cls)
    return C().run(question)
