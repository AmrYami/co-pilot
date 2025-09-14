from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import hashlib

@dataclass
class SourceDoc:
    source_type: str      # 'internal_doc', 'web', ...
    locator: str          # path or URL
    title: str
    content: str          # parsed text
    is_redistributable: bool = True

@dataclass
class ResearchResult:
    facts: Dict[str, Any]
    sources: List[SourceDoc]
    summary: str

class BaseResearcher:
    def __init__(self, settings, namespace: str):
        self.settings = settings
        self.namespace = namespace

    def search(self, question: str, prefixes: list[str]) -> ResearchResult:
        # default: no-op
        return ResearchResult(facts={}, sources=[], summary="")

# Simple loader from FQCN in settings
def load_researcher(settings, namespace: str) -> Optional[BaseResearcher]:
    fqcn = settings.get("RESEARCHER_CLASS", scope="global", namespace=namespace)
    if not fqcn:
        return None
    mod_name, cls_name = fqcn.rsplit(".", 1)
    mod = __import__(mod_name, fromlist=[cls_name])
    cls = getattr(mod, cls_name)
    return cls(settings=settings, namespace=namespace)
