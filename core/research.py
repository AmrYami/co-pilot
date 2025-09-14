from __future__ import annotations
from typing import Any, Dict, List, Optional
import importlib, hashlib
from sqlalchemy import text
from .settings import Settings


class NoopResearcher:
    """Safe stub used when research is disabled or class fails to load."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings

    def search(self, question: str, prefixes: List[str], settings: Settings) -> Dict[str, Any]:
        # Shape expected by pipeline; feel free to extend later
        return {"facts": [], "sources": []}


def load_researcher(settings: Settings):
    if not settings.get("RESEARCH_MODE", scope="namespace"):
        print("[research] disabled via RESEARCH_MODE")
        return None
    cls_path = settings.get("RESEARCHER_CLASS", scope="global")
    if not cls_path:
        print("[research] RESEARCHER_CLASS not set; using NoopResearcher")
        return NoopResearcher(settings)
    try:
        mod_name, cls_name = cls_path.rsplit(".", 1)
        mod = importlib.import_module(mod_name)
        cls = getattr(mod, cls_name)
        inst = cls(settings=settings) if "settings" in getattr(cls, "__init__", (lambda: None)).__code__.co_varnames else cls()
        print(f"[research] loaded: {cls_path}")
        return inst
    except Exception as e:
        print(f"[research] load failed: {e}; using NoopResearcher")
        return NoopResearcher(settings)


def persist_sources_and_link(mem_engine, namespace: str, run_id: int, items: List[Dict[str, Any]]) -> List[int]:
    """
    items: list of {title?, url?/locator?, content/text?, type?}
    Writes to mem_sources and links each to mem_citations as (fact_type='run', fact_id=run_id).
    Returns the list of inserted source ids.
    """
    if not items:
        return []
    source_ids: List[int] = []
    with mem_engine.begin() as c:
        for it in items:
            title = it.get("title") or it.get("url") or "source"
            locator = it.get("url") or it.get("locator")
            content = it.get("content") or it.get("text") or ""
            stype = it.get("type") or "web"

            h = hashlib.sha256((content or "").encode("utf-8")).hexdigest()
            sid = c.execute(
                text(
                    """
                INSERT INTO mem_sources(namespace, source_type, locator, title, content_hash,
                                        is_redistributable, parsed_content, added_at)
                VALUES (:ns, :stype, :loc, :title, :h, true, :content, NOW())
                ON CONFLICT (namespace, content_hash) DO UPDATE
                SET last_accessed = NOW()
                RETURNING id
            """
                ),
                {"ns": namespace, "stype": stype, "loc": locator, "title": title, "h": h, "content": content},
            ).scalar_one()
            source_ids.append(sid)

            c.execute(
                text(
                    """
                INSERT INTO mem_citations(namespace, fact_type, fact_id, source_id, quote, confidence, created_at)
                VALUES (:ns, 'run', :rid, :sid, :quote, :conf, NOW())
            """
                ),
                {
                    "ns": namespace,
                    "rid": run_id,
                    "sid": sid,
                    "quote": (content or "")[:240],
                    "conf": 0.7,
                },
            )
    return source_ids
