import re
from typing import List, Dict, Any, Tuple

_RE_TOKEN = re.compile(r"[A-Za-z0-9_]{2,}")
_STOP = {"the", "and", "or", "of", "a", "an", "by", "per", "for", "to", "in", "on"}


def tokenize(q: str) -> List[str]:
    toks = [t.upper() for t in _RE_TOKEN.findall(q or "") if t.lower() not in _STOP]
    return toks


def load_columns(settings, table: str) -> List[str]:
    cfg = settings.get("DW_FTS_COLUMNS") or {}
    if isinstance(cfg, str):
        # If stored as text JSON by caller, Settings already parses to object; just be safe
        return []
    cols = cfg.get(table) or cfg.get("*") or []
    return cols


def build_predicate(cols: List[str], tokens: List[str]) -> Tuple[str, Dict[str, Any]]:
    if not cols or not tokens:
        return "", {}
    ors = []
    binds: Dict[str, Any] = {}
    k = 0
    for c in cols:
        for t in tokens:
            k += 1
            name = f"fts{k}"
            ors.append(f"UPPER({c}) LIKE :{name}")
            binds[name] = f"%{t}%"
    return "(" + " OR ".join(ors) + ")", binds
