from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency
    from sqlalchemy import inspect
    from sqlalchemy.engine import Engine
    from sqlalchemy.sql.sqltypes import String, Text
except Exception:  # pragma: no cover - lightweight fallback
    inspect = None  # type: ignore[assignment]
    Engine = object  # type: ignore[assignment]

    class _DummyType:  # pragma: no cover - sentinel for isinstance checks
        pass

    String = Text = _DummyType  # type: ignore[assignment]

from .utils import env_flag, env_int

_STOPWORDS = {
    "the",
    "a",
    "an",
    "by",
    "for",
    "of",
    "to",
    "in",
    "on",
    "at",
    "with",
    "last",
    "next",
    "this",
    "that",
    "these",
    "those",
    "and",
    "or",
    "per",
    "value",
    "contract",
    "contracts",
}


_QUOTE_PATTERN = re.compile(r'"([^"]+)"|“([^”]+)”|\'([^\']+)\'')
_WORD_PATTERN = re.compile(r"[A-Za-z0-9_]+")


def _normalise_token(token: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_ ]+", " ", token or "").strip().lower()
    return re.sub(r"\s+", " ", cleaned)


def extract_search_tokens(question: str, min_len: int | None = None) -> List[str]:
    """Tokenise a natural language question for full-text search."""

    if not question:
        return []

    min_len = min_len if min_len is not None else env_int("DW_FTS_MIN_TOKEN_LEN", 3)

    tokens: List[str] = []
    seen: set[str] = set()

    remainder_parts: List[str] = []
    last_index = 0
    for match in _QUOTE_PATTERN.finditer(question):
        phrase = next((g for g in match.groups() if g), "")
        norm = _normalise_token(phrase)
        if norm and len(norm.replace(" ", "")) >= min_len and norm not in seen:
            seen.add(norm)
            tokens.append(norm)
        remainder_parts.append(question[last_index : match.start()])
        last_index = match.end()
    remainder_parts.append(question[last_index:])

    remainder = " ".join(remainder_parts).lower()
    for word in _WORD_PATTERN.findall(remainder):
        if word in _STOPWORDS or len(word) < min_len:
            continue
        if word not in seen:
            seen.add(word)
            tokens.append(word)

    return tokens


def _string_columns(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
    max_cols: int | None = None,
) -> List[str]:
    if inspect is None:  # pragma: no cover - fallback when SQLAlchemy missing
        return []

    try:
        insp = inspect(engine)
        cols = insp.get_columns(table_name, schema=schema)
    except Exception:
        return []

    names: List[str] = []
    for col in cols:
        col_type = col.get("type")
        if isinstance(col_type, (String, Text)):
            names.append(col["name"])
    limit = max_cols if max_cols is not None else env_int("DW_FTS_MAX_COLS", 80)
    return names[:limit]


def build_fulltext_where(
    engine: Engine,
    table_name: str,
    tokens: List[str],
    schema: str | None = None,
) -> Tuple[str, Dict[str, Any], List[str]]:
    """Build a composite predicate scanning all string columns."""

    if not tokens:
        return "", {}, []

    columns = _string_columns(engine, table_name, schema=schema)
    if not columns:
        return "", {}, []

    binds: Dict[str, Any] = {}
    clauses: List[str] = []

    for idx, token in enumerate(tokens, start=1):
        bind_name = f"kw{idx}"
        binds[bind_name] = f"%{token}%"
        ors = [f'LOWER("{col}") LIKE :{bind_name}' for col in columns]
        clauses.append("(" + " OR ".join(ors) + ")")

    return "(" + " AND ".join(clauses) + ")", binds, columns


def inject_fulltext_where(sql_text: str, predicate: str) -> str:
    """Inject the predicate into the SQL before ORDER/GROUP/FETCH clauses."""

    if not predicate:
        return sql_text

    lower = sql_text.lower()
    insert_pos = len(sql_text)
    for keyword in ("\nfetch ", "\norder by", "\ngroup by"):
        idx = lower.find(keyword)
        if idx != -1 and idx < insert_pos:
            insert_pos = idx

    head = sql_text[:insert_pos].rstrip()
    tail = sql_text[insert_pos:]

    if " where " in head.lower():
        head = head + "\nAND " + predicate
    else:
        head = head + "\nWHERE " + predicate

    if tail:
        tail = tail.lstrip("\n")
        return head + "\n" + tail
    return head


def is_fulltext_allowed() -> bool:
    """Gatekeeper for enabling full-text search."""

    return env_flag("DW_FTS_ALLOW", True)
