"""Full-text search helpers for DW queries."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


VALID_ENGINES = {"like", "contains", "tsvector"}


class FTSEngine:
    """Simple resolver for DW FTS engine configuration."""

    def __init__(self, name: str, *, min_token_len: int = 2) -> None:
        self.name = (name or "like").strip().lower() or "like"
        try:
            self.min_token_len = max(1, int(min_token_len))
        except Exception:
            self.min_token_len = 2

    @staticmethod
    def like(*, min_token_len: int = 2) -> "FTSEngine":
        return FTSEngine("like", min_token_len=min_token_len)

    @staticmethod
    def from_name(name: Optional[str], settings: Optional[Dict[str, Any]] = None) -> "FTSEngine":
        settings = settings or {}
        raw_min = settings.get("DW_FTS_MIN_TOKEN_LEN", 2)
        try:
            min_len = max(1, int(raw_min))
        except Exception:
            min_len = 2

        normalized = (name or "").strip().lower()
        if normalized in ("", "like"):
            return FTSEngine.like(min_token_len=min_len)
        # Placeholder for future engines; default to LIKE for unknown values.
        return FTSEngine.like(min_token_len=min_len)


def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    getter = getattr(cfg, "get", None)
    if callable(getter):
        try:
            value = getter(key, default)
        except TypeError:
            value = getter(key)
        if value is not None:
            return value
    return default


_STOP_WORDS = {"list", "all", "contracts", "where", "has", "have"}


def extract_fts_tokens(question: str) -> List[str]:
    """Backward-compatible token extraction used by legacy planners."""

    tokens, _ = _tokenize_question(question or "")
    return tokens


def build_fts_where(
    question: str,
    settings: Any,
    *,
    table: str = "Contract",
    mode: str = "auto",
) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
    """Return a SQL WHERE fragment for LIKE-based FTS and the bind parameters."""

    columns = list(_load_fts_columns(settings, table))
    if not columns:
        return None, {}, None

    tokens, join_op = _tokenize_question(question or "")
    if not tokens:
        return None, {}, None

    clauses: List[str] = []
    binds: Dict[str, str] = {}

    for idx, token in enumerate(tokens):
        bind = f"fts_{idx}"
        binds[bind] = f"%{token}%"
        per_col = [f"UPPER(TRIM({col})) LIKE UPPER(:{bind})" for col in columns]
        if per_col:
            clauses.append("(" + " OR ".join(per_col) + ")")

    if not clauses:
        return None, {}, None

    where_sql = "(" + f" {join_op} ".join(clauses) + ")"
    return where_sql, binds, join_op


def _tokenize_question(question: str) -> Tuple[List[str], str]:
    text = " ".join((question or "").split()).lower()
    if not text:
        return [], "OR"

    if " and " in text:
        parts = re.split(r"\s+and\s+", text)
        op = "AND"
    elif " or " in text:
        parts = re.split(r"\s+or\s+", text)
        op = "OR"
    else:
        parts = [text]
        op = "OR"

    tokens: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for segment in re.split(r"[,/;]\s*", part):
            words = [w for w in segment.split() if w and w not in _STOP_WORDS]
            if not words:
                continue
            phrase = " ".join(words)
            phrase = phrase.strip("'\"")
            if not phrase or phrase in seen:
                continue
            seen.add(phrase)
            tokens.append(phrase)
    return tokens, op


def _load_fts_columns(settings: Any, table: str) -> Iterable[str]:
    raw = _settings_get(settings, "DW_FTS_COLUMNS", {}) or {}
    columns: List[str] = []
    if isinstance(raw, dict):
        candidates = [table, table.strip('"'), table.upper(), table.lower(), "*"]
        for key in candidates:
            vals = raw.get(key)
            if isinstance(vals, list):
                columns.extend(vals)
    elif isinstance(raw, list):
        columns.extend(raw)

    for col in columns:
        norm = _normalize_column(col)
        if norm:
            yield norm


def _normalize_column(col: Any) -> Optional[str]:
    if not isinstance(col, str):
        return None
    cleaned = col.strip()
    if not cleaned:
        return None
    if cleaned.startswith('"') and cleaned.endswith('"'):
        return cleaned
    cleaned = cleaned.replace(" ", "_")
    if "." in cleaned:
        return ".".join(_normalize_column(part) or part for part in cleaned.split("."))
    return f'"{cleaned.upper()}"'


def _settings_get(settings: Any, key: str, default: Any = None) -> Any:
    if settings is None:
        return default
    getter_json = getattr(settings, "get_json", None)
    if callable(getter_json):
        try:
            value = getter_json(key, default)
        except TypeError:
            value = getter_json(key)
        if value is not None:
            return value
    getter = getattr(settings, "get", None)
    if callable(getter):
        try:
            value = getter(key, default)
        except TypeError:
            value = getter(key)
        if value is not None:
            return value
    if isinstance(settings, dict):
        return settings.get(key, default)
    return default


try:  # pragma: no cover - optional dependency in some deployments
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover - fall back to a safe stub
    def _get_setting(*args, **kwargs):
        return {}


STOP_WORDS = {"the", "a", "an", "of", "for", "to", "in", "on", "at", "by", "and", "or"}


def _split_or_and(text: str) -> List[List[str]]:
    """Split text into OR groups that contain AND-constrained tokens."""

    groups: List[List[str]] = []
    for or_part in re.split(r"(?i)\bor\b", text):
        tokens = [seg.strip() for seg in re.split(r"(?i)\band\b", or_part)]
        tokens = [tok for tok in tokens if tok and tok.lower() not in STOP_WORDS]
        if tokens:
            groups.append(tokens)
    return groups


def build_fts_tokens(q: str) -> List[List[str]]:
    """Normalize question text into OR-of-AND token groups for FTS."""

    text = re.sub(r"\s+", " ", q or "").strip()
    if not text:
        return []
    return _split_or_and(text)


def get_fts_columns(schema_key: str) -> List[str]:
    """Return configured FTS columns for ``schema_key`` with sane fallbacks."""

    raw = _get_setting("DW_FTS_COLUMNS", scope="namespace", namespace="dw::common") or {}
    cols = raw.get(schema_key) or raw.get(schema_key.upper()) or raw.get("*") or []
    seen: set[str] = set()
    result: List[str] = []
    for col in cols:
        if not isinstance(col, str):
            continue
        normalized = col.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    if not result:
        result = ["CONTRACT_SUBJECT", "CONTRACT_PURPOSE"]
    return result


def build_like_fts_where(
    schema_key: str,
    groups: List[List[str]],
    *,
    bind_prefix: str = "fts",
) -> Tuple[str, Dict[str, str]]:
    """Construct a LIKE-based WHERE clause for FTS token groups."""

    columns = get_fts_columns(schema_key)
    if not columns or not groups:
        return "", {}

    binds: Dict[str, str] = {}
    or_clauses: List[str] = []
    bind_i = 0

    for and_tokens in groups:
        and_clauses: List[str] = []
        for token in and_tokens:
            if not token:
                continue
            bind_name = f"{bind_prefix}_{bind_i}"
            bind_i += 1
            binds[bind_name] = f"%{token}%"
            per_token = " OR ".join(
                f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in columns
            )
            and_clauses.append(f"({per_token})")
        if and_clauses:
            or_clauses.append("(" + " AND ".join(and_clauses) + ")")

    if not or_clauses:
        return "", {}

    where_sql = "(" + " OR ".join(or_clauses) + ")"
    return where_sql, binds


def build_fts_clause(
    table: str,
    groups: List[List[str]],
    operator: str,
    cfg: Any,
) -> Tuple[str, Dict[str, str], List[str]]:
    """Return a SQL fragment for FTS with bind parameters and used columns."""

    raw_engine = _cfg_get(cfg, "DW_FTS_ENGINE", "like")
    engine = str(raw_engine or "like").lower()
    if engine not in VALID_ENGINES:
        engine = "like"

    columns_setting = _cfg_get(cfg, "DW_FTS_COLUMNS", {}) or {}
    columns_cfg = columns_setting if isinstance(columns_setting, dict) else {}

    specific = (
        columns_cfg.get(table)
        or columns_cfg.get(table.strip('"'))
        or columns_cfg.get(table.upper())
        or columns_cfg.get(table.lower())
    )
    fallback = columns_cfg.get("*")
    ordered_cols: List[str] = []
    seen_cols: set[str] = set()
    for collection in (specific, fallback):
        if not isinstance(collection, list):
            continue
        for col in collection:
            normalized = str(col or "").strip()
            if not normalized or normalized in seen_cols:
                continue
            seen_cols.add(normalized)
            ordered_cols.append(normalized)

    if not ordered_cols:
        return "", {}, []

    min_len_raw = _cfg_get(cfg, "DW_FTS_MIN_TOKEN_LEN", 2)
    try:
        min_len = int(min_len_raw)
    except (TypeError, ValueError):
        min_len = 2
    if min_len < 0:
        min_len = 0

    binds: Dict[str, str] = {}
    used_tokens: List[str] = []
    group_clauses: List[str] = []

    for group in groups or []:
        token_clauses: List[str] = []
        for raw in group or []:
            token = str(raw or "").strip()
            if len(token) < min_len:
                continue
            bind_name = f"fts_{len(used_tokens)}"
            used_tokens.append(token)
            binds[bind_name] = f"%{token}%"
            comparisons = [
                f"UPPER(NVL({column},'')) LIKE UPPER(:{bind_name})" for column in ordered_cols
            ]
            token_clauses.append("(" + " OR ".join(comparisons) + ")")
        if token_clauses:
            group_clauses.append("(" + " OR ".join(token_clauses) + ")")

    if not group_clauses:
        return "", {}, []

    glue = " OR " if str(operator or "").upper() == "OR" else " AND "
    clause = "(" + glue.join(group_clauses) + ")"
    return clause, binds, ordered_cols


__all__ = [
    "extract_fts_tokens",
    "build_fts_where",
    "build_fts_tokens",
    "get_fts_columns",
    "build_like_fts_where",
    "build_fts_clause",
]
