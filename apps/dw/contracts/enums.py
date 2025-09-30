# -*- coding: utf-8 -*-
"""
Enum synonym utilities for Contract-specific fields.
All comments and strings inside code are in English only.
"""

from typing import Dict, List, Tuple, Callable, Any

from apps.dw.domain.synonyms import DEFAULT_ENUM_SYNONYMS

DEFAULT_EMPTY_TOKENS = {"", "N/A", "NA", "-"}


def _safe_settings_get(settings_get: Callable[..., Any] | None, key: str):
    if not callable(settings_get):
        return None
    try:
        return settings_get(key, scope="namespace", default={})
    except TypeError:
        try:
            return settings_get(key, scope="namespace")
        except TypeError:
            return settings_get(key)


def load_enum_synonyms(settings_get, table: str, column: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Load enum synonyms for a specific table/column from mem_settings.
    Expected key: DW_ENUM_SYNONYMS with shape:
      {
        "Contract.REQUEST_TYPE": {
           "renewal": {"equals":[...], "prefix":[...], "contains":[...]},
           ...
        }
      }
    """
    cfg = _safe_settings_get(settings_get, "DW_ENUM_SYNONYMS") or {}
    key = f"{table}.{column}"
    raw = cfg.get(key) or DEFAULT_ENUM_SYNONYMS.get(key) or {}
    # Normalize lists and ensure keys exist
    norm: Dict[str, Dict[str, List[str]]] = {}
    for k, v in raw.items():
        if not isinstance(v, dict):
            continue
        norm[k.lower()] = {
            "equals": [x for x in v.get("equals", []) if x is not None],
            "prefix": [x for x in v.get("prefix", []) if x is not None],
            "contains": [x for x in v.get("contains", []) if x is not None],
        }
    return norm


def build_enum_where_clause(
    column: str,
    value: str,
    synonyms: Dict[str, Dict[str, List[str]]],
    bind_prefix: str = "enum",
) -> Tuple[str, Dict[str, str]]:
    """
    Build an Oracle-friendly, case-insensitive WHERE fragment for an enum value.
    - If `value` maps to a synonyms bucket, expand (equals/prefix/contains).
    - Special handling for "null" bucket if provided.
    Returns: (sql_fragment, binds)
    """
    v = (value or "").strip().lower()
    binds: Dict[str, str] = {}

    rules = synonyms.get(v)
    # If no rules for this value, fall back to a simple equals/like guess:
    if not rules:
        if not value:
            return "(1=0)", binds
        # Simple robust fallback: contains match on the value
        b = f":{bind_prefix}_c0"
        binds[f"{bind_prefix}_c0"] = f"%{value.strip()}%"
        return f"(UPPER({column}) LIKE UPPER({b}))", binds

    # Special null bucket: treat as empty/NULL indicators
    if v == "null":
        empties = list(DEFAULT_EMPTY_TOKENS)
        placeholders = []
        for i, tok in enumerate(empties):
            k = f"{bind_prefix}_e{i}"
            binds[k] = tok
            placeholders.append(f"UPPER(TRIM({column})) = UPPER(:{k})")
        return f"(({column} IS NULL) OR " + " OR ".join(placeholders) + ")", binds

    parts = []

    # equals -> IN (...)
    eqs = rules.get("equals") or []
    if eqs:
        in_binds = []
        for i, val in enumerate(eqs):
            if val is None:
                continue
            k = f"{bind_prefix}_eq{i}"
            binds[k] = val
            in_binds.append(f"UPPER(:{k})")
        if in_binds:
            parts.append(f"UPPER({column}) IN (" + ", ".join(in_binds) + ")")

    # prefix -> LIKE 'val%'
    prefs = rules.get("prefix") or []
    for i, val in enumerate(prefs):
        if val is None:
            continue
        k = f"{bind_prefix}_p{i}"
        binds[k] = f"{val}%"
        parts.append(f"UPPER({column}) LIKE UPPER(:{k})")

    # contains -> LIKE '%val%'
    conts = rules.get("contains") or []
    for i, val in enumerate(conts):
        if val is None:
            continue
        k = f"{bind_prefix}_c{i}"
        binds[k] = f"%{val}%"
        parts.append(f"UPPER({column}) LIKE UPPER(:{k})")

    if not parts:
        if not value:
            return "(1=0)", binds
        # As a safety fallback, do contains on the original value
        k = f"{bind_prefix}_c0"
        binds[k] = f"%{value.strip()}%"
        parts.append(f"UPPER({column}) LIKE UPPER(:{k})")

    return "(" + " OR ".join(parts) + ")", binds
