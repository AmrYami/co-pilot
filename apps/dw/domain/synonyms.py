# -*- coding: utf-8 -*-
# English-only code & comments.
from __future__ import annotations
from typing import Dict, List, Tuple, Optional


def _upper(s: str) -> str:
    return s.upper().strip()


# Default, overridable via mem_settings key: DW_ENUM_SYNONYMS (scope=namespace)
# Shape:
# {
#   "Contract.REQUEST_TYPE": {
#       "renewal": {
#           "equals": ["RENEWAL", "RENEW", "RENEW CONTRACT"],
#           "prefix": ["RENEW", "EXTENS"],   # EXTENS covers EXTENSION / EXTENDED
#           "contains": []
#       },
#       "addendum": {
#           "equals": ["ADDENDUM", "AMENDMENT"],
#           "prefix": ["AMEND", "APPEND", "MODIF"],
#           "contains": []
#       },
#       "new": {
#           "equals": ["NEW", "NEW CONTRACT"],
#           "prefix": ["NEW", "CREATE"],
#           "contains": []
#       },
#       "termination": {
#           "equals": ["TERMINATION", "CANCELLATION"],
#           "prefix": ["TERMIN", "CANCEL", "CLOSE"],
#           "contains": []
#       }
#   }
# }
DEFAULT_ENUM_SYNONYMS: Dict[str, Dict[str, Dict[str, List[str]]]] = {
    "Contract.REQUEST_TYPE": {
        "renewal": {
            "equals": ["RENEWAL", "RENEW", "RENEW CONTRACT", "RENEWED"],
            "prefix": ["RENEW", "EXTENS"],  # EXTENS* -> EXTENSION/EXTENDED
            "contains": []
        },
        "addendum": {
            "equals": ["ADDENDUM", "AMENDMENT"],
            "prefix": ["AMEND", "APPEND", "MODIF"],  # amendment/appendix/modification
            "contains": []
        },
        "new": {
            "equals": ["NEW", "NEW CONTRACT"],
            "prefix": ["NEW", "CREATE"],
            "contains": []
        },
        "termination": {
            "equals": ["TERMINATION", "CANCELLATION"],
            "prefix": ["TERMIN", "CANCEL", "CLOSE"],
            "contains": []
        },
        # Optional: bucket textual nulls, but NOT used unless user explicitly asks for "null"
        "null": {
            "equals": ["NULL", "N/A", "NA", "-"],
            "prefix": [],
            "contains": []
        }
    }
}


def normalize_value_to_category(table_col: str, user_value: str,
                                cfg: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None
                                ) -> Optional[str]:
    """
    Maps a user-provided value (e.g., 'Renewal') to a canonical category key (e.g., 'renewal').
    Matching is case-insensitive and checks both equals/prefix/contains buckets.
    """
    user = _upper(user_value)
    cfg = (cfg or {}).get(table_col) or DEFAULT_ENUM_SYNONYMS.get(table_col) or {}
    for category, rules in cfg.items():
        # equals
        for eq in rules.get("equals", []):
            if user == _upper(eq):
                return category
        # prefix
        for px in rules.get("prefix", []):
            if user.startswith(_upper(px)):
                return category
        # contains
        for ct in rules.get("contains", []):
            if _upper(ct) in user:
                return category
    # No category matched
    return None


def patterns_for_category(table_col: str, category: str,
                          cfg: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None
                          ) -> Dict[str, List[str]]:
    """
    Returns dict of pattern lists per bucket for a category on a given table.column.
    Example: {"equals": [...], "prefix": [...], "contains": [...]} (all already uppercased).
    """
    rules = ((cfg or {}).get(table_col) or DEFAULT_ENUM_SYNONYMS.get(table_col) or {}).get(category) or {}
    out = {
        "equals": [_upper(x) for x in rules.get("equals", [])],
        "prefix": [_upper(x) for x in rules.get("prefix", [])],
        "contains": [_upper(x) for x in rules.get("contains", [])],
    }
    return out


def build_synonym_filter_sql(column_sql: str,
                             user_value: str,
                             table_col: str,
                             cfg: Optional[Dict[str, Dict[str, Dict[str, List[str]]]]] = None,
                             bind_prefix: str = "rt") -> Tuple[str, Dict[str, object]]:
    """
    Build a robust SQL predicate for a value using synonym sets.
    Returns (sql_fragment, binds).
    Strategy:
      - If value maps to a known category, build OR of:
          UPPER(col) IN (:eq0, :eq1, ...)
          OR UPPER(col) LIKE :px0 (prefix%)
          OR UPPER(col) LIKE :ct0 (%contains%)
      - If no category matches, fallback to UPPER(col) = :eq0
    """
    binds: Dict[str, object] = {}
    parts: List[str] = []

    category = normalize_value_to_category(table_col, user_value, cfg=cfg)
    if category is None:
        # Fallback: strict equality on user value (case-insensitive)
        b = f"{bind_prefix}_eq0"
        binds[b] = _upper(user_value)
        return f"UPPER({column_sql}) = :{b}", binds

    pat = patterns_for_category(table_col, category, cfg=cfg)
    bi = 0

    eq_values = pat.get("equals", []) or []
    if eq_values:
        in_binds = []
        for v in eq_values:
            b = f"{bind_prefix}_eq{bi}"; bi += 1
            binds[b] = v
            in_binds.append(f":{b}")
        parts.append(f"UPPER({column_sql}) IN ({', '.join(in_binds)})")

    px_values = pat.get("prefix", []) or []
    for v in px_values:
        b = f"{bind_prefix}_px{bi}"; bi += 1
        binds[b] = f"{v}%"
        parts.append(f"UPPER({column_sql}) LIKE :{b}")

    ct_values = pat.get("contains", []) or []
    for v in ct_values:
        b = f"{bind_prefix}_ct{bi}"; bi += 1
        binds[b] = f"%{v}%"
        parts.append(f"UPPER({column_sql}) LIKE :{b}")

    if not parts:
        # None configured? Safe fallback
        b = f"{bind_prefix}_eq0"
        binds[b] = _upper(user_value)
        return f"UPPER({column_sql}) = :{b}", binds

    return "(" + " OR ".join(parts) + ")", binds
