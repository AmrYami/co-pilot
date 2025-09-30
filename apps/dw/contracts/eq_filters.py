from __future__ import annotations
import re
from typing import Dict, List, Tuple, Optional

_EQU_OP_RE = re.compile(
    r"""(?ix)
    \b
    (?P<lhs>[A-Z0-9_ \-./@]+?)     # column-ish (allow spaces/underscores)
    \s*(?:=|==|:|is|equals)\s*     # equality cue
    ['"]?(?P<rhs>[A-Z0-9_ \-./@]+)['"]?  # value (simple)
    \b
    """
)

def _norm(s: str) -> str:
    """Normalize tokens to compare columns: uppercase and strip non-alnum."""
    return re.sub(r'[^A-Z0-9]', '', s.upper())


def _canonical_col(candidate: str, allow_map: Dict[str, str]) -> Optional[str]:
    """Map free-form lhs to a canonical COLUMN name using allow_map (normalized)."""
    key = _norm(candidate)
    return allow_map.get(key)


def _build_allow_map(explicit_cols: List[str], fts_cols: List[str]) -> Dict[str, str]:
    """Build a normalization map from various forms -> canonical column."""
    allow = {}
    for col in list(dict.fromkeys((explicit_cols or []) + (fts_cols or []))):
        allow[_norm(col)] = col  # keep canonical uppercase/underscore name as-is
        # Also map a human form "REQUEST TYPE" to "REQUEST_TYPE"
        human = col.replace('_', ' ')
        allow[_norm(human)] = col
    return allow


def _collect_synonym_tokens(
    table: str,
    column: str,
    user_value: str,
    enum_syn: Dict
) -> List[str]:
    """
    Collect LIKE tokens for a column:
      - from DW_ENUM_SYNONYMS (equals/prefix/contains)
      - plus the user-specified value (as contains token)
    All tokens returned as %...% or ...% strings (without UPPER applied).
    """
    tokens: List[str] = []

    # Always include user value as contains token
    uv = user_value.strip()
    if uv:
        tokens.append(f"%{uv}%")

    # Look under "Table.Column" key if present
    syn_for_col = enum_syn.get(f"{table}.{column}") or enum_syn.get(column) or {}
    # syn_for_col is a dict of categories; values contain equals/prefix/contains lists
    for _cat, spec in syn_for_col.items():
        eqs = (spec or {}).get("equals", [])
        pfx = (spec or {}).get("prefix", [])
        cts = (spec or {}).get("contains", [])
        for v in eqs:
            v = (v or "").strip()
            if v:
                tokens.append(f"%{v}%")  # treat equals also as contains for recall
        for v in pfx:
            v = (v or "").strip()
            if v:
                tokens.append(f"{v}%")   # prefix
        for v in cts:
            v = (v or "").strip()
            if v:
                tokens.append(f"%{v}%")  # contains

    # de-duplicate while preserving order
    seen = set()
    uniq = []
    for t in tokens:
        if t.lower() not in seen:
            uniq.append(t)
            seen.add(t.lower())
    return uniq


def detect_explicit_equality_filters(
    question: str,
    *,
    table: str,
    explicit_cols_setting: List[str] | Dict[str, List[str]],
    fts_setting: Dict[str, List[str]] | List[str],
    enum_syn: Dict
) -> Tuple[str, Dict[str, str], Optional[str]]:
    """
    Return (where_sql_fragment, binds, suggested_order_by_column)

    - explicit_cols_setting: the DW_EXPLICIT_FILTER_COLUMNS value (list or per-table dict)
    - fts_setting: DW_FTS_COLUMNS value (per-table dict or list)
    - enum_syn: DW_ENUM_SYNONYMS value
    """
    # Resolve allow-lists
    if isinstance(explicit_cols_setting, dict):
        explicit_cols = explicit_cols_setting.get(table, [])
    else:
        explicit_cols = explicit_cols_setting or []

    if isinstance(fts_setting, dict):
        fts_cols = fts_setting.get(table, fts_setting.get("*", []))
        if isinstance(fts_cols, dict):  # safety
            fts_cols = []
    else:
        fts_cols = fts_setting or []

    allow_map = _build_allow_map(explicit_cols, fts_cols)

    # Extract all "lhs op rhs" pairs from the question
    conditions = []
    binds: Dict[str, str] = {}
    bind_idx = 0
    suggested_order_col: Optional[str] = None

    for m in _EQU_OP_RE.finditer(question or ""):
        lhs_raw = (m.group("lhs") or "").strip()
        rhs_raw = (m.group("rhs") or "").strip()

        col = _canonical_col(lhs_raw, allow_map)
        if not col:
            continue  # skip unknown columns

        # Suggested default order if REQUEST_DATE present in dataset and not explicitly requested otherwise
        # We can suggest REQUEST_DATE DESC for most listing questions.
        if suggested_order_col is None and col != "REQUEST_DATE":
            suggested_order_col = "REQUEST_DATE"

        # Build LIKE tokens (synonyms + user value)
        tokens = _collect_synonym_tokens(table, col, rhs_raw, enum_syn)
        if not tokens:
            # Fallback to plain equals (case-insensitive)
            bname = f"eq_{col.lower()}_{bind_idx}"
            bind_idx += 1
            binds[bname] = rhs_raw
            conditions.append(f"UPPER(TRIM({col})) = UPPER(:{bname})")
            continue

        # Build OR ... LIKE ... group
        like_terms = []
        for t in tokens:
            bname = f"like_{col.lower()}_{bind_idx}"
            bind_idx += 1
            binds[bname] = t
            like_terms.append(f"UPPER(TRIM({col})) LIKE UPPER(:{bname})")

        if like_terms:
            conditions.append("(" + " OR ".join(like_terms) + ")")

    where_sql = " AND ".join(conditions)
    return where_sql, binds, suggested_order_col
