import re
from typing import Dict, List, Sequence, Tuple

DEFAULT_CONTRACT_FTS_COLUMNS = [
    "CONTRACT_SUBJECT",
    "CONTRACT_PURPOSE",
    "OWNER_DEPARTMENT",
    "DEPARTMENT_OUL",
    "CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "LEGAL_NAME_OF_THE_COMPANY",
    "ENTITY",
    "ENTITY_NO",
    "REPRESENTATIVE_EMAIL",
]

AND_RE = re.compile(r"\band\b", flags=re.IGNORECASE)


def resolve_fts_columns(
    settings_getter, table_name: str, default_columns: Sequence[str] | None = None
) -> List[str]:
    """
    Resolve FTS columns for a given table using settings:
      - DW_FTS_COLUMNS[table]
      - DW_FTS_COLUMNS["*"]
      - fallback default list if both are empty/missing
    """
    default_columns = list(default_columns or DEFAULT_CONTRACT_FTS_COLUMNS)

    cols_map = settings_getter("DW_FTS_COLUMNS", {}) or {}
    if not isinstance(cols_map, dict):
        # Unexpected shape; fallback
        return default_columns

    # Try exact table key, then wildcard "*"
    cols = cols_map.get(table_name) or cols_map.get("*") or []
    if isinstance(cols, list) and cols:
        return cols

    # Fallback
    return default_columns


def build_boolean_fts_where(
    question_text: str,
    terms: Sequence[str],
    fts_columns: Sequence[str],
    binds: Dict[str, str],
    bind_prefix: str = "fts"
) -> Tuple[str, Dict[str, str], str]:
    """
    Build a boolean FTS WHERE clause:
      - Join across columns with OR (any column may match a term)
      - Join across terms with AND if 'and' appears in the question text; otherwise OR.
      - Terms are matched with LIKE and wrapped in %...%.
    Returns: (where_sql, binds, join_op)
    """
    if not terms:
        return "", binds, "OR"

    # Decide join operator across terms based on presence of 'and' in the question.
    join_op = "AND" if AND_RE.search(question_text or "") else "OR"

    # For each term -> (col1 LIKE :b OR col2 LIKE :b OR ...)
    term_predicates: List[str] = []
    for i, term in enumerate(terms):
        bind_name = f"{bind_prefix}_{i}"
        binds[bind_name] = f"%{term.strip()}%"
        col_predicates = [f"UPPER(TRIM({col})) LIKE UPPER(:{bind_name})" for col in fts_columns]
        term_predicates.append("(" + " OR ".join(col_predicates) + ")")

    where_sql = f" {join_op} ".join(term_predicates)
    return where_sql, binds, join_op
