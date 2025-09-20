import re

_SQL_FENCE_RE = re.compile(r"```sql\s*(.*?)\s*```", re.IGNORECASE | re.DOTALL)
_BIND_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")

_ILLEGAL_START = re.compile(r"^\s*(UPDATE|DELETE|INSERT|MERGE|CREATE|ALTER|DROP|TRUNCATE)\b", re.IGNORECASE)

def extract_sql_from_fenced(text: str) -> str:
    """
    Accept only fenced SQL blocks. If none, return empty to force repair/clarification.
    """
    if not text:
        return ""
    m = _SQL_FENCE_RE.search(text)
    if not m:
        return ""
    sql = m.group(1).strip()
    return sql

def find_named_binds(sql: str) -> list[str]:
    return sorted(set(_BIND_RE.findall(sql))) if sql else []

def validate_sql(sql: str, allow_binds: set[str] | None = None) -> dict:
    """
    Very lightweight safety gate:
      - Must start with SELECT or WITH
      - Must not start with DML/DDL
      - If binds are present, all must be in allow list (when provided)
    """
    if not sql:
        return {"ok": False, "errors": ["empty_sql"], "binds": []}

    head = sql.strip()[:6].upper()
    if head not in ("SELECT", "WITH"):
        return {"ok": False, "errors": ["not_select"], "binds": []}

    if _ILLEGAL_START.match(sql or ""):
        return {"ok": False, "errors": ["illegal_command"], "binds": []}

    binds = find_named_binds(sql)
    errors = []
    if allow_binds is not None:
        illegal = [b for b in binds if b not in allow_binds]
        if illegal:
            errors.append(f"illegal_binds:{','.join(illegal)}")

    return {"ok": len(errors) == 0, "errors": errors, "binds": binds}


__all__ = ["extract_sql_from_fenced", "find_named_binds", "validate_sql"]
