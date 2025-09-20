import re

SQL_ALLOWED_START = re.compile(r'^\s*(select|with)\b', re.IGNORECASE | re.DOTALL)
BIND_RE = re.compile(r':([A-Za-z_][A-Za-z0-9_]*)')
DML_DDL_RE = re.compile(r'\b(insert|update|delete|merge|create|alter|drop|truncate|grant|revoke)\b', re.IGNORECASE)


def analyze_binds(sql: str):
    """Return the sorted list of bind names present in the SQL string."""

    if not sql:
        return []
    return sorted({m.group(1) for m in BIND_RE.finditer(sql)})


def validate_sql(sql: str, allow_tables=None, allow_binds=None):
    """Basic validation ensuring we only run SELECT/CTE statements and approved binds."""

    errors = []
    cleaned = (sql or '').strip()
    if not cleaned:
        return {"ok": False, "errors": ["empty_sql"], "binds": []}
    if not SQL_ALLOWED_START.search(cleaned):
        errors.append("not_select")
    if DML_DDL_RE.search(cleaned):
        errors.append("dml_or_ddl")
    used_binds = analyze_binds(cleaned)
    if allow_binds is not None:
        unknown = [b for b in used_binds if b not in allow_binds]
        if unknown:
            errors.append(f"illegal_bind:{','.join(unknown)}")
    return {"ok": len(errors) == 0, "errors": errors, "binds": used_binds}
