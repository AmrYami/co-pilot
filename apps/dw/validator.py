import re

_BIND_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def validate_sql(sql: str):
    if not sql or not sql.strip():
        return {"ok": False, "errors": ["empty_sql"], "binds": [], "bind_names": []}
    stripped = sql.strip()
    head = stripped[:32].upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        return {"ok": False, "errors": ["not_select"], "binds": [], "bind_names": []}
    forbidden = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
    ]
    upper = stripped.upper()
    for keyword in forbidden:
        if f" {keyword} " in upper or upper.startswith(f"{keyword} "):
            return {
                "ok": False,
                "errors": [f"forbidden_{keyword.lower()}"],
                "binds": [],
                "bind_names": [],
            }
    binds = list(dict.fromkeys(_BIND_RE.findall(stripped)))
    return {"ok": True, "errors": [], "binds": binds, "bind_names": binds}


__all__ = ["validate_sql"]
