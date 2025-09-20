import re

_WHITELIST_BINDS = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}


def analyze_sql(sql: str) -> dict:
    errors: list[str] = []
    binds: list[str] = []

    if not sql or not sql.strip():
        return {"ok": False, "errors": ["empty_sql"], "binds": []}

    trimmed = sql.strip()
    if not re.match(r"^(SELECT|WITH)\b", trimmed, re.IGNORECASE):
        errors.append("must_start_select_or_with")

    lowered = trimmed.lower()
    if "```" in trimmed or "allowed columns" in lowered or "question:" in lowered:
        errors.append("instruction_leak")

    for match in re.finditer(r":([A-Za-z_][A-Za-z0-9_]*)", trimmed):
        name = match.group(1)
        binds.append(name)
        if name not in _WHITELIST_BINDS:
            errors.append(f"illegal_bind:{name}")

    if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER)\b", trimmed, re.IGNORECASE):
        errors.append("no_dml_ddl")

    return {"ok": not errors, "errors": errors, "binds": binds}


__all__ = ["analyze_sql"]

