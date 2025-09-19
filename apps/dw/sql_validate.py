import re

_ALLOWED_BINDS = {
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
}


def ensure_select_only(sql: str) -> str:
    s = (sql or "").strip()
    if not re.match(r"^(SELECT|WITH)\b", s, flags=re.IGNORECASE):
        raise ValueError("not_select")
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|ALTER|DROP|CREATE)\b",
        s,
        flags=re.IGNORECASE,
    ):
        raise ValueError("not_select")
    return s


def ensure_allowed_binds(sql: str) -> set[str]:
    binds = set(re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql or ""))
    bad = [b for b in binds if b not in _ALLOWED_BINDS]
    if bad:
        raise ValueError(f"bad_binds:{','.join(bad)}")
    return binds


_TIMEWORDS = re.compile(
    r"\b(last|next|between|since|before|after|today|yesterday|month|months|year|years|week|weeks|day|days)\b",
    re.IGNORECASE,
)


def forbid_implicit_date_window(sql: str, question: str) -> str:
    """If query uses :date_* but the question didn't ask for time, strip the window."""

    if ":date_start" in (sql or "") or ":date_end" in (sql or ""):
        if not _TIMEWORDS.search(question or ""):
            sql = re.sub(
                r"\s+(AND|WHERE)\s+[^)]*\b(REQUEST_DATE|START_DATE|END_DATE)\s*(?:BETWEEN|>=|>|<=|<).+?:date_end",
                " ",
                sql,
                flags=re.IGNORECASE | re.DOTALL,
            )
            sql = re.sub(r"\bWHERE\s+AND\b", " WHERE ", sql, flags=re.IGNORECASE)
            sql = re.sub(r"\bWHERE\s*$", "", sql, flags=re.IGNORECASE)
    return sql.strip()


__all__ = [
    "ensure_select_only",
    "ensure_allowed_binds",
    "forbid_implicit_date_window",
]
