"""
Small helpers for safe-ish SQL text tweaks without a full parser.
Keep generic; FA specifics stay in apps/fa.
"""
from __future__ import annotations

import json
import re
from typing import Optional

import sqlglot
from sqlglot import exp

_FENCE = re.compile(r"```(?:sql)?\s*(.*?)```", re.S | re.I)
_CODE_FENCE = _FENCE
_SQL_START = re.compile(r"(?is)\b(select|with)\b")
_SQL_START_STRICT = _SQL_START
_NONSQL_LINES = re.compile(r"(?:^|\n)\s*(?:Fix and return only.*|Return only .* SQL.*)$", re.I | re.M)

_CUT_AFTER = (
    "Fix and return only",
    "Return Oracle SQL",
    "<<JSON>>",
    "</JSON>",
    "<</JSON>>",
    "No prose",
    "Explanation:",
)

_WHERE_RE = re.compile(r"(?is)\bwhere\b")

# Fenced code block: ```sql ... ```
_SQL_FENCE = _CODE_FENCE
# First SQL-ish token (legacy helper keeps broader match)
_SQL_START_LOOSE = re.compile(r"(?is)\b(SELECT|WITH|EXPLAIN|SHOW)\b")

_RE_FENCE = re.compile(r"```(?:sql)?\s*(?P<body>.*?)```", re.I | re.S)
_RE_HEAD = re.compile(r"(?mi)^\s*(?:WITH\b|SELECT\b(?!\s*\())")


def extract_sql_one_stmt(text: str, dialect: str = "generic") -> str:
    """
    Extract exactly one SQL statement suitable for execution.
    - Prefer first fenced ```sql``` block if present.
    - Strip instructional lines / non-SQL chatter.
    - Keep only the first statement; drop anything after the first ; (outside quotes).
    - Enforce read-only: must start with SELECT or WITH.
    - For Oracle/SQLAlchemy: strip trailing semicolon.
    """
    if not text:
        return ""

    # Prefer fenced code
    m = _CODE_FENCE.search(text)
    if m:
        text = m.group(1)

    # Remove obvious non-SQL instruction lines
    text = _NONSQL_LINES.sub("", text).strip()

    # If the SQL starts later in the string, trim leading chatter
    m = _SQL_START_STRICT.search(text)
    if m:
        text = text[m.start():]

    # Keep only first statement; respect quoted strings
    out: list[str] = []
    in_s = False
    in_d = False
    for ch in text:
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        out.append(ch)
        if ch == ';' and not in_s and not in_d:
            break
    sql = "".join(out).strip()

    # Enforce read-only
    if not _SQL_START_STRICT.match(sql or ""):
        return ""

    # Oracle through SQLAlchemy: avoid trailing semicolon
    if dialect.lower().startswith("oracle"):
        if sql.endswith(";"):
            sql = sql[:-1].rstrip()

    # Kill accidental stray bracket lines, markdown, etc.
    sql = sql.replace("\r", "").strip()
    # Guard against leftover instructional keywords
    if "Fix and return only" in sql or "ONLY SQL" in sql:
        return ""

    return sql


def extract_sql(text: str) -> str | None:
    """Pull a single SQL statement out of model output (markdown fences, chatter, etc.)."""
    if not text:
        return None
    body = text.strip()
    m = _SQL_FENCE.search(body)
    if m:
        body = m.group(1).strip()
    # drop leading 'sql:' labels etc.
    body = re.sub(r"^\s*sql\s*:\s*", "", body, flags=re.I).strip()
    m2 = _SQL_START_LOOSE.search(body)
    if not m2:
        return None
    sql = body[m2.start():]
    # stop at a trailing fence if present
    sql = sql.split("```", 1)[0].strip()
    # keep up to the last semicolon if multiple statements
    if ";" in sql:
        sql = sql[: sql.rfind(";") + 1]
    # strip stray backticks
    sql = sql.replace("`", "").strip()
    return sql or None


def extract_sql_block(text: str) -> str:
    """Return SQL extracted from fenced block or first SELECT/WITH chunk."""
    if not text:
        return ""

    match = _FENCE.search(text)
    if match:
        return match.group(1).strip()

    match2 = _SQL_START.search(text or "")
    if not match2:
        return ""

    tail = text[match2.start() :]
    out: list[str] = []
    for line in tail.splitlines():
        if any(marker.lower() in line.lower() for marker in _CUT_AFTER):
            break
        out.append(line)
    sql = "\n".join(out).strip()
    sql = sql.lstrip('`"\'').rstrip('`"\'[]').strip()
    return sql


def _first_sql_statement(text: str) -> str:
    if not text:
        return ""

    match = _RE_FENCE.search(text)
    if match:
        candidate = match.group("body").strip()
    else:
        head = _RE_HEAD.search(text)
        if not head:
            return ""
        candidate = text[head.start() :].strip()

    candidate = candidate.split("```", 1)[0]
    candidate = re.split(r"\n\s*(?:Explanation:|Errors?:)", candidate, maxsplit=1)[0].strip()

    bad_prefixes = (
        "Return Oracle SQL only inside",
        "Return only one Oracle SELECT",
        "Write only Oracle SQL",
        "No code fences",
        "No comments",
        "No explanations",
        "Statement:",
        "Fix and return only Oracle SQL",
        "SELECT (or CTE)",
    )
    lowered = candidate.lower()
    for prefix in bad_prefixes:
        if lowered.startswith(prefix.lower()):
            return ""

    if ";" in candidate:
        before, _ = candidate.split(";", 1)
        candidate = (before + ";").strip()

    if candidate.endswith(";"):
        candidate = candidate[:-1].rstrip()

    return candidate


def looks_like_oracle_sql(text: str) -> bool:
    return bool(re.match(r"(?is)^\s*(WITH|SELECT)\b(?!\s*\()", text or ""))


def sanitize_oracle_sql(*candidates: str) -> str:
    """Return the first plausible Oracle SELECT/WITH statement from the candidates."""

    for raw in candidates:
        if not raw:
            continue
        sql = _first_sql_statement(raw)
        if not sql:
            continue
        if looks_like_oracle_sql(sql):
            return sql.strip()
    return ""


def looks_like_instruction(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"(?i)\b(return only|fix and return|no prose|no explanation)\b", text))


def validate_oracle_sql(sql: str) -> None:
    """Raise ValueError if SQL isn’t valid Oracle SELECT/WITH with a FROM-clause."""

    if not sql or not _SQL_START.match(sql):
        raise ValueError("No SELECT/WITH detected")

    try:
        tree = sqlglot.parse_one(sql, read="oracle")
    except Exception as exc:  # pragma: no cover - sqlglot raises many subclasses
        raise ValueError(f"SQL parse failed (oracle): {exc}") from exc

    sel = tree.find(exp.Select)
    if not sel:
        raise ValueError("No SELECT found in statement")
    if not sel.args.get("from"):
        raise ValueError("SELECT has no FROM clause")


JSON_BOUNDS = re.compile(r"<<JSON>>\s*(\{.*?\})\s*<</JSON>>", re.S)


def extract_json_bracket(raw: str) -> Optional[dict]:
    """Extract JSON payload enclosed in <<JSON>> ... <</JSON>> markers."""

    if not raw:
        return None
    match = JSON_BOUNDS.search(raw)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def looks_like_sql(text: str) -> bool:
    return bool(_SQL_START_LOOSE.search((text or "").strip()))

def _strip_semicolon(sql: str) -> str:
    return sql.rstrip().rstrip(";").rstrip()

def inject_between_date_filter(sql: str, fully_qualified_col: str, start_iso: str, end_iso: str) -> str:
    """
    Add a BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' condition on the given column.
    If a WHERE exists -> append AND (...). Otherwise add WHERE (...).
    Leaves everything else intact. Returns modified SQL string.
    """
    base = _strip_semicolon(sql)
    cond = f"{fully_qualified_col} BETWEEN '{start_iso}' AND '{end_iso}'"
    if _WHERE_RE.search(base):
        return f"{base} AND {cond}"
    return f"{base} WHERE {cond}"


def ensure_limit_100(sql: str) -> str:
    """Ensure the SQL statement has a LIMIT 100 if none present."""
    base = _strip_semicolon(sql)
    if re.search(r"\blimit\b", base, re.I):
        return base
    return f"{base} LIMIT 100"


def explain_sql(engine, sql: str):
    """Run EXPLAIN on the given SQL and return the plan rows."""
    from sqlalchemy import text as _text

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "generic"))
    cleaned = extract_sql_one_stmt(sql, dialect=dialect)
    if not cleaned:
        raise ValueError("empty_or_invalid_sql_after_sanitize")
    with engine.connect() as c:
        rs = c.execute(_text(f"EXPLAIN {cleaned}"))
        return [tuple(r) for r in rs.fetchall()]


def execute_sql(engine, sql: str):
    """Execute the SQL and return a list of rows (dicts)."""
    from sqlalchemy import text as _text

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "generic"))
    base = extract_sql_one_stmt(sql, dialect=dialect)
    if dialect.lower().startswith("oracle"):
        cleaned = sanitize_oracle_sql(sql, base)
        if not cleaned:
            raise ValueError("empty_or_invalid_sql_after_sanitize")
        validate_oracle_sql(cleaned)
    else:
        cleaned = base
        if not cleaned:
            raise ValueError("empty_or_invalid_sql_after_sanitize")
    with engine.connect() as c:
        rs = c.execute(_text(cleaned))
        cols = list(rs.keys())
        return [dict(zip(cols, r)) for r in rs.fetchall()]
