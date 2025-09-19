"""LLM helpers for DocuWare Oracle analytics."""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from core.model_loader import get_model

JSON_TAG_OPEN = "<<JSON>>"
JSON_TAG_CLOSE = "<<END>>"

_INTENT_SCHEMA_HINT = """
Return ONLY a compact JSON between <<JSON>> and <<END>> with this shape:
{
  "tables": ["Contract"],
  "select": ["CONTRACT_ID", "END_DATE", "CONTRACT_STATUS"],
  "filters": [
    {"column": "END_DATE", "op": "between", "value": ["{TODAY}", "{TODAY}+30d"]},
    {"column": "VAT", "op": "is_null"},
    {"column": "CONTRACT_VALUE_NET_OF_VAT", "op": ">", "value": 0}
  ],
  "group_by": [],
  "order_by": [{"expr": "END_DATE", "dir": "asc"}],
  "limit": 100
}
Date literals allowed in filter values: {TODAY}, {NOW}, {TODAY-7d}, {TODAY+30d}, {START_OF_MONTH}, {END_OF_MONTH}.
Only include fields you actually need for the question.
"""


def _extract_json(text: str) -> Optional[str]:
    match = re.search(
        re.escape(JSON_TAG_OPEN) + r"(.*)" + re.escape(JSON_TAG_CLOSE), text, re.S
    )
    if match:
        return match.group(1).strip()
    last_brace = text.rfind("}")
    first_brace = text.find("{")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        return text[first_brace : last_brace + 1]
    return None


def clarify_intent(user_q: str, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ask the clarifier model for structured intent JSON."""

    clarifier = get_model("clarifier")
    if clarifier is None:
        return {"ok": False, "error": "clarifier_unavailable"}

    sys_prompt = (
        "You are a careful intent normalizer for DocuWare (Oracle). "
        "Output STRICT JSON only — no prose, no comments, no markdown.\n"
        + _INTENT_SCHEMA_HINT
    )
    prompt = (
        f"{sys_prompt}\nUser question:\n{user_q}\n"
        f"Return JSON only between {JSON_TAG_OPEN} and {JSON_TAG_CLOSE}."
    )
    out = clarifier.generate(prompt, max_new_tokens=256, temperature=0.0, stop=[JSON_TAG_CLOSE])
    js = _extract_json(out or "")
    if not js:
        retry_prompt = (
            "RETURN JSON ONLY. NO PROSE. NO MARKDOWN.\n"
            + _INTENT_SCHEMA_HINT
            + f"\nUser question:\n{user_q}\n{JSON_TAG_OPEN}"
        )
        out2 = clarifier.generate(
            retry_prompt, max_new_tokens=256, temperature=0.0, stop=[JSON_TAG_CLOSE]
        )
        js = _extract_json(out2 or "")
        if not js:
            return {"ok": False, "error": "clarifier_no_json", "raw": out2}
    try:
        intent = json.loads(js)
    except Exception as exc:  # pragma: no cover - defensive
        return {"ok": False, "error": f"clarifier_bad_json: {exc}", "raw_json": js}

    intent["ok"] = True
    return intent


def _resolve_date_literal(token: str) -> dt.datetime:
    today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if token.startswith("{TODAY"):
        base = today
        match = re.match(r"\{TODAY([+-]\d+)d\}", token)
        if match:
            return base + dt.timedelta(days=int(match.group(1)))
        return base
    if token.startswith("{NOW"):
        base = dt.datetime.now()
        match = re.match(r"\{NOW([+-]\d+)d\}", token)
        if match:
            return base + dt.timedelta(days=int(match.group(1)))
        return base
    if token == "{START_OF_MONTH}":
        return today.replace(day=1)
    if token == "{END_OF_MONTH}":
        next_month = (today.replace(day=28) + dt.timedelta(days=4)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return next_month - dt.timedelta(days=next_month.day)
    raise ValueError(f"unknown date literal {token}")


def _compile_filters(filters: List[Dict[str, Any]] | None) -> Tuple[List[str], Dict[str, Any]]:
    where: List[str] = []
    binds: Dict[str, Any] = {}
    bind_i = 1

    for item in filters or []:
        column = item.get("column")
        op = (item.get("op") or "=").lower()
        value = item.get("value")
        if not column:
            continue
        if op in {"is_null", "is null"}:
            where.append(f"{column} IS NULL")
            continue
        if op in {"is_not_null", "is not null"}:
            where.append(f"{column} IS NOT NULL")
            continue
        if op in {"=", "eq", "==", "!=", "<>", ">", ">=", "<", "<=", "like"}:
            bind_name = f"b{bind_i}"
            bind_i += 1
            if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
                bind_value = _resolve_date_literal(value)
            else:
                bind_value = value
            binds[bind_name] = bind_value
            actual_op = op if op not in {"eq", "=="} else "="
            where.append(f"{column} {actual_op} :{bind_name}")
            continue
        if op == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
            bind_name_1 = f"b{bind_i}"
            bind_name_2 = f"b{bind_i + 1}"
            bind_i += 2
            start, end = value
            if isinstance(start, str) and start.startswith("{"):
                start = _resolve_date_literal(start)
            if isinstance(end, str) and end.startswith("{"):
                end = _resolve_date_literal(end)
            binds[bind_name_1] = start
            binds[bind_name_2] = end
            where.append(f"{column} BETWEEN :{bind_name_1} AND :{bind_name_2}")
            continue
        if op == "in" and isinstance(value, (list, tuple)) and value:
            names: List[str] = []
            for entry in value:
                bind_name = f"b{bind_i}"
                bind_i += 1
                binds[bind_name] = entry
                names.append(f":{bind_name}")
            where.append(f"{column} IN ({', '.join(names)})")
            continue

    return where, binds


def intent_to_sql(intent: Dict[str, Any]) -> Dict[str, Any]:
    """Compile structured intent JSON into Oracle SQL."""

    if not intent.get("ok"):
        return {"ok": False, "error": intent.get("error", "intent_missing")}

    tables = intent.get("tables") or ["Contract"]
    select = intent.get("select") or ["CONTRACT_ID"]
    filters = intent.get("filters") or []
    group_by = intent.get("group_by") or []
    order_by = intent.get("order_by") or []
    limit = intent.get("limit")

    if len(tables) != 1 or tables[0] != "Contract":
        return {"ok": False, "error": "only_Contract_supported_now"}

    where_sql, binds = _compile_filters(filters)
    columns = ", ".join(select)
    sql = 'SELECT ' + columns + ' FROM "Contract"'

    if where_sql:
        sql += "\nWHERE " + "\n  AND ".join(where_sql)
    if group_by:
        sql += "\nGROUP BY " + ", ".join(group_by)
    if order_by:
        clauses: List[str] = []
        for ob in order_by:
            expr = ob.get("expr")
            if not expr:
                continue
            direction = (ob.get("dir") or "asc").upper()
            clauses.append(f"{expr} {direction}")
        if clauses:
            sql += "\nORDER BY " + ", ".join(clauses)
    if isinstance(limit, int) and limit > 0:
        sql += f"\nFETCH FIRST {limit} ROWS ONLY"

    return {"ok": True, "sql": sql, "binds": binds}


def nl_to_sql_with_llm(user_q: str, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ask SQLCoder for direct SQL when structured intent is unavailable."""

    mdl = get_model("sql")
    if mdl is None:
        return {"ok": False, "error": "sql_model_unavailable"}

    sys = (
        "Return ONLY Oracle SQL. No prose. No comments. SELECT/CTE only.\n"
        'Use table "Contract". Use Oracle functions: NVL, LISTAGG WITHIN GROUP, TRIM, UPPER.\n'
        "If filtering by date is requested (e.g., 'next 30 days'), filter on the explicit column mentioned (END_DATE) with binds.\n"
        "Use named binds like :b1, :b2, ... never positional.\n"
    )
    prompt = f"{sys}\nUser question:\n{user_q}\nSQL:"
    sql = mdl.generate(prompt, max_new_tokens=256, temperature=0.0)
    sql = re.sub(r"^```sql|^```|```$", "", sql.strip(), flags=re.I | re.M)
    return {"ok": True, "sql": sql.strip()}
