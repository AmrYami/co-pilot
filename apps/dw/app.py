from __future__ import annotations

import csv
import datetime
import json
import os
import pathlib
import re
from calendar import monthrange
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request
from sqlalchemy import text

from apps.dw.intent import DWIntent, extract_intent
from apps.dw.sql_compose import compose_sql
from core.datasources import DatasourceRegistry
from core.settings import Settings
from core.model_loader import get_model
from core.sql_exec import get_mem_engine
from core.sql_utils import (
    extract_bind_names,
    looks_like_instruction,
    sanitize_oracle_sql,
    validate_oracle_sql,
)
from core.logging_utils import get_logger, log_event
import sqlglot


parse_one = getattr(sqlglot, "parse_one", lambda sql, read=None: None)
exp = getattr(sqlglot, "expressions", None) or getattr(sqlglot, "exp", None)

JSON_BLOCK_RE = re.compile(r"<<JSON>>\s*(\{.*?\})\s*<</JSON>>", re.S | re.I)
TOP_N_RE = re.compile(r"\btop\s+(\d{1,4})\b", re.I)
NEXT_DAYS_RE = re.compile(r"\b(next|within|in)\s+(\d{1,4})\s+days?\b", re.I)
LAST_MONTH_RE = re.compile(r"\blast\s+month\b", re.I)
COUNT_RE = re.compile(r"\b(count|how many|عدد)\b", re.I)
VALUE_RE = re.compile(r"\b(value|net\s*of\s*vat|amount|القيمة)\b", re.I)


DW_DIM_MAP = {
    "owner department": "OWNER_DEPARTMENT",
    "department": "OWNER_DEPARTMENT",
    "entity": "ENTITY_NO",
    "owner": "CONTRACT_OWNER",
    "stakeholder": "CONTRACT_STAKEHOLDER_1",
}

_TOPN_RE = re.compile(r"\btop\s+(\d{1,4})\b", re.I)
_BY_PER_RE = re.compile(
    r"\b(?:by|per)\s+([a-zA-Z_ ]+?)(?=(?:\s+(?:last|next|this)\b|[.,]|$))", re.I
)


def _extract_group_by(q: str) -> Optional[str]:
    match = _BY_PER_RE.search(q or "")
    if not match:
        return None
    phrase = re.sub(r"\s+", " ", match.group(1).strip().lower())
    for alias, col in DW_DIM_MAP.items():
        if alias in phrase:
            return col
    return None


def _detect_measure(q: str) -> str:
    lowered = (q or "").lower()
    if "gross" in lowered:
        return "gross"
    if "net" in lowered:
        return "net"
    if "count" in lowered or "(count)" in lowered:
        return "count"
    return "value"


def _extract_topn(q: str) -> Optional[int]:
    match = _TOPN_RE.search(q or "")
    if not match:
        return None
    try:
        value = int(match.group(1))
    except Exception:
        return None
    return value if value > 0 else None


def _metric_expr(measure: str) -> Tuple[str, str]:
    if measure == "gross":
        return (
            "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
            "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END",
            "TOTAL_GROSS_VALUE",
        )
    return ("NVL(CONTRACT_VALUE_NET_OF_VAT,0)", "TOTAL_CONTRACT_VALUE")


def _build_count_sql(
    table: str,
    date_col: str,
    start: str,
    end: str,
    group_col: Optional[str],
    topn: Optional[int],
) -> str:
    select_parts: list[str] = []
    group_parts: list[str] = []
    if group_col:
        select_parts.append(f"{group_col} AS GROUP_KEY")
        group_parts.append(group_col)
    select_parts.append("COUNT(*) AS CNT")
    sql_lines = [
        "SELECT",
        "  " + ",\n  ".join(select_parts),
        f'FROM "{table}"' if not table.startswith('"') else f"FROM {table}",
        f"WHERE {date_col} BETWEEN :date_start AND :date_end",
    ]
    if group_parts:
        sql_lines.append("GROUP BY " + ", ".join(group_parts))
        sql_lines.append("ORDER BY CNT DESC")
        if topn:
            sql_lines.append("FETCH FIRST :top_n ROWS ONLY")
    return "\n".join(sql_lines)


def _build_agg_sql(
    table: str,
    date_col: str,
    start: str,
    end: str,
    group_col: str,
    measure: str,
    topn: Optional[int],
) -> str:
    expr, alias = _metric_expr(measure)
    table_literal = f'"{table}"' if not table.startswith('"') else table
    group_expr = group_col
    if group_col.upper() == "OWNER_DEPARTMENT":
        group_expr = "NVL(OWNER_DEPARTMENT, '(Unknown)')"
    lines = [
        "SELECT",
        f"  {group_expr} AS GROUP_KEY,",
        f"  SUM({expr}) AS {alias}",
        f"FROM {table_literal}",
        f"WHERE {date_col} BETWEEN :date_start AND :date_end",
        f"GROUP BY {group_expr}",
        f"ORDER BY {alias} DESC",
    ]
    if topn:
        lines.append("FETCH FIRST :top_n ROWS ONLY")
    return "\n".join(lines)


def _should_select_all_columns(q: str, group_col: Optional[str], measure: str) -> bool:
    if group_col or measure == "count":
        return False
    return True


# --- DW Intent helpers -------------------------------------------------------

_DIMENSION_MAP = {
    # user phrase -> column
    r"\bstakeholder(s)?\b": "CONTRACT_STAKEHOLDER_1",
    r"\bowner\s+department\b": "OWNER_DEPARTMENT",
    r"\bdepartment\b": "OWNER_DEPARTMENT",
    r"\bentity\b": "ENTITY_NO",
    r"\bowner\b": "CONTRACT_OWNER",
}

_VALUE_COL_NET = "NVL(CONTRACT_VALUE_NET_OF_VAT, 0)"  # "contract value" / "net value"
_VALUE_COL_GROSS = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT, 0) + CASE WHEN NVL(VAT, 0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT, 0) * NVL(VAT, 0) ELSE NVL(VAT, 0) END"
)

_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.I)
_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+month", re.I)
_LAST_3_MONTHS = re.compile(r"\blast\s+3\s+months?\b", re.I)


def _detect_top_n(q: str) -> Optional[int]:
    m = _TOP_RE.search(q or "")
    return int(m.group(1)) if m else None


def _detect_dimension(q: str) -> Optional[str]:
    t = (q or "").lower()
    for pat, col in _DIMENSION_MAP.items():
        if re.search(pat, t):
            return col
    return None


def _wants_count(q: str) -> bool:
    text = (q or "").lower()
    return " count" in (" " + text) or "(count" in text or text.strip().endswith("(count)")


def _mentions_gross(q: str) -> bool:
    return "gross value" in (q or "").lower()


def _mentions_contract_value(q: str) -> bool:
    t = (q or "").lower()
    return "contract value" in t or "value of contracts" in t


def _default_window_for(q: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Returns (date_col, start_iso, end_iso). If last 3 months is mentioned, compute start/end.
    Otherwise None start/end (caller may inject).
    Default date col: REQUEST_DATE.
    """

    date_col = "REQUEST_DATE"
    ql = (q or "").lower()

    m = _LAST_N_MONTHS.search(ql)
    if m:
        n = max(1, min(24, int(m.group(1))))
        today = datetime.date.today()
        start_month = today
        for _ in range(n):
            year = start_month.year
            month = start_month.month - 1
            if month == 0:
                month = 12
                year -= 1
            days_in_month = [
                31,
                29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                31,
                30,
                31,
                30,
                31,
                31,
                30,
                31,
                30,
                31,
            ][month - 1]
            day = min(start_month.day, days_in_month)
            start_month = datetime.date(year, month, day)
        start_iso = start_month.isoformat()
        end_iso = today.isoformat()
        return date_col, start_iso, end_iso

    if _LAST_3_MONTHS.search(ql):
        today = datetime.date.today()
        start_month = today
        for _ in range(3):
            year = start_month.year
            month = start_month.month - 1
            if month == 0:
                month = 12
                year -= 1
            days_in_month = [
                31,
                29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                31,
                30,
                31,
                30,
                31,
                31,
                30,
                31,
                30,
                31,
            ][month - 1]
            day = min(start_month.day, days_in_month)
            start_month = datetime.date(year, month, day)
        return date_col, start_month.isoformat(), today.isoformat()

    return date_col, None, None


def _sanitize_sql(text: str) -> Optional[str]:
    """
    Keep only the first SELECT/WITH statement from the model output.
    Remove backticks, fences, instructions, and anything before SELECT/WITH.
    """

    if not text:
        return None
    cleaned = text.strip().replace("```sql", "```").replace("```SQL", "```")
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("` \n")
    match = re.search(r"(?is)\b(SELECT|WITH)\b", cleaned)
    if not match:
        return None
    cleaned = cleaned[match.start() :].strip()
    return cleaned or None


def _fallback_dw_sql(
    question: str,
    date_col: str,
    start_iso: Optional[str],
    end_iso: Optional[str],
    top_n: Optional[int],
    wants_count: bool,
    group_dim: Optional[str],
    *,
    table_name: str = '"Contract"',
) -> Tuple[str, Dict[str, Any]]:
    """
    Deterministic query generator when the model output is invalid.
    - If group_dim or wants_count: aggregate projection
    - Else: SELECT * (show all columns)
    """

    binds: Dict[str, Any] = {}
    where_parts: List[str] = []

    normalized_date_col = (date_col or "REQUEST_DATE").strip().upper()
    if normalized_date_col not in {"REQUEST_DATE", "END_DATE", "START_DATE"}:
        normalized_date_col = "REQUEST_DATE"

    table_literal = table_name.strip()
    if not table_literal.startswith('"'):
        table_literal = f'"{table_literal.strip("\"")}"'

    if start_iso and end_iso:
        where_parts.append(f"{normalized_date_col} BETWEEN :date_start AND :date_end")
        binds["date_start"] = start_iso
        binds["date_end"] = end_iso

    where_clause = f"\nWHERE {' AND '.join(where_parts)}" if where_parts else ""

    value_expr = _VALUE_COL_GROSS if _mentions_gross(question) else _VALUE_COL_NET

    top_n_is_explicit = False
    if isinstance(top_n, int) and top_n > 0 and re.search(r"\btop\b", question or "", re.I):
        top_n_is_explicit = True
        binds["top_n"] = int(top_n)

    if wants_count or group_dim:
        if wants_count and not group_dim:
            sql = f"""SELECT COUNT(*) AS CNT
FROM {table_literal}{where_clause}"""
            return sql, binds

        dimension = group_dim or ""
        if dimension.upper() == "OWNER_DEPARTMENT":
            dimension = "NVL(OWNER_DEPARTMENT, '(Unknown)')"
        agg_alias = "TOTAL_GROSS_VALUE" if _mentions_gross(question) else "TOTAL_CONTRACT_VALUE"
        sql = (
            "SELECT\n"
            f"  {dimension} AS DIMENSION,\n"
            f"  SUM({value_expr}) AS {agg_alias}\n"
            f"FROM {table_literal}{where_clause}\n"
            f"GROUP BY {dimension}\n"
            f"ORDER BY {agg_alias} DESC"
        )
        if top_n_is_explicit:
            sql += "\nFETCH FIRST :top_n ROWS ONLY"
        return sql, binds

    sql = f"""SELECT *
FROM {table_literal}{where_clause}"""
    if top_n_is_explicit:
        sql += "\nFETCH FIRST :top_n ROWS ONLY"
    if start_iso and end_iso:
        sql += f"\nORDER BY {normalized_date_col} ASC"
    return sql, binds


def _find_json_objects(text: str) -> list[str]:
    """Return every balanced {...} substring in order."""

    objs: list[str] = []
    stack: list[str] = []
    start = -1
    for idx, ch in enumerate(text or ""):
        if ch == "{":
            if not stack:
                start = idx
            stack.append("{")
        elif ch == "}":
            if stack:
                stack.pop()
                if not stack and start >= 0:
                    objs.append((text or "")[start : idx + 1])
                    start = -1
    return objs


def _parse_clarifier_output(raw: str) -> dict:
    """Robustly parse clarifier JSON even if the model ignored the tags."""

    if not raw:
        return {}

    match = JSON_BLOCK_RE.search(raw)
    if match:
        try:
            obj = json.loads(match.group(1))
            if obj:
                return obj
        except Exception:
            pass

    for candidate in reversed(_find_json_objects(raw)):
        try:
            data = json.loads(candidate)
            if isinstance(data, dict) and data:
                return data
        except Exception:
            continue
    return {}


def _parse_top_n(text: str) -> int | None:
    match = TOP_N_RE.search(text or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _month_bounds_for_last_month(today: date) -> tuple[date, date]:
    first_this = today.replace(day=1)
    last_month_last_day = first_this - timedelta(days=1)
    last_month_first_day = last_month_last_day.replace(day=1)
    return last_month_first_day, last_month_last_day


def _iso(d: date) -> str:
    return d.isoformat()


def _normalize_intent(question: str, parsed: dict) -> dict:
    text = (question or "").strip()
    intent = {
        "has_time_window": parsed.get("has_time_window"),
        "date_column": parsed.get("date_column"),
        "explicit_dates": parsed.get("explicit_dates"),
        "top_n": parsed.get("top_n"),
        "agg": None,
        "wants_all_columns": True,
        "sort_by": None,
        "sort_desc": False,
    }

    question_has_top = bool(TOP_N_RE.search(text))
    intent["user_requested_top_n"] = question_has_top

    if COUNT_RE.search(text):
        intent["agg"] = "count"
        intent["wants_all_columns"] = False

    if intent.get("top_n") is None:
        match = TOP_N_RE.search(text)
        if match:
            try:
                intent["top_n"] = int(match.group(1))
            except Exception:
                pass
    if intent.get("top_n"):
        if VALUE_RE.search(text):
            intent["sort_by"] = "CONTRACT_VALUE_NET_OF_VAT"
            intent["sort_desc"] = True
        intent["wants_all_columns"] = True

    today = date.today()

    if not intent.get("explicit_dates"):
        match = NEXT_DAYS_RE.search(text)
        if match:
            try:
                n_days = int(match.group(2))
                start = today
                end = today + timedelta(days=n_days)
                intent["explicit_dates"] = {"start": _iso(start), "end": _iso(end)}
                intent["has_time_window"] = True
            except Exception:
                pass

    if not intent.get("explicit_dates") and LAST_MONTH_RE.search(text):
        start, end = _month_bounds_for_last_month(today)
        intent["explicit_dates"] = {"start": _iso(start), "end": _iso(end)}
        intent["has_time_window"] = True

    if not intent.get("date_column"):
        lowered = text.lower()
        if "end_date" in lowered or "expir" in lowered:
            intent["date_column"] = "END_DATE"
        elif "start_date" in lowered:
            intent["date_column"] = "START_DATE"
        else:
            intent["date_column"] = "REQUEST_DATE"

    if intent.get("date_column"):
        intent["date_column"] = str(intent["date_column"]).upper()

    return intent


def _parse_date_yyyy_mm_dd(s: str) -> datetime.datetime:
    return datetime.datetime.strptime(s, "%Y-%m-%d")


def _fmt_date_yyyy_mm_dd(dt_obj: datetime.datetime) -> str:
    return dt_obj.strftime("%Y-%m-%d")


def _maybe_autowiden_and_rerun(
    sql_text: str,
    binds: Dict[str, Any],
    rows: list[tuple],
    intent: Dict[str, Any],
    settings,
    exec_fn,
) -> Tuple[list[tuple], Dict[str, Any], Optional[dict]]:
    """Retry the DW query with a wider window if appropriate."""

    try:
        enabled = bool(settings.get("DW_AUTOWIDEN_ENABLED", True))
        if not enabled:
            return rows, binds, None

        if not intent or not intent.get("has_time_window"):
            return rows, binds, None
        if "date_start" not in binds or "date_end" not in binds:
            return rows, binds, None
        if rows and len(rows) > 0:
            return rows, binds, None

        threshold_days = int(settings.get("DW_AUTOWIDEN_THRESHOLD_DAYS", 45) or 45)
        widen_to_days = int(settings.get("DW_AUTOWIDEN_TO_DAYS", 90) or 90)

        ds = binds["date_start"]
        de = binds["date_end"]
        if not isinstance(ds, str) or not isinstance(de, str):
            return rows, binds, None

        d0 = _parse_date_yyyy_mm_dd(ds)
        d1 = _parse_date_yyyy_mm_dd(de)
        window_days = (d1 - d0).days

        if window_days > threshold_days:
            return rows, binds, None

        today = datetime.datetime.utcnow().date()
        new_start = today
        new_end = today + timedelta(days=widen_to_days)

        new_binds = dict(binds)
        new_binds["date_start"] = _fmt_date_yyyy_mm_dd(new_start)
        new_binds["date_end"] = _fmt_date_yyyy_mm_dd(new_end)

        rows2, cols2 = exec_fn(sql_text, new_binds)

        meta = {
            "autowiden_applied": True,
            "autowiden_from_days": window_days,
            "autowiden_to_days": widen_to_days,
            "rows_after_autowiden": len(rows2),
            "new_binds": {
                "date_start": new_binds["date_start"],
                "date_end": new_binds["date_end"],
            },
            "columns": [c.lower() for c in cols2],
        }
        return rows2, new_binds, meta

    except Exception:
        return rows, binds, None
from .llm import clarify_intent, derive_bind_values, nl_to_sql_with_llm
from .validator import WHITELIST_BINDS, basic_checks


# Columns exposed by the DW endpoint (referenced in prompts and heuristics)
ALLOWED_COLUMNS = [
    "CONTRACT_ID",
    "CONTRACT_OWNER",
    "CONTRACT_STAKEHOLDER_1",
    "CONTRACT_STAKEHOLDER_2",
    "CONTRACT_STAKEHOLDER_3",
    "CONTRACT_STAKEHOLDER_4",
    "CONTRACT_STAKEHOLDER_5",
    "CONTRACT_STAKEHOLDER_6",
    "CONTRACT_STAKEHOLDER_7",
    "CONTRACT_STAKEHOLDER_8",
    "DEPARTMENT_1",
    "DEPARTMENT_2",
    "DEPARTMENT_3",
    "DEPARTMENT_4",
    "DEPARTMENT_5",
    "DEPARTMENT_6",
    "DEPARTMENT_7",
    "DEPARTMENT_8",
    "OWNER_DEPARTMENT",
    "CONTRACT_VALUE_NET_OF_VAT",
    "VAT",
    "CONTRACT_PURPOSE",
    "CONTRACT_SUBJECT",
    "START_DATE",
    "END_DATE",
    "REQUEST_DATE",
    "REQUEST_TYPE",
    "CONTRACT_STATUS",
    "ENTITY_NO",
    "REQUESTER",
]

ALLOWED_BINDS = sorted(WHITELIST_BINDS)

dw_bp = Blueprint("dw", __name__, url_prefix="/dw")
log = get_logger("main")


_STAR_SAFE_TABLE = r'"?Contract"?'
_DATE_COLS = {"end_date", "start_date", "request_date"}
_PROJECTION_COLS = {
    "contract_id",
    "contract_owner",
    "contract_stakeholder_1",
    "contract_stakeholder_2",
    "contract_stakeholder_3",
    "contract_stakeholder_4",
    "contract_stakeholder_5",
    "contract_stakeholder_6",
    "contract_stakeholder_7",
    "contract_stakeholder_8",
    "department_1",
    "department_2",
    "department_3",
    "department_4",
    "department_5",
    "department_6",
    "department_7",
    "department_8",
    "owner_department",
    "contract_value_net_of_vat",
    "vat",
    "contract_purpose",
    "contract_subject",
    "request_type",
    "contract_status",
    "entity_no",
    "requester",
}


def _mentions_specific_projection(question: str) -> bool:
    """Return True when the question asks for specific non-date columns."""

    q = (question or "").lower()
    if re.search(r"\b(all columns|everything|full details|show all|display all)\b", q):
        return False

    sanitized = q
    for date_col in _DATE_COLS:
        date_pattern = date_col.replace("_", r"[_\s]+")
        sanitized = re.sub(rf"\b{date_pattern}\b", " ", sanitized)

    for token in _PROJECTION_COLS:
        pattern = token.replace("_", r"[_\s]+")
        if re.search(rf"\b{pattern}\b", sanitized):
            return True
    return False


def _strip_code_fences(sql: str) -> str:
    if not sql:
        return sql
    sql = re.sub(r"(?is)^\s*```sql\s*", "", sql)
    sql = re.sub(r"(?is)\s*```\s*$", "", sql)
    return sql.strip()


def _is_simple_contract_select(sql: str) -> bool:
    if re.search(r"(?is)\bwith\b", sql):
        return False
    if re.search(r"(?is)\bjoin\b", sql):
        return False
    return bool(re.search(rf"(?is)^\s*select\b.+\bfrom\s+{_STAR_SAFE_TABLE}\b", sql))


def _rewrite_projection_to_star(sql: str) -> str:
    return re.sub(
        rf"(?is)^\s*select\s+(.+?)\s+(from\s+{_STAR_SAFE_TABLE}\b)",
        r"SELECT * \2",
        sql,
        count=1,
    )


def _strip_limits(sql: str) -> str:
    if not sql:
        return sql
    sql = re.sub(r"(?is)\s+fetch\s+(first|next)\s+(?::\w+|\d+)\s+rows\s+only", "", sql)
    sql = re.sub(r"(?is)\s+offset\s+\d+\s+rows", "", sql)
    sql = re.sub(r"(?is)\s+limit\s+(?::\w+|\d+)\b", "", sql)
    return sql.strip()


def _build_oracle_prompt(
    question: str,
    intent: dict,
    *,
    table: str,
    allowed_columns: list[str],
    allowed_binds: list[str],
) -> str:
    allowed_cols = ", ".join(allowed_columns)
    lines = [
        "Return Oracle SQL only inside ```sql fenced block.",
        f'Table: "{table}"',
        f"Allowed columns: {allowed_cols}",
        "Oracle syntax only (NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY). SELECT/CTE only.",
        f"Allowed binds: {', '.join(allowed_binds)}",
    ]

    if intent.get("agg") == "count":
        lines.append("Return a single COUNT query: SELECT COUNT(*) AS CNT ...")
        lines.append("Do not select other columns.")
    else:
        if intent.get("wants_all_columns", True):
            lines.append("If the question does not specify which columns to show, SELECT ALL columns (use SELECT *).")
        else:
            lines.append("If unsure, default to SELECT *.")

    lines.extend(
        [
            "If the question says \"by\" or \"per <dimension>\", you MUST aggregate and GROUP BY that column.",
            "Dimension mapping:",
            "- \"owner department\" -> OWNER_DEPARTMENT",
            "- \"department\" -> OWNER_DEPARTMENT",
            "- \"entity\" -> ENTITY_NO",
            "- \"owner\" -> CONTRACT_OWNER",
            "- \"stakeholder\" -> CONTRACT_STAKEHOLDER_1",
            "If the question mentions \"gross value\", define GROSS_VALUE := NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END and use SUM(GROSS_VALUE).",
            "If it mentions \"net value\" or just \"contract value\", use SUM(NVL(CONTRACT_VALUE_NET_OF_VAT,0)).",
            "If the question asks for count (contains the word 'count' or '(count)'), return COUNT(*) (and include the dimension in SELECT if grouped).",
            "Only add a row limit (FETCH FIRST :top_n ROWS ONLY) if the user explicitly asks for Top N.",
        ]
    )

    default_col = intent.get("date_column") or "REQUEST_DATE"
    lines.append("Add date filter ONLY if user asks. For windows use :date_start and :date_end.")
    lines.append(f"Default window column: {default_col}.")
    lines.append("No prose, comments, or explanations.\n")
    lines.append("Question:")
    lines.append(question.strip())
    lines.append("\n```sql")
    return "\n".join(lines)


def _build_oracle_prompt_strict(
    question: str,
    intent: dict,
    *,
    table: str,
    allowed_columns: list[str],
    allowed_binds: list[str],
) -> str:
    allowed_cols = ", ".join(allowed_columns)
    parts = [
        "Write only an Oracle query. Start with SELECT or WITH.",
        "No code fences. No comments. No explanations. No extra text.",
        f'Table: "{table}"',
        f"Allowed columns: {allowed_cols}",
        f"Allowed binds: {', '.join(allowed_binds)}.",
    ]
    if intent.get("agg") == "count":
        parts.append("Return exactly: SELECT COUNT(*) AS CNT ... with the filters applied. No other columns.")
    else:
        if intent.get("wants_all_columns", True):
            parts.append("If the question does not specify which columns to show, SELECT *.")
    parts.extend(
        [
            "If the question says \"by\" or \"per <dimension>\", you MUST aggregate and GROUP BY that column.",
            "Dimension mapping:",
            "- \"owner department\" -> OWNER_DEPARTMENT",
            "- \"department\" -> OWNER_DEPARTMENT",
            "- \"entity\" -> ENTITY_NO",
            "- \"owner\" -> CONTRACT_OWNER",
            "- \"stakeholder\" -> CONTRACT_STAKEHOLDER_1",
            "If the question mentions \"gross value\", define GROSS_VALUE := NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END and use SUM(GROSS_VALUE).",
            "If it mentions \"net value\" or just \"contract value\", use SUM(NVL(CONTRACT_VALUE_NET_OF_VAT,0)).",
            "If the question asks for count (contains the word 'count' or '(count)'), return COUNT(*) (and include the dimension in SELECT if grouped).",
            "Only add row limit (FETCH FIRST :top_n ROWS ONLY) when Top N is asked explicitly.",
        ]
    )
    default_col = intent.get("date_column") or "REQUEST_DATE"
    parts.append(f"Use :date_start and :date_end on {default_col} when a time window is implied.")
    parts.append("Question:")
    parts.append(question.strip())
    parts.append("Statement:")
    return "\n".join(parts)


def _maybe_rewrite_sql_for_intent(sql: str, intent: dict) -> tuple[str, dict, dict]:
    sql_text = (sql or "").strip()
    meta = {
        "used_projection_rewrite": False,
        "used_limit_inject": False,
        "used_order_inject": False,
    }

    lowered = sql_text.lower()
    top_n_value = None
    user_requested_limit = False
    if isinstance(intent, dict):
        top_n_value = intent.get("top_n")
        user_requested_limit = bool(intent.get("user_requested_top_n"))

    if intent.get("agg") == "count" and "count(" not in lowered:
        match = re.match(r"(?is)\s*select\s+.+?\s+from\s+", sql_text)
        if match:
            sql_text = re.sub(
                r"(?is)\A\s*select\s+.+?\s+from\s+",
                "SELECT COUNT(*) AS CNT FROM ",
                sql_text,
                count=1,
            )
            meta["used_projection_rewrite"] = True
        else:
            sql_text = f"SELECT COUNT(*) AS CNT FROM ({sql_text}) q"
            meta["used_projection_rewrite"] = True
        lowered = sql_text.lower()

    if intent.get("sort_by") and "order by" not in lowered:
        direction = " DESC" if intent.get("sort_desc") else ""
        sql_text = sql_text.rstrip() + f"\nORDER BY {intent['sort_by']}{direction}"
        meta["used_order_inject"] = True
        lowered = sql_text.lower()

    if user_requested_limit and top_n_value and "fetch first" not in lowered:
        sql_text = sql_text.rstrip() + "\nFETCH FIRST :top_n ROWS ONLY"
        meta["used_limit_inject"] = True

    return sql_text, meta, {}


# --- Helpers to compute date ranges ---------------------------------------------------------
def _month_bounds(offset: int = 0, today: date | None = None) -> tuple[date, date]:
    """Return first/last day for month `today` + offset (offset=-1 -> last month)."""

    today = today or date.today()
    year, month = today.year, today.month + offset
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start, end


def _quarter_bounds(offset: int = 0, today: date | None = None) -> tuple[date, date]:
    """Quarter bounds for quarter containing today + offset quarters."""

    today = today or date.today()
    quarter = (today.month - 1) // 3 + 1
    quarter += offset
    year = today.year + (quarter - 1) // 4
    quarter = ((quarter - 1) % 4) + 1
    month_start = 3 * (quarter - 1) + 1
    start = date(year, month_start, 1)
    end_month = month_start + 2
    end = date(year, end_month, monthrange(year, end_month)[1])
    return start, end


def _window_dates_from_compiler(
    question: str, intent_obj: DWIntent, *, today: date | None = None
) -> tuple[Optional[str], Optional[str]]:
    """Resolve window placeholders for the deterministic DW compiler."""

    if not intent_obj or not intent_obj.window_key:
        return None, None

    today = today or date.today()
    key = intent_obj.window_key

    if key == "last_month":
        start, end = _month_bounds(-1, today)
        return start.isoformat(), end.isoformat()

    if key == "last_quarter":
        start, end = _quarter_bounds(-1, today)
        return start.isoformat(), end.isoformat()

    if key == "last_3_months":
        _, end = _month_bounds(-1, today)
        start = end.replace(day=1)
        for _ in range(2):
            prev_month_end = start - timedelta(days=1)
            start = prev_month_end.replace(day=1)
        return start.isoformat(), end.isoformat()

    if key == "next_n_days":
        days = intent_obj.window_param or 0
        if days <= 0:
            match = re.search(r"\bnext\s+(\d{1,3})\s+days?\b", (question or "").lower())
            if match:
                try:
                    days = int(match.group(1))
                except ValueError:
                    days = 0
        if days <= 0:
            return None, None
        start = today
        end = today + timedelta(days=days)
        return start.isoformat(), end.isoformat()

    return None, None


def derive_window_from_text(q: str) -> dict:
    """Best-effort parser for common date windows from free-form text."""

    lowered = (q or "").lower().strip()
    if not lowered:
        return {}

    today = date.today()

    match = re.search(r"\bnext\s+(\d{1,3})\s+days?\b", lowered)
    if match:
        days = int(match.group(1))
        return {
            "start": today.isoformat(),
            "end": (today + timedelta(days=days)).isoformat(),
        }

    match = re.search(r"\blast\s+(\d{1,3})\s+days?\b", lowered)
    if match:
        days = int(match.group(1))
        return {
            "start": (today - timedelta(days=days)).isoformat(),
            "end": today.isoformat(),
        }

    if "last month" in lowered:
        start, end = _month_bounds(-1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "this month" in lowered or "current month" in lowered:
        start, end = _month_bounds(0, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "next month" in lowered:
        start, end = _month_bounds(+1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}

    if "last quarter" in lowered:
        start, end = _quarter_bounds(-1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "this quarter" in lowered or "current quarter" in lowered:
        start, end = _quarter_bounds(0, today)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if "next quarter" in lowered:
        start, end = _quarter_bounds(+1, today)
        return {"start": start.isoformat(), "end": end.isoformat()}

    if "next 30 days" in lowered:
        return {
            "start": today.isoformat(),
            "end": (today + timedelta(days=30)).isoformat(),
        }

    return {}


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no"}


NAMESPACE = os.environ.get("DW_NAMESPACE", "dw::common")
DW_INCLUDE_DEBUG = _env_truthy("DW_INCLUDE_DEBUG", default=True)


def _as_dict(value) -> dict:
    return value if isinstance(value, Mapping) else {}


def _llm_out_default() -> dict:
    return {"prompt": "", "raw1": "", "raw2": "", "raw_strict": "", "errors": []}


def _settings():
    return Settings()


def _heuristic_fill(question: str, intent: dict, default_date_col: str) -> dict:
    if not isinstance(intent, dict):
        return {}

    upper = (question or "").upper()

    derived_window = derive_window_from_text(question or "")
    if derived_window and not intent.get("explicit_dates"):
        intent["explicit_dates"] = derived_window

    if intent.get("has_time_window") is None and derived_window:
        intent["has_time_window"] = True

    if intent.get("date_column") is None:
        if "END_DATE" in upper:
            intent["date_column"] = "END_DATE"
        elif "START_DATE" in upper:
            intent["date_column"] = "START_DATE"
        elif "REQUEST_DATE" in upper:
            intent["date_column"] = "REQUEST_DATE"
        elif intent.get("has_time_window"):
            intent["date_column"] = default_date_col

    if intent.get("explicit_dates") and intent.get("has_time_window") is None:
        intent["has_time_window"] = True

    if intent.get("has_time_window") and intent.get("date_column") is None:
        intent["date_column"] = default_date_col

    return intent


def _synthesize_window_query(table: str, date_col: str, top_n: Optional[int] = None) -> str:
    table_literal = table.strip() or "Contract"
    if not table_literal.startswith('"') or not table_literal.endswith('"'):
        table_literal = f'"{table_literal.strip("\"")}"'

    base = f"""
SELECT
  CONTRACT_ID,
  CONTRACT_OWNER,
  {date_col} AS WINDOW_DATE,
  CONTRACT_VALUE_NET_OF_VAT
FROM {table_literal}
WHERE {date_col} BETWEEN :date_start AND :date_end
ORDER BY {date_col} ASC
""".strip()

    if top_n is not None:
        try:
            top_val = int(top_n)
        except Exception:
            top_val = None
        if top_val and top_val > 0:
            base = f"{base}\nFETCH FIRST {top_val} ROWS ONLY"

    return base


def _write_csv(rows, headers) -> str:
    if not rows:
        return None
    out_dir = pathlib.Path(os.environ.get("DW_EXPORT_DIR", "/tmp/dw_exports"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = out_dir / f"dw_{ts}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
    return path


@dw_bp.route("/answer", methods=["POST"])
def answer():
    settings = _settings()
    ds_registry = DatasourceRegistry(settings=settings, namespace=NAMESPACE)
    mem = get_mem_engine(settings)

    body = request.get_json(force=True, silent=False) or {}
    q = (body.get("question") or "").strip()
    top_n_from_text = _parse_top_n(q)
    auth_email = body.get("auth_email")
    prefixes = body.get("prefixes") or []
    include_debug = DW_INCLUDE_DEBUG or (request.args.get("debug") == "true")

    try:
        window_days = int(body.get("window_days") or 0)
    except (TypeError, ValueError):
        window_days = 0
    date_column_override = (body.get("date_column") or "").upper().strip()
    override_explicit_dates = None
    override_date_column = None
    if window_days > 0:
        override_start = date.today()
        override_end = override_start + timedelta(days=window_days)
        override_explicit_dates = {
            "start": override_start.isoformat(),
            "end": override_end.isoformat(),
        }
        override_date_column = date_column_override or "END_DATE"

    table_name = settings.get("DW_CONTRACT_TABLE", scope="namespace") or "Contract"
    default_date_col = settings.get("DW_DATE_COLUMN", scope="namespace") or "REQUEST_DATE"

    def _prompt_builder(question_text: str, _ctx: dict, intent_data: dict | None) -> str:
        return _build_oracle_prompt(
            question_text,
            intent_data or {},
            table=table_name,
            allowed_columns=ALLOWED_COLUMNS,
            allowed_binds=ALLOWED_BINDS,
        )

    ctx = {
        "table": table_name,
        "allowed_columns": ALLOWED_COLUMNS,
        "allowed_binds": ALLOWED_BINDS,
        "default_date_col": default_date_col,
        "prompt_builder": _prompt_builder,
    }

    with mem.begin() as conn:
        inq_id = conn.execute(
            text(
                """
            INSERT INTO mem_inquiries(namespace, question, auth_email, prefixes, status, created_at, updated_at)
            VALUES (:ns, :q, :email, CAST(:pfx AS jsonb), 'open', NOW(), NOW())
            RETURNING id
        """
            ),
            {"ns": NAMESPACE, "q": q, "email": auth_email, "pfx": json.dumps(prefixes)},
        ).scalar_one()

    log_event(
        log,
        "dw",
        "inquiry_start",
        {
            "id": inq_id,
            "q": q,
            "email": auth_email,
            "ns": NAMESPACE,
            "prefixes": prefixes,
        },
    )

    clarifier = clarify_intent(q, ctx)
    clarifier_raw = ""
    parsed_from_clarifier: dict = {}
    if isinstance(clarifier, dict):
        clarifier_raw = clarifier.get("raw") or ""
        maybe_intent = clarifier.get("intent")
        if isinstance(maybe_intent, dict):
            parsed_from_clarifier = maybe_intent

    parsed = _parse_clarifier_output(clarifier_raw) or {}
    if not parsed and parsed_from_clarifier:
        parsed = parsed_from_clarifier

    intent = _normalize_intent(q, parsed)
    compiler_intent = extract_intent(q)

    if override_explicit_dates:
        intent["explicit_dates"] = override_explicit_dates
        intent["has_time_window"] = True
        if override_date_column:
            intent["date_column"] = override_date_column
    elif date_column_override:
        intent["date_column"] = date_column_override

    if override_date_column:
        compiler_intent.date_column = override_date_column

    intent = _heuristic_fill(q, intent, default_date_col)

    if intent.get("date_column"):
        compiler_intent.date_column = intent.get("date_column")
    if compiler_intent.date_column:
        compiler_intent.date_column = str(compiler_intent.date_column).upper()

    measure = _detect_measure(q)
    if intent.get("agg") == "count":
        measure = "count"
        compiler_intent.agg = "count"
    elif measure == "count":
        intent["agg"] = "count"
        compiler_intent.agg = "count"

    if measure in {"gross", "net"}:
        compiler_intent.measure = measure

    group_col = _extract_group_by(q)
    if not compiler_intent.dimension and group_col:
        compiler_intent.dimension = group_col
    topn_req = _extract_topn(q)
    question_requested_topn = bool(top_n_from_text or topn_req)

    if intent.get("top_n") is not None:
        try:
            topn_int = int(intent["top_n"])
        except Exception:
            topn_int = None
        if topn_int is not None:
            topn_req = topn_int
            intent["top_n"] = topn_int
    elif top_n_from_text:
        topn_req = top_n_from_text
        intent["top_n"] = topn_req
    elif topn_req is not None:
        intent["top_n"] = topn_req

    if question_requested_topn:
        intent["user_requested_top_n"] = True
        compiler_intent.user_requested_top_n = True

    if isinstance(topn_req, int) and topn_req <= 0:
        topn_req = None

    if isinstance(topn_req, int) and topn_req > 0:
        compiler_intent.top_n = topn_req

    wants_all_default = _should_select_all_columns(q, group_col, measure)
    intent["wants_all_columns"] = wants_all_default
    compiler_intent.wants_all_columns = wants_all_default

    explicit_dates = intent.get("explicit_dates") if isinstance(intent.get("explicit_dates"), dict) else {}
    date_start = explicit_dates.get("start")
    date_end = explicit_dates.get("end")
    date_col = (intent.get("date_column") or default_date_col or "REQUEST_DATE").upper()

    compiler_date_start = date_start
    compiler_date_end = date_end
    if not (compiler_date_start and compiler_date_end):
        c_start, c_end = _window_dates_from_compiler(q, compiler_intent)
        if c_start and c_end:
            compiler_date_start = c_start
            compiler_date_end = c_end
        elif compiler_intent.window_key:
            compiler_intent.window_key = None
    elif not compiler_intent.window_key:
        compiler_intent.window_key = "explicit"

    fallback_sql: Optional[str] = None
    fallback_bind_values: dict[str, Any] = {}

    prefer_compiler = (
        bool(compiler_intent.dimension)
        or compiler_intent.agg == "count"
        or compiler_intent.user_requested_top_n
    )

    if prefer_compiler:
        compiler_binds: dict[str, Any] = {}
        if compiler_intent.window_key and compiler_date_start and compiler_date_end:
            compiler_binds["date_start"] = compiler_date_start
            compiler_binds["date_end"] = compiler_date_end
        if compiler_intent.user_requested_top_n and compiler_intent.top_n:
            compiler_binds["top_n"] = compiler_intent.top_n
        fallback_sql = compose_sql(compiler_intent, table=table_name)
        fallback_bind_values = compiler_binds

    if fallback_sql is None and date_start and date_end and (measure == "count" or group_col):
        fallback_bind_values = {"date_start": date_start, "date_end": date_end}
        if question_requested_topn and topn_req:
            fallback_bind_values["top_n"] = topn_req
        table_literal = table_name.strip()
        if measure == "count":
            fallback_sql = _build_count_sql(
                table=table_literal,
                date_col=date_col,
                start=date_start,
                end=date_end,
                group_col=group_col,
                topn=topn_req if question_requested_topn else None,
            )
        elif group_col:
            fallback_sql = _build_agg_sql(
                table=table_literal,
                date_col=date_col,
                start=date_start,
                end=date_end,
                group_col=group_col,
                measure=measure,
                topn=topn_req if question_requested_topn else None,
            )

    explicit_projection = _mentions_specific_projection(q)
    if explicit_projection:
        intent["wants_all_columns"] = False

    if intent.get("agg") == "count":
        intent["wants_all_columns"] = False

    wants_all_columns = bool(intent.get("wants_all_columns", True))

    if top_n_from_text and not intent.get("top_n"):
        intent["top_n"] = top_n_from_text

    if intent.get("explicit_dates") and intent.get("has_time_window") is None:
        intent["has_time_window"] = True
    if intent.get("has_time_window") and intent.get("date_column") is None:
        intent["date_column"] = default_date_col

    log_event(log, "dw", "clarifier_intent_adjusted", json.loads(json.dumps(intent, default=str)))
    if clarifier_raw and include_debug:
        log_event(
            log,
            "dw",
            "clarifier_raw_debug",
            {"size": len(clarifier_raw), "text": clarifier_raw[:900]},
        )

    used_rule_fallback = bool(fallback_sql)

    if used_rule_fallback:
        llm_out = {"prompt": "", "raw1": fallback_sql, "sql": fallback_sql}
    else:
        try:
            llm_out = nl_to_sql_with_llm(q, ctx, intent=intent)
        except Exception as exc:  # pragma: no cover - defensive guard
            log.exception("dw nl_to_sql_with_llm failed")
            llm_out = {"errors": [f"llm_generate:{type(exc).__name__}:{exc}"]}

    d = _as_dict(llm_out) or _llm_out_default()
    prompt_text = d.get("prompt", "") or ""
    raw1 = d.get("raw1", "") or ""
    raw2 = d.get("raw2", "") or ""
    raw_strict_hint = d.get("raw_strict", "") or ""

    strict_attempted = False
    strict_raw = ""

    def _strict_retry() -> str:
        nonlocal strict_attempted, strict_raw
        if strict_attempted:
            return strict_raw
        strict_attempted = True
        mdl = get_model("sql")
        if mdl is None:
            strict_raw = ""
            log_event(log, "dw", "llm_raw_strict", {"size": 0, "skipped": True})
            return strict_raw
        strict_prompt = _build_oracle_prompt_strict(
            q,
            intent,
            table=table_name,
            allowed_columns=ALLOWED_COLUMNS,
            allowed_binds=ALLOWED_BINDS,
        )
        try:
            strict_raw = mdl.generate(
                strict_prompt,
                max_new_tokens=200,
                temperature=0.0,
                top_p=0.95,
                stop=["```", "<<JSON>>"],
            )
        except Exception as exc:  # pragma: no cover - logging only
            strict_raw = ""
            log_event(log, "dw", "strict_retry_error", {"error": str(exc)})
        log_event(log, "dw", "llm_raw_strict", {"size": len(strict_raw)})
        return strict_raw

    def _maybe_synthesize(reason: str) -> str:
        explicit = intent.get("explicit_dates") if isinstance(intent.get("explicit_dates"), dict) else None
        if not explicit:
            return ""
        start = explicit.get("start")
        end = explicit.get("end")
        if not (start and end):
            return ""
        date_col_for_window = intent.get("date_column") or default_date_col
        if not date_col_for_window:
            return ""
        if date_col_for_window not in ALLOWED_COLUMNS:
            return ""
        table_clean = table_name.strip().strip('"')
        if table_clean.lower() != "contract":
            return ""
        top_n_val = intent.get("top_n")
        top_n_num: Optional[int] = None
        if top_n_val is not None:
            try:
                top_n_num = int(top_n_val)
            except Exception:
                top_n_num = None
        synth_sql = _synthesize_window_query(table_name, date_col_for_window, top_n_num)
        if synth_sql:
            log_event(
                log,
                "dw",
                "synthetic_sql_fallback",
                {
                    "reason": reason,
                    "table": table_name,
                    "date_column": date_col_for_window,
                    "start": start,
                    "end": end,
                    "top_n": top_n_num,
                },
            )
        return synth_sql

    sql_from_llm = d.get("sql") or ""
    sql_final = ""
    candidates = [
        (raw_strict_hint, raw1 or raw2 or sql_from_llm),
        (raw1, raw2 or sql_from_llm),
        (raw2, raw1 or sql_from_llm),
        (sql_from_llm, raw1 or raw2),
    ]
    for primary, fallback in candidates:
        if not primary and not fallback:
            continue
        sql_final = sanitize_oracle_sql(primary, fallback)
        if sql_final:
            break
    if not sql_final:
        strict_raw = _strict_retry()
        if strict_raw:
            raw_strict_hint = strict_raw
            sql_final = sanitize_oracle_sql(strict_raw, raw1 or raw2 or sql_from_llm)
    if not sql_final:
        sql_final = _maybe_synthesize("empty_sanitize")
    if not sql_final:
        sql_payload = {"size": 0}
        sql_payload["sql"] = "" if include_debug else "<hidden>"
        log_event(log, "dw", "final_sql", sql_payload)
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = 'no_sql_extracted',
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {"sql": "", "id": inq_id},
            )
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "no_sql_extracted"},
        )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "no_sql_extracted",
            "sql": "",
            "questions": [
                "I couldn't extract a SELECT statement. Can you restate the request with the date column and time window?",
            ],
        }
        if include_debug:
            debug_payload = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "clarifier_raw": clarifier_raw,
            }
            if raw2:
                debug_payload["raw2"] = raw2
            strict_debug = strict_raw if strict_attempted else raw_strict_hint
            if strict_debug:
                debug_payload["raw_strict"] = strict_debug
            errors = d.get("errors") or []
            if errors:
                debug_payload["errors"] = errors
            res["debug"] = debug_payload
        return jsonify(res)

    deterministic_fallback_used = False
    sanitized_sql = _sanitize_sql(sql_final)
    if sanitized_sql:
        sql_final = sanitized_sql
    else:
        date_col_for_fallback = (
            (intent.get("date_column") or "REQUEST_DATE").strip() or "REQUEST_DATE"
        )
        date_col_for_fallback = date_col_for_fallback.upper()
        if not intent.get("date_column"):
            intent["date_column"] = date_col_for_fallback
        start_iso = date_start
        end_iso = date_end
        explicit_dates_value = intent.get("explicit_dates")
        if isinstance(explicit_dates_value, dict):
            start_iso = explicit_dates_value.get("start") or start_iso
            end_iso = explicit_dates_value.get("end") or end_iso
        else:
            inferred_col, inferred_start, inferred_end = _default_window_for(q)
            if not intent.get("date_column"):
                date_col_for_fallback = inferred_col
            if not (start_iso and end_iso) and inferred_start and inferred_end:
                start_iso, end_iso = inferred_start, inferred_end

        top_n_detected = intent.get("top_n") or _detect_top_n(q) or topn_req
        if isinstance(top_n_detected, str) and top_n_detected.isdigit():
            top_n_detected = int(top_n_detected)
        group_dim = group_col or _detect_dimension(q)
        wants_count = intent.get("agg") == "count" or _wants_count(q)
        fallback_sql_text, forced_binds = _fallback_dw_sql(
            question=q,
            date_col=date_col_for_fallback,
            start_iso=start_iso,
            end_iso=end_iso,
            top_n=top_n_detected if isinstance(top_n_detected, int) else None,
            wants_count=wants_count,
            group_dim=group_dim,
            table_name=table_name,
        )
        sql_final = fallback_sql_text
        deterministic_fallback_used = True
        used_rule_fallback = True
        fallback_sql = fallback_sql_text
        date_start = start_iso or date_start
        date_end = end_iso or date_end
        if forced_binds:
            fallback_bind_values.update(forced_binds)
        if start_iso and end_iso:
            fallback_bind_values.setdefault("date_start", start_iso)
            fallback_bind_values.setdefault("date_end", end_iso)
        if isinstance(top_n_detected, int) and top_n_detected > 0:
            topn_req = top_n_detected
            fallback_bind_values.setdefault("top_n", top_n_detected)

    if not used_rule_fallback and date_start and date_end:
        lower_sql = sql_final.lower()
        forced_sql: Optional[str] = None
        table_literal = table_name.strip()
        if measure == "count" and "count(" not in lower_sql:
            forced_sql = _build_count_sql(
                table=table_literal,
                date_col=date_col,
                start=date_start,
                end=date_end,
                group_col=group_col,
                topn=topn_req,
            )
        elif group_col and "group by" not in lower_sql:
            forced_sql = _build_agg_sql(
                table=table_literal,
                date_col=date_col,
                start=date_start,
                end=date_end,
                group_col=group_col,
                measure=measure,
                topn=topn_req,
            )
        if forced_sql:
            sql_final = forced_sql
            used_rule_fallback = True
            fallback_sql = forced_sql
            if not fallback_bind_values:
                fallback_bind_values = {"date_start": date_start, "date_end": date_end}
            if topn_req and "top_n" not in fallback_bind_values:
                fallback_bind_values["top_n"] = topn_req

    requested_top_n = intent.get("top_n") if isinstance(intent, dict) else None
    top_n_raw = requested_top_n
    if isinstance(top_n_raw, str):
        try:
            top_n_raw = int(top_n_raw)
        except Exception:
            top_n_raw = None
    top_n_int = top_n_raw if isinstance(top_n_raw, int) else None
    if top_n_int is not None and top_n_int <= 0:
        top_n_int = None
    if top_n_int is not None:
        intent["top_n"] = top_n_int

    user_requested_top_n = bool(intent.get("user_requested_top_n")) if isinstance(intent, dict) else False

    projection_rewrite_applied = False
    limit_strip_applied = False
    intent_rewrite_meta = {
        "used_projection_rewrite": False,
        "used_limit_inject": False,
        "used_order_inject": False,
    }

    def _apply_projection_and_limits(sql_text: str) -> str:
        nonlocal projection_rewrite_applied, limit_strip_applied
        sql_clean = _strip_code_fences(sql_text)
        if wants_all_columns and not user_requested_top_n and _is_simple_contract_select(sql_clean):
            rewritten = _rewrite_projection_to_star(sql_clean)
            if rewritten != sql_clean:
                projection_rewrite_applied = True
                sql_clean = rewritten
        if not user_requested_top_n:
            stripped = _strip_limits(sql_clean)
            if stripped != sql_clean:
                limit_strip_applied = True
                sql_clean = stripped
        return sql_clean

    def _apply_intent_rewrite(sql_text: str) -> str:
        nonlocal intent_rewrite_meta
        rewritten, meta, _ = _maybe_rewrite_sql_for_intent(sql_text, intent)
        for key, value in meta.items():
            if value:
                intent_rewrite_meta[key] = True
        return rewritten

    sql_final = _apply_projection_and_limits(sql_final)
    sql_final = _apply_intent_rewrite(sql_final)

    def _oracle_parse_error(sql_text: str) -> str | None:
        if not sql_text:
            return "empty_sql_after_sanitize"
        if looks_like_instruction(sql_text):
            return "instruction_echo"
        try:
            validate_oracle_sql(sql_text)
        except ValueError as exc:
            return str(exc)
        return None

    parse_error = _oracle_parse_error(sql_final)
    if parse_error:
        strict_raw = _strict_retry()
        if strict_raw:
            raw_strict_hint = strict_raw
            alt_sql = sanitize_oracle_sql(strict_raw, raw1 or raw2 or sql_from_llm)
            if alt_sql:
                sql_final = alt_sql
                sql_final = _apply_projection_and_limits(sql_final)
                sql_final = _apply_intent_rewrite(sql_final)
                parse_error = _oracle_parse_error(sql_final)
    if parse_error:
        synthesized = _maybe_synthesize("parse_error")
        if synthesized:
            sql_final = synthesized
            sql_final = _apply_projection_and_limits(sql_final)
            sql_final = _apply_intent_rewrite(sql_final)
            parse_error = _oracle_parse_error(sql_final)

    sql_payload = {"size": len(sql_final)}
    sql_payload["sql"] = sql_final[:900] if include_debug else "<hidden>"
    log_event(log, "dw", "final_sql", sql_payload)

    bind_names_in_sql = extract_bind_names(sql_final)
    bind_name_map = {name.lower(): name for name in bind_names_in_sql}

    validation = d.get("validation") or basic_checks(sql_final, allowed_binds=ALLOWED_BINDS)
    if validation is None or not isinstance(validation, dict):
        validation = basic_checks(sql_final, allowed_binds=ALLOWED_BINDS)
    if parse_error:
        validation = dict(validation)
        validation.setdefault("errors", [])
        validation["errors"].append(f"oracle_parse:{parse_error}")
        validation["ok"] = False
    validation_payload = {
        "ok": bool(validation.get("ok")),
        "errors": validation.get("errors"),
        "binds": validation.get("binds"),
        "bind_names": sorted(bind_names_in_sql),
    }
    log_event(log, "dw", "validation", json.loads(json.dumps(validation_payload, default=str)))

    if not validation.get("ok"):
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {
                    "sql": sql_final,
                    "err": ",".join(validation.get("errors") or []),
                    "id": inq_id,
                },
            )
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "validation_failed"},
        )
        res = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": (validation.get("errors") or ["error"])[0],
            "sql": sql_final,
            "questions": [
                "I couldn't derive a clean SELECT. Can you rephrase or specify filters (stakeholders, departments, date columns)?"
            ],
        }
        if include_debug:
            debug_payload = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "validation": validation,
                "clarifier_raw": clarifier_raw,
            }
            if raw2:
                debug_payload["raw2"] = raw2
            strict_debug = strict_raw if strict_attempted else raw_strict_hint
            if strict_debug:
                debug_payload["raw_strict"] = strict_debug
            errors = d.get("errors") or []
            if errors:
                debug_payload["errors"] = errors
            res["debug"] = debug_payload
        return jsonify(res)

    needed_canonical = sorted(bind_name_map.keys())

    raw_bind_values = derive_bind_values(q, needed_canonical, intent) or {}
    bind_values: dict[str, object] = dict(raw_bind_values)

    if used_rule_fallback and fallback_bind_values:
        for key, value in fallback_bind_values.items():
            bind_values.setdefault(key, value)

    needs_dates = {"date_start", "date_end"} & set(needed_canonical)
    if needs_dates:
        window = {}
        if isinstance(intent, dict):
            maybe_window = intent.get("explicit_dates")
            if isinstance(maybe_window, dict):
                window = maybe_window
        if (
            (not window or not window.get("start") or not window.get("end"))
            and not (bind_values.get("date_start") and bind_values.get("date_end"))
        ):
            window = derive_window_from_text(q)

        if isinstance(window, dict) and window.get("start") and window.get("end"):
            def _coerce_date(value):
                if isinstance(value, datetime.datetime):
                    return value.date()
                if isinstance(value, date):
                    return value
                if isinstance(value, str):
                    try:
                        return date.fromisoformat(value)
                    except Exception:
                        return value
                return value

            bind_values["date_start"] = _coerce_date(window.get("start"))
            bind_values["date_end"] = _coerce_date(window.get("end"))

    if "top_n" in bind_name_map:
        top_n_val = None
        if isinstance(intent, dict):
            top_n_val = intent.get("top_n")
        if isinstance(top_n_val, int) and top_n_val > 0:
            bind_values["top_n"] = top_n_val

    missing = [
        name
        for name in needed_canonical
        if name not in bind_values or bind_values[name] is None
    ]
    if missing:
        missing_pretty = [bind_name_map.get(name, name) for name in missing]
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status='needs_clarification', last_sql=:sql, last_error='missing_bind_values', updated_at=NOW()
             WHERE id=:id
            """
                ),
                {"sql": sql_final, "id": inq_id},
            )
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "needs_clarification", "reason": "missing_bind_values"},
        )
        resp = {
            "ok": False,
            "status": "needs_clarification",
            "inquiry_id": inq_id,
            "error": "missing_bind_values",
            "sql": sql_final,
            "questions": [
                f"Provide values for: {', '.join(sorted(missing_pretty))} or rephrase with explicit filters."
            ],
        }
        if include_debug:
            resp["debug"] = {
                "intent": intent,
                "prompt": prompt_text,
                "raw1": raw1,
                "validation": validation,
            }
        return jsonify(resp)

    exec_binds = {
        bind_name_map.get(key, key): value
        for key, value in bind_values.items()
        if key in bind_name_map
    }
    ds_bind = bind_name_map.get("date_start")
    de_bind = bind_name_map.get("date_end")

    def _as_loggable(value):
        return value.isoformat() if hasattr(value, "isoformat") else value

    def _describe_window(bind_dict: Dict[str, Any]) -> tuple[Optional[str], Optional[int]]:
        if not ds_bind or not de_bind:
            return None, None
        start_val = bind_dict.get(ds_bind)
        end_val = bind_dict.get(de_bind)
        if not (isinstance(start_val, str) and isinstance(end_val, str)):
            return None, None
        try:
            start_dt = _parse_date_yyyy_mm_dd(start_val).date()
            end_dt = _parse_date_yyyy_mm_dd(end_val).date()
        except Exception:
            return None, None
        window_days = (end_dt - start_dt).days
        if window_days <= 0:
            return None, window_days
        today_date = datetime.datetime.utcnow().date()
        description = f"{window_days}-day window"
        if start_dt == today_date:
            description = f"next {window_days} days"
        elif end_dt == today_date:
            description = f"last {window_days} days"
        return description, window_days

    log_event(
        log,
        "dw",
        "execution_binds",
        {
            "date_start": _as_loggable(exec_binds.get(ds_bind) if ds_bind else None),
            "date_end": _as_loggable(exec_binds.get(de_bind) if de_bind else None),
            "other": sorted(
                [
                    key
                    for key in exec_binds.keys()
                    if key not in {ds_bind, de_bind}
                ]
            ),
        },
    )

    orig_exec_binds = dict(exec_binds)
    initial_window_desc, _ = _describe_window(orig_exec_binds)

    oracle_engine = ds_registry.engine(None)

    def _oracle_exec(sql_text: str, bind_params: Dict[str, Any]) -> tuple[list[Any], list[str]]:
        with oracle_engine.begin() as oc:
            rs_local = oc.execute(text(sql_text), bind_params)
            cols_local = list(rs_local.keys())
            rows_local = rs_local.fetchall()
        return rows_local, cols_local

    rows: list[Any] = []
    headers: list[str] = []
    error = None
    started = datetime.datetime.utcnow()
    try:
        rows, headers = _oracle_exec(sql_final, exec_binds)
    except Exception as exc:
        error = str(exc)
        log_event(log, "dw", "oracle_error", {"error": error})

    duration_ms = int((datetime.datetime.utcnow() - started).total_seconds() * 1000)
    log_event(
        log,
        "dw",
        "execution_result",
        {"rows": len(rows), "cols": headers, "ms": duration_ms},
    )

    initial_rowcount = len(rows)
    widen_meta = None
    if not error:
        exec_state: Dict[str, Any] = {"headers": headers}

        def _exec_for_widen(sql_text: str, bind_params: Dict[str, Any]):
            widened_rows, widened_cols = _oracle_exec(sql_text, bind_params)
            exec_state["headers"] = widened_cols
            return widened_rows, widened_cols

        rows, exec_binds, widen_meta = _maybe_autowiden_and_rerun(
            sql_final,
            exec_binds,
            rows,
            intent,
            settings,
            exec_fn=_exec_for_widen,
        )
        headers = exec_state["headers"]
        if widen_meta:
            log_event(log, "dw", "autowiden", widen_meta)
            for canonical, actual in bind_name_map.items():
                if actual in exec_binds:
                    bind_values[canonical] = exec_binds[actual]

    if error:
        with mem.begin() as conn:
            conn.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'failed',
                       last_sql = :sql,
                       last_error = :err,
                       updated_at = NOW()
                 WHERE id = :id
            """
                ),
                {"sql": sql_final, "err": error, "id": inq_id},
            )
        log_event(
            log,
            "dw",
            "inquiry_status",
            {"id": inq_id, "from": "open", "to": "failed", "reason": "oracle_error"},
        )
        return jsonify({"ok": False, "error": error, "inquiry_id": inq_id, "status": "failed"})

    csv_path = None
    if rows:
        csv_path = _write_csv(rows, headers)
        if csv_path:
            log_event(log, "dw", "csv_export", {"path": str(csv_path)})

    autosave = bool(settings.get("SNIPPETS_AUTOSAVE", scope="namespace", default=True))
    snippet_id = None
    if autosave and rows:
        with mem.begin() as conn:
            snippet_id = conn.execute(
                text(
                    """
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                         input_tables, output_columns, tags, is_verified, created_at, updated_at)
                VALUES (:ns, :title, :desc, :tmpl, :raw, CAST(:inputs AS jsonb), CAST(:cols AS jsonb),
                        CAST(:tags AS jsonb), :verified, NOW(), NOW())
                RETURNING id
            """
                ),
                {
                    "ns": NAMESPACE,
                    "title": f"dw auto: {q[:60]}",
                    "desc": "Auto-saved by DW pipeline",
                    "tmpl": sql_final,
                    "raw": sql_final,
                    "inputs": json.dumps([table_name]),
                    "cols": json.dumps(headers),
                    "tags": json.dumps(["dw", "contracts", "auto"]),
                    "verified": False,
                },
            ).scalar_one()
        log_event(log, "dw", "snippet_saved", {"id": snippet_id})

    with mem.begin() as conn:
        conn.execute(
            text(
                """
            UPDATE mem_inquiries
               SET status='answered', answered_by=:by, answered_at=NOW(), updated_at=NOW(),
                   last_sql=:sql, last_error=NULL
             WHERE id=:id
        """
            ),
            {"by": auth_email, "sql": sql_final, "id": inq_id},
        )
    log_event(
        log,
        "dw",
        "inquiry_status",
        {"id": inq_id, "from": "open", "to": "answered", "rows": len(rows)},
    )

    rowcount = len(rows)

    binds_public = {
        bind_name_map.get(k, k): (
            v.isoformat() if hasattr(v, "isoformat") else v
        )
        for k, v in bind_values.items()
    }
    meta = {
        "rowcount": rowcount,
        "columns": [c.lower() for c in headers] if headers else [],
        "duration_ms": duration_ms,
        "used_repair": bool(d.get("used_repair")),
        "used_strict_retry": strict_attempted and bool(strict_raw),
        "suggested_date_column": intent.get("date_column") or default_date_col,
        "clarifier_intent": intent,
        "binds": binds_public,
        "wants_all_columns": wants_all_columns,
        "used_rule_fallback": used_rule_fallback,
        "used_deterministic_fallback": deterministic_fallback_used,
        "used_projection_rewrite": projection_rewrite_applied,
        "used_limit_strip": limit_strip_applied,
        "intent_projection_rewrite": intent_rewrite_meta["used_projection_rewrite"],
        "intent_limit_inject": intent_rewrite_meta["used_limit_inject"],
        "intent_order_inject": intent_rewrite_meta["used_order_inject"],
        "top_n_from_text": top_n_from_text,
    }
    if widen_meta:
        meta["autowiden"] = widen_meta

    resp = {
        "ok": True,
        "inquiry_id": inq_id,
        "sql": sql_final,
        "rows": [list(r) for r in rows[:200]],
        "csv_path": str(csv_path) if csv_path else None,
        "meta": meta,
    }

    message: Optional[str] = None
    suggestions: list[str] = []
    if rowcount == 0:
        message = "No contracts matched that window."
        if widen_meta:
            message += f" I widened to {widen_meta['autowiden_to_days']} days and still found 0."
            suggestions = [
                "Try a longer window (e.g., next 180 days)",
                "Filter by OWNER_DEPARTMENT or REQUEST_TYPE",
            ]
        else:
            suggestions = [
                "Try a longer window (e.g., next 90 days)",
                "Try ‘contracts ending after today’",
            ]
    elif widen_meta and initial_rowcount == 0 and rowcount > 0:
        window_desc = initial_window_desc or f"{widen_meta['autowiden_from_days']}-day window"
        message = (
            f"No contracts found in the {window_desc}. "
            f"I widened to {widen_meta['autowiden_to_days']} days and found {rowcount} rows."
        )

    if message:
        resp["message"] = message
    if suggestions:
        resp["suggestions"] = suggestions
    if include_debug:
        debug_payload = {
            "intent": intent,
            "prompt": prompt_text,
            "raw1": raw1,
            "validation": validation,
            "clarifier_raw": clarifier_raw,
        }
        if d.get("used_repair"):
            debug_payload["raw2"] = d.get("raw2")
        strict_debug = strict_raw if strict_attempted else raw_strict_hint
        if strict_debug:
            debug_payload["raw_strict"] = strict_debug
        errors = d.get("errors") or []
        if errors:
            debug_payload["errors"] = errors
        debug_payload["projection_rewrite_applied"] = projection_rewrite_applied
        debug_payload["limit_strip_applied"] = limit_strip_applied
        debug_payload["wants_all_columns"] = wants_all_columns
        debug_payload["used_rule_fallback"] = used_rule_fallback
        debug_payload["used_deterministic_fallback"] = deterministic_fallback_used
        debug_payload["intent_projection_rewrite"] = intent_rewrite_meta["used_projection_rewrite"]
        debug_payload["intent_limit_inject"] = intent_rewrite_meta["used_limit_inject"]
        debug_payload["intent_order_inject"] = intent_rewrite_meta["used_order_inject"]
        resp["debug"] = debug_payload
    return jsonify(resp)


def create_dw_blueprint(*args, **kwargs):
    """Factory function returning the DocuWare blueprint."""
    return dw_bp
