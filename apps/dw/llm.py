"""LLM utilities for the DW application."""

from __future__ import annotations

import json
import os
import re
from typing import Callable

from core.model_loader import get_model
from .validator import validate_sql

DEFAULT_BINDS = [
    "date_start",
    "date_end",
    "top_n",
    "owner_name",
    "dept",
    "entity_no",
    "contract_id_pattern",
    "request_type",
]


# ------------- Clarifier -------------

_JSON_RE = re.compile(r"<<JSON>>\s*(\{.*?\})\s*<</JSON>>", re.S)
_FENCE_RE = re.compile(r"```sql\s*(.+?)\s*```", re.I | re.S)
_GENERIC_FENCE_RE = re.compile(r"```\s*(.+?)\s*```", re.S)
_SQL_START_RE = re.compile(r"\b(SELECT|WITH)\b.*", re.I | re.S)


def _extract_sql(text: str) -> str:
    text = text or ""
    match = _FENCE_RE.search(text)
    if match:
        return match.group(1).strip()

    match = _GENERIC_FENCE_RE.search(text)
    if match:
        body = match.group(1).strip()
        start = _SQL_START_RE.search(body)
        if start:
            return body[start.start():].strip()

    start = _SQL_START_RE.search(text)
    if start:
        candidate = text[start.start():].strip()
        candidate = re.split(r"\n`{3,}|\n--\s*END\b", candidate, 1)[0].strip()
        return candidate
    return ""


def _heuristic_intent(question: str) -> dict:
    q = (question or "").lower()
    has_window = any(
        w in q for w in ["last month", "next 30 days", "in 2024", "between", "since "]
    )
    date_col = "END_DATE" if any(w in q for w in ["expire", "expiry", "end date"]) else "REQUEST_DATE"
    top_n = None
    m = re.search(r"\btop\s+(\d+)\b", q)
    if m:
        try:
            top_n = int(m.group(1))
        except Exception:
            top_n = None
    return {
        "has_time_window": bool(has_window),
        "date_column": (
            date_col
            if has_window
            or "end date" in q
            or "request date" in q
            or "start date" in q
            else None
        ),
        "top_n": top_n,
        "explicit_dates": None,
    }


def clarify_intent(question: str, ctx: dict) -> dict:
    """Return {'intent': {...}, 'ok': True, 'used': bool, 'raw': str}."""

    mdl = get_model("clarifier")
    prompt = (
        "You are a precise NLU clarifier. Output JSON only.\n"
        "Keys:\n"
        "  has_time_window: boolean\n"
        "  date_column: string|null (END_DATE|REQUEST_DATE|START_DATE)\n"
        "  top_n: integer|null\n"
        "  explicit_dates: object|null {start,end} (ISO dates)\n"
        "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
        f"Question: {question}\n\n"
        "<<JSON>>\n{}\n<</JSON>>"
    )

    if not mdl:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": ""}

    raw = mdl.generate(prompt, max_new_tokens=192, temperature=0.0)
    m = _JSON_RE.search(raw or "")
    if not m:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": raw or ""}

    try:
        data = json.loads(m.group(1))
        filled = _heuristic_intent(question)
        intent = {
            "has_time_window": data.get("has_time_window", filled["has_time_window"]),
            "date_column": data.get("date_column") or filled["date_column"],
            "top_n": data.get("top_n") if isinstance(data.get("top_n"), int) else filled["top_n"],
            "explicit_dates": data.get("explicit_dates") or None,
        }
        return {"intent": intent, "ok": True, "used": True, "raw": raw}
    except Exception:
        return {"intent": _heuristic_intent(question), "ok": True, "used": False, "raw": raw}


# ------------- SQL Prompt Builders -------------


def _columns_whitelist() -> str:
    return (
        "CONTRACT_ID, CONTRACT_OWNER, "
        "CONTRACT_STAKEHOLDER_1, CONTRACT_STAKEHOLDER_2, CONTRACT_STAKEHOLDER_3, CONTRACT_STAKEHOLDER_4, "
        "CONTRACT_STAKEHOLDER_5, CONTRACT_STAKEHOLDER_6, CONTRACT_STAKEHOLDER_7, CONTRACT_STAKEHOLDER_8, "
        "DEPARTMENT_1, DEPARTMENT_2, DEPARTMENT_3, DEPARTMENT_4, DEPARTMENT_5, DEPARTMENT_6, DEPARTMENT_7, DEPARTMENT_8, "
        "OWNER_DEPARTMENT, CONTRACT_VALUE_NET_OF_VAT, VAT, CONTRACT_PURPOSE, CONTRACT_SUBJECT, "
        "START_DATE, END_DATE, REQUEST_DATE, REQUEST_TYPE, CONTRACT_STATUS, ENTITY_NO, REQUESTER"
    )


def _build_prompt_fenced(question: str, intent: dict, allow_binds) -> str:
    cols = _columns_whitelist()
    date_col = intent.get("date_column") or "REQUEST_DATE"
    binds = ", ".join(allow_binds)
    return (
        "Return Oracle SQL only inside ```sql fenced block. No prose.\n"
        'Table: "Contract"\n'
        f"Allowed columns: {cols}\n"
        "Use Oracle syntax: NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY. SELECT/CTE only.\n"
        f"Allowed binds: {binds}\n"
        f"Default date column: {date_col}.\n"
        "Question:\n"
        f"{question}\n\n"
        "```sql\n"
    )


def _build_prompt_plain(question: str, intent: dict, allow_binds) -> str:
    cols = _columns_whitelist()
    default_date_col = intent.get("date_column") or "REQUEST_DATE"
    has_window = bool(intent.get("has_time_window"))
    window_line = (
        f"If a time window is requested, filter on {default_date_col} BETWEEN :date_start AND :date_end."
        if has_window
        else "Do not add any date filter."
    )
    return (
        "Output only Oracle SQL. No code fences. No prose. Start with SELECT or WITH.\n\n"
        'Table: "Contract"\n'
        f"Allowed columns only: {cols}\n"
        "Oracle syntax only: NVL, TRIM, LISTAGG WITHIN GROUP, FETCH FIRST N ROWS ONLY.\n"
        f"Allowed binds: {', '.join(allow_binds)}\n"
        f"Default date column for windows: {default_date_col}.\n"
        f"{window_line}\n"
        "When aggregating by stakeholder, UNPIVOT across CONTRACT_STAKEHOLDER_1..8 paired with DEPARTMENT_1..8 using UNION ALL.\n"
        "Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).\n\n"
        "Question:\n"
        f"{question}\n\n"
        "SQL:\n"
    )


def _build_prompt_prefix_select(question: str, intent: dict, allow_binds) -> str:
    base = _build_prompt_plain(question, intent, allow_binds)
    return base + "SELECT "


def _get_logger(ctx: dict | None) -> Callable[[str, object], None]:
    if not ctx:
        return lambda *_args, **_kwargs: None
    fn = ctx.get("log") if isinstance(ctx, dict) else None
    if callable(fn):
        return fn  # type: ignore[return-value]
    return lambda *_args, **_kwargs: None


# ------------- NL → SQL (three-pass) -------------


def nl_to_sql_with_llm(question: str, ctx: dict) -> dict:
    sql_mdl = get_model("sql")
    if not sql_mdl:
        raise RuntimeError("SQL model not available")

    allow_binds = ctx.get("allow_binds") or DEFAULT_BINDS
    logger = _get_logger(ctx)

    intent_out = clarify_intent(question, ctx)
    intent = intent_out.get("intent") or {}

    raw_clarifier = intent_out.get("raw")
    if raw_clarifier is not None:
        logger(
            "clarifier_raw",
            {
                "used": intent_out.get("used"),
                "ok": intent_out.get("ok"),
                "raw": (raw_clarifier or "")[:400],
            },
        )
    logger("clarifier_intent", intent)

    max_new_tokens = int(os.getenv("SQL_MAX_NEW_TOKENS", "384"))

    # --- PASS 1: fenced prompt
    prompt1 = _build_prompt_fenced(question, intent, allow_binds)
    logger("sql_prompt_pass1", {"preview": prompt1[:400]})
    raw1 = sql_mdl.generate(prompt1, max_new_tokens=max_new_tokens)
    sql1 = _extract_sql(raw1)
    logger(
        "llm_raw_pass1",
        {
            "size": len(raw1 or ""),
            "head": (raw1 or "")[:40],
            "tail": (raw1 or "")[-40:],
        },
    )
    logger(
        "llm_sql_pass1",
        {
            "size": len(sql1 or ""),
            "preview": (sql1 or "")[:80],
        },
    )
    ok1, errs1, binds1 = validate_sql(sql1, allow_tables=("Contract",), allow_binds=allow_binds)
    logger("validation_pass1", {"ok": ok1, "errors": errs1, "binds": binds1})
    if ok1:
        return {
            "prompt": prompt1,
            "raw": raw1,
            "sql": sql1,
            "binds": binds1,
            "pass": 1,
            "ok": True,
            "errors": [],
            "intent": intent,
            "clarifier": intent_out,
        }

    # --- PASS 2: plain prompt
    prompt2 = _build_prompt_plain(question, intent, allow_binds)
    logger("sql_prompt_pass2", {"preview": prompt2[:400]})
    raw2 = sql_mdl.generate(prompt2, max_new_tokens=max_new_tokens)
    sql2 = _extract_sql(raw2)
    logger(
        "llm_raw_pass2",
        {
            "size": len(raw2 or ""),
            "head": (raw2 or "")[:40],
            "tail": (raw2 or "")[-40:],
        },
    )
    logger(
        "llm_sql_pass2",
        {
            "size": len(sql2 or ""),
            "preview": (sql2 or "")[:80],
        },
    )
    ok2, errs2, binds2 = validate_sql(sql2, allow_tables=("Contract",), allow_binds=allow_binds)
    logger("validation_pass2", {"ok": ok2, "errors": errs2, "binds": binds2})
    if ok2:
        return {
            "prompt": prompt2,
            "raw": raw2,
            "sql": sql2,
            "binds": binds2,
            "pass": 2,
            "ok": True,
            "errors": [],
            "intent": intent,
            "clarifier": intent_out,
        }

    # --- PASS 3: prefix-primed SELECT
    prompt3 = _build_prompt_prefix_select(question, intent, allow_binds)
    logger("sql_prompt_pass3", {"preview": prompt3[:400]})
    raw3 = sql_mdl.generate(prompt3, max_new_tokens=max_new_tokens)
    text3 = raw3 or ""
    if not text3.strip().lower().startswith("select"):
        text3 = "SELECT " + text3.lstrip()
    sql3 = _extract_sql(text3)
    logger(
        "llm_raw_pass3",
        {
            "size": len(raw3 or ""),
            "head": (raw3 or "")[:160],
            "tail": (raw3 or "")[-160:],
        },
    )
    logger(
        "llm_sql_pass3",
        {
            "size": len(sql3 or ""),
            "preview": (sql3 or "")[:240],
        },
    )
    ok3, errs3, binds3 = validate_sql(sql3, allow_tables=("Contract",), allow_binds=allow_binds)
    logger("validation_pass3", {"ok": ok3, "errors": errs3, "binds": binds3})
    if ok3:
        return {
            "prompt": prompt3,
            "raw": text3,
            "sql": sql3,
            "binds": binds3,
            "pass": 3,
            "ok": True,
            "errors": [],
            "intent": intent,
            "clarifier": intent_out,
        }

    # Give up with the last attempt
    return {
        "prompt": prompt3,
        "raw": text3,
        "sql": sql3,
        "binds": binds3,
        "pass": 3,
        "ok": False,
        "errors": errs3,
        "intent": intent,
        "clarifier": intent_out,
    }
