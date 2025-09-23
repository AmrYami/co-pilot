from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Dict, Optional

from core.logging_utils import get_logger, log_event
from core.model_loader import get_model
from core.nlu.clarify import infer_intent
from core.nlu.types import NLIntent
from .validator import basic_checks, extract_sql

_MONTH_WORDS = re.compile(r"\blast\s+month\b", re.IGNORECASE)
_NEXT_30 = re.compile(r"\bnext\s+30\s+days\b", re.IGNORECASE)
_LAST_DAYS = re.compile(r"\blast\s+(\d+)\s+days\b", re.IGNORECASE)


def _intent_payload(intent: NLIntent, default_col: str) -> Dict[str, object]:
    payload: Dict[str, object] = {}

    payload["top_n"] = intent.top_n
    payload["group_by"] = intent.group_by
    payload["agg"] = intent.agg
    payload["sort_by"] = intent.sort_by
    payload["sort_desc"] = intent.sort_desc
    payload["wants_all_columns"] = intent.wants_all_columns

    window = intent.explicit_dates
    if window and window.start and window.end:
        payload["explicit_dates"] = {"start": window.start, "end": window.end}
        payload["has_time_window"] = (
            True if intent.has_time_window is None else intent.has_time_window
        )
        payload["date_column"] = (intent.date_column or default_col).upper()
    else:
        payload["explicit_dates"] = None
        payload["has_time_window"] = intent.has_time_window
        column = intent.date_column or default_col
        payload["date_column"] = column.upper() if isinstance(column, str) else column

    return payload


def _dates_for_last_month(today: date) -> tuple[date, date]:
    first_this = today.replace(day=1)
    last_month_end = first_this
    last_month_last = last_month_end - timedelta(days=1)
    last_month_first = last_month_last.replace(day=1)
    return last_month_first, last_month_end


log = get_logger("main")


def clarify_intent(question: str, context: dict) -> Dict[str, object]:
    default_col = context.get("default_date_col", "REQUEST_DATE")
    settings = context.get("settings")
    all_columns_default = context.get("all_columns_default", True)

    base_intent = infer_intent(
        question,
        default_date_col=default_col,
        all_columns_default=all_columns_default,
    )
    data = _intent_payload(base_intent, default_col)

    upper = (question or "").upper()
    if "END_DATE" in upper:
        data["date_column"] = "END_DATE"
    elif "START_DATE" in upper:
        data["date_column"] = "START_DATE"
    elif "REQUEST_DATE" in upper:
        data["date_column"] = "REQUEST_DATE"

    if data.get("explicit_dates") and data.get("has_time_window") is None:
        data["has_time_window"] = True

    clarifier_enabled = False
    if settings and hasattr(settings, "get_bool"):
        try:
            clarifier_enabled = bool(settings.get_bool("CLARIFIER_ENABLED", False))
        except Exception:
            clarifier_enabled = False

    raw = ""
    mdl = None
    if clarifier_enabled:
        mdl = get_model("clarifier")

    needs_window = not bool(data.get("has_time_window"))
    if needs_window and mdl is not None:
        skeleton = {
            "has_time_window": data.get("has_time_window"),
            "date_column": data.get("date_column"),
            "top_n": data.get("top_n"),
            "explicit_dates": data.get("explicit_dates"),
        }
        prompt = (
            "You are a precise NLU clarifier. Output JSON only.\n"
            "Update only the NULL values in the JSON skeleton. Keys:\n"
            "  has_time_window: boolean\n"
            "  date_column: string|null (END_DATE|REQUEST_DATE|START_DATE)\n"
            "  top_n: integer|null\n"
            "  explicit_dates: object|null {start,end} (ISO dates)\n"
            "Return JSON only between <<JSON>> and <</JSON>>.\n\n"
            f"Question: {question}\n\n<<JSON>>\n{json.dumps(skeleton)}\n<</JSON>>\n"
        )
        log_event(log, "dw", "clarifier_prompt", {"size": len(prompt)})
        try:
            raw = mdl.generate(prompt, max_new_tokens=192)
        except Exception:
            raw = ""
        payload = "{}"
        if raw:
            log_event(
                log,
                "dw",
                "clarifier_raw",
                {"size": len(raw), "text": raw[:1200]},
            )
            start = raw.find("<<JSON>>")
            end = raw.find("<</JSON>>")
            if start != -1 and end != -1 and end > start:
                payload = raw[start + 8 : end].strip() or "{}"
        try:
            parsed = json.loads(payload)
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            has_window_val = parsed.get("has_time_window")
            if data.get("has_time_window") in {None, False} and isinstance(has_window_val, bool):
                data["has_time_window"] = has_window_val
            date_col_val = parsed.get("date_column")
            if data.get("date_column") in {None, ""} and isinstance(date_col_val, str):
                data["date_column"] = date_col_val.upper()
            top_n_val = parsed.get("top_n")
            if data.get("top_n") is None and isinstance(top_n_val, int):
                data["top_n"] = top_n_val
            explicit_val = parsed.get("explicit_dates")
            if (
                not data.get("explicit_dates")
                and isinstance(explicit_val, dict)
                and explicit_val.get("start")
                and explicit_val.get("end")
            ):
                data["explicit_dates"] = {
                    "start": explicit_val.get("start"),
                    "end": explicit_val.get("end"),
                }
                data.setdefault("has_time_window", True)

    if data.get("explicit_dates") and data.get("has_time_window") is None:
        data["has_time_window"] = True

    log_event(
        log,
        "dw",
        "clarifier_intent",
        json.loads(json.dumps({"intent": data}, default=str)),
    )
    return {"intent": data, "raw": raw}


def _build_prompt(question: str, ctx: dict, intent: Dict[str, object]) -> str:
    prompt_builder = ctx.get("prompt_builder")
    if callable(prompt_builder):
        return prompt_builder(question, ctx, intent)

    allowed_cols = ctx.get("allowed_columns", [])
    allowed_binds = ctx.get("allowed_binds", [])
    table = ctx.get("table") or ctx.get("contract_table") or "Contract"
    default_date_col = intent.get("date_column") or ctx.get("default_date_col", "REQUEST_DATE")

    lines = [
        "Return Oracle SQL only inside ```sql fenced block.",
        f'Table: "{table}"',
        f"Allowed columns: {', '.join(allowed_cols)}",
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
        lines.append("Only add a row limit (FETCH FIRST :top_n ROWS ONLY) if the user explicitly asks for Top N.")

    lines.extend([
        "Add date filter ONLY if user asks. For windows use :date_start and :date_end.",
        f"Default window column: {default_date_col}.",
        "No prose, comments, or explanations.",
        "",
        f"Question:\n{question}\n",
        "```sql",
    ])
    return "\n".join(lines)


def nl_to_sql_with_llm(
    question: str,
    ctx: dict,
    *,
    intent: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    mdl = get_model("sql")
    clarifier_raw = None
    if intent is None:
        clarifier = clarify_intent(question, ctx)
        intent = clarifier.get("intent", {})
        clarifier_raw = clarifier.get("raw")

    intent = intent or {}
    prompt = _build_prompt(question, ctx, intent)
    log_event(log, "dw", "sql_prompt_compact", {"size": len(prompt)})
    log_event(log, "dw", "sql_prompt", {"prompt": prompt[:1600]})
    result: Dict[str, object] = {
        "prompt": prompt,
        "raw1": "",
        "raw2": "",
        "raw_strict": "",
        "clarifier_raw": clarifier_raw or "",
        "intent": intent,
        "sql": "",
        "validation": {"ok": False, "errors": [], "binds": [], "bind_names": []},
        "used_repair": False,
        "errors": [],
    }

    if mdl is None:
        result["errors"].append("model_unavailable")
        result["validation"] = {"ok": False, "errors": ["model_unavailable"], "binds": [], "bind_names": []}
        return result

    try:
        raw1 = mdl.generate(prompt, max_new_tokens=192, stop=["```"])
    except Exception as exc:  # pragma: no cover - propagate diagnostics upstream
        log_event(
            log,
            "dw",
            "llm_pass1_error",
            {"error": str(exc), "type": type(exc).__name__},
        )
        result["errors"].append(f"pass1_generate:{type(exc).__name__}:{exc}")
        result["validation"] = {"ok": False, "errors": ["pass1_generate"], "binds": [], "bind_names": []}
        return result

    raw1 = raw1 or ""
    result["raw1"] = raw1
    log_event(log, "dw", "llm_raw_pass1", {"size": len(raw1)})
    sql1 = extract_sql(raw1) or ""
    val1 = basic_checks(sql1, allowed_binds=ctx.get("allowed_binds"))
    result.update({"sql": sql1, "validation": val1, "used_repair": False})

    if val1.get("ok"):
        return result

    repair_lines = [
        f"Errors: {json.dumps(val1['errors'])}",
        "Fix and return only Oracle SQL in ```sql block.",
        f'Table: "{ctx.get("table") or ctx.get("contract_table") or "Contract"}". '
        f"Allowed columns: {', '.join(ctx.get('allowed_columns', []))}. "
        f"Allowed binds: {', '.join(ctx.get('allowed_binds', []))}.",
    ]
    if intent.get("agg") == "count":
        repair_lines.append("Return a single COUNT query: SELECT COUNT(*) AS CNT ... Do not select other columns.")
    else:
        if intent.get("wants_all_columns", True):
            repair_lines.append("If the question does not specify which columns to show, SELECT ALL columns (use SELECT *).")
        else:
            repair_lines.append("If unsure, default to SELECT *.")
        repair_lines.append("Only add a row limit (FETCH FIRST :top_n ROWS ONLY) if the user explicitly asks for Top N.")
    repair_lines.extend(
        [
            f"Default window column: {intent.get('date_column') or ctx.get('default_date_col', 'REQUEST_DATE')}.",
            f"Question:\n{question}\n\nPrevious SQL:\n```sql\n{sql1}\n```\n```sql\n",
        ]
    )
    repair_prompt = "\n".join(repair_lines)
    log_event(log, "dw", "sql_prompt_repair", {"size": len(repair_prompt)})
    try:
        raw2 = mdl.generate(repair_prompt, max_new_tokens=160, stop=["```"])
    except Exception as exc:  # pragma: no cover - propagate diagnostics upstream
        log_event(
            log,
            "dw",
            "llm_pass2_error",
            {"error": str(exc), "type": type(exc).__name__},
        )
        result["errors"].append(f"pass2_generate:{type(exc).__name__}:{exc}")
        return result

    raw2 = raw2 or ""
    result["raw2"] = raw2
    log_event(log, "dw", "llm_raw_pass2", {"size": len(raw2)})
    sql2 = extract_sql(raw2) or ""
    val2 = basic_checks(sql2, allowed_binds=ctx.get("allowed_binds"))
    result.update({"sql": sql2, "validation": val2, "used_repair": True})
    return result


def derive_bind_values(question: str, used_binds: list[str], intent: Dict[str, object]) -> Dict[str, object]:
    binds: Dict[str, object] = {}
    used = {b.lower() for b in used_binds}
    today = date.today()

    if {"date_start", "date_end"} & used:
        explicit = intent.get("explicit_dates") if isinstance(intent, dict) else None
        if isinstance(explicit, dict) and explicit.get("start") and explicit.get("end"):
            try:
                binds["date_start"] = datetime.fromisoformat(str(explicit["start"]))
                binds["date_end"] = datetime.fromisoformat(str(explicit["end"]))
            except Exception:
                binds.pop("date_start", None)
                binds.pop("date_end", None)
        if "date_start" not in binds or "date_end" not in binds:
            if _MONTH_WORDS.search(question or ""):
                ds, de = _dates_for_last_month(today)
                binds["date_start"] = datetime.combine(ds, datetime.min.time())
                binds["date_end"] = datetime.combine(de, datetime.min.time())
            elif _NEXT_30.search(question or ""):
                binds["date_start"] = datetime.combine(today, datetime.min.time())
                binds["date_end"] = datetime.combine(today + timedelta(days=30), datetime.min.time())
            else:
                m = _LAST_DAYS.search(question or "")
                if m:
                    try:
                        days = int(m.group(1))
                    except Exception:
                        days = 30
                    binds["date_start"] = datetime.combine(today - timedelta(days=days), datetime.min.time())
                    binds["date_end"] = datetime.combine(today, datetime.min.time())
                else:
                    binds["date_end"] = datetime.combine(today, datetime.min.time())
                    binds["date_start"] = datetime.combine(today - timedelta(days=30), datetime.min.time())

    if "top_n" in used:
        top_n_val = intent.get("top_n") if isinstance(intent, dict) else None
        if not isinstance(top_n_val, int):
            m = _TOP_N.search(question or "")
            if m:
                try:
                    top_n_val = int(m.group(1))
                except Exception:
                    top_n_val = None
        binds["top_n"] = top_n_val or 10

    return binds


__all__ = [
    "clarify_intent",
    "derive_bind_values",
    "nl_to_sql_with_llm",
]
