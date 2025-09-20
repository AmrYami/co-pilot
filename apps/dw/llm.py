import re
import json
from typing import Dict, Any, Optional, Tuple

from core.model_loader import get_model

_SQL_MARKER = re.compile(r"<<SQL>>\s*(.*?)\s*<<ENDSQL>>", re.IGNORECASE | re.DOTALL)
_SQL_FENCE = re.compile(r"```sql\s*(.*?)\s*```", re.IGNORECASE | re.DOTALL)
_SQL_START = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)

STOP_SEQUENCES = ["```", "<<ENDSQL>>"]


def _extract_json_between(
    text: str,
    start_tag: str = "<<JSON>>",
    end_tag: str = "<</JSON>>",
) -> Optional[Dict[str, Any]]:
    try:
        i = text.find(start_tag)
        j = text.find(end_tag)
        if i != -1 and j != -1 and j > i:
            payload = text[i + len(start_tag) : j].strip()
            if payload:
                return json.loads(payload)
    except Exception:
        pass
    return None


def _clarifier_heuristics(q: str) -> Dict[str, Any]:
    qlow = (q or "").lower()
    has_window = any(
        w in qlow for w in ["last month", "next 30 days", "last 30 days", "in 2024", "between"]
    )
    top_n: Optional[int] = None
    m = re.search(r"\btop\s+(\d+)\b", qlow)
    if m:
        try:
            top_n = int(m.group(1))
        except Exception:
            top_n = None
    date_col: Optional[str] = None
    if "end date" in qlow or "end_date" in qlow:
        date_col = "END_DATE"
    elif "start date" in qlow or "start_date" in qlow:
        date_col = "START_DATE"
    elif "request date" in qlow or "request_date" in qlow:
        date_col = "REQUEST_DATE"
    return {
        "has_time_window": has_window,
        "date_column": date_col,
        "top_n": top_n,
        "explicit_dates": None,
    }


def normalize_intent(intent: Optional[Dict[str, Any]], question: str) -> Dict[str, Any]:
    out = dict(intent or {})
    q = (question or "").lower()

    if out.get("has_time_window") is None:
        tokens = ["last month", "next", "between", "in 20", "since"]
        out["has_time_window"] = any(token in q for token in tokens)

    if out.get("top_n") is None:
        match = re.search(r"\btop\s+(\d+)\b", q)
        out["top_n"] = int(match.group(1)) if match else None

    if out.get("date_column") is None:
        if "end date" in q or "end_date" in q:
            out["date_column"] = "END_DATE"
        elif "start date" in q or "start_date" in q:
            out["date_column"] = "START_DATE"
        elif "request date" in q or "request_date" in q:
            out["date_column"] = "REQUEST_DATE"

    out.setdefault("has_time_window", False)
    out.setdefault("top_n", None)
    out.setdefault("date_column", None)
    out.setdefault("explicit_dates", None)
    return out


def clarify_intent(question: str, context: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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
        "<<JSON>>\n"
        '{"has_time_window": null, "date_column": null, "top_n": null, "explicit_dates": null}'
        "\n<</JSON>>"
    )
    raw = ""
    if mdl is not None:
        try:
            raw = mdl.generate(
                prompt,
                max_new_tokens=128,
                temperature=0.0,
                stop=["<</JSON>>"],
            )
        except Exception:
            raw = ""
    obj = _extract_json_between(raw or "")
    if not obj or not isinstance(obj, dict) or all(v is None for v in obj.values()):
        obj = _clarifier_heuristics(question)
    obj = normalize_intent(obj, question)
    dbg = {
        "ok": True,
        "used": True,
        "raw": raw[:1500] if raw else "",
        "intent": obj,
    }
    return obj, dbg


def _build_sql_prompt(
    question: str,
    llm_context: Dict[str, Any],
    use_window: bool,
    default_date_col: str,
    top_n_hint: Optional[int],
) -> str:
    table = llm_context.get("table") or "Contract"
    allowed_clause = llm_context.get("allowed_columns_clause")
    if not allowed_clause:
        allowed_cols = llm_context.get("allowed_columns")
        if isinstance(allowed_cols, (list, tuple)):
            allowed_clause = ", ".join(allowed_cols)
        elif allowed_cols:
            allowed_clause = str(allowed_cols)
        else:
            allowed_clause = ""

    binds_clause = llm_context.get("binds_whitelist")
    if not binds_clause:
        binds = llm_context.get("allowed_binds")
        if isinstance(binds, (list, tuple)):
            binds_clause = ", ".join(binds)
        elif binds:
            binds_clause = str(binds)
        else:
            binds_clause = ""

    pattern_hint = llm_context.get("unpivot_hint", "")

    lines = [
        "You are a SQL generator for Oracle. Return SQL only between <<SQL>> and <<ENDSQL>>.",
        f'- Use only table "{table}".',
    ]
    if allowed_clause:
        lines.append(f"- Allowed columns only: {allowed_clause}")
    else:
        lines.append("- Use only documented Contract columns.")
    lines.extend(
        [
            "Oracle syntax only (NVL, TRIM, UPPER, LISTAGG ... WITHIN GROUP, FETCH FIRST N ROWS ONLY).",
            "SELECT / CTE only. No DML. No comments. No prose.",
            f"- Allowed named binds only: {binds_clause}" if binds_clause else "- Use only the approved bind names.",
            "- Do not add a date filter unless the user explicitly asks for a window.",
            "- When a window IS requested, use :date_start and :date_end on the correct column.",
            f"- If user doesn’t name a date column, use {default_date_col} for the window.",
        ]
    )
    if pattern_hint:
        lines.append(
            f"- If aggregating by stakeholder, unpivot slots 1..8 via UNION ALL. Pattern: {pattern_hint}"
        )
    if top_n_hint is not None:
        try:
            top_n_value = int(top_n_hint)
        except (TypeError, ValueError):
            top_n_value = None
        if top_n_value:
            lines.append(
                f"- A TOP clause is implied; prefer a literal FETCH FIRST {top_n_value} ROWS ONLY."
            )
    if use_window:
        lines.append("- The user requested a date window; ensure the SQL filters using binds.")
    lines.extend([
        "",
        "Question:",
        question,
        "",
        "<<SQL>>",
    ])
    return "\n".join(lines)


def _build_repair_prompt(
    question: str,
    sql_prev: str,
    errors: Any,
    llm_context: Dict[str, Any],
    default_date_col: str,
    top_n_hint: Optional[int],
) -> str:
    table = llm_context.get("table") or "Contract"
    allowed_clause = llm_context.get("allowed_columns_clause")
    if not allowed_clause:
        allowed_cols = llm_context.get("allowed_columns")
        if isinstance(allowed_cols, (list, tuple)):
            allowed_clause = ", ".join(allowed_cols)
        elif allowed_cols:
            allowed_clause = str(allowed_cols)
        else:
            allowed_clause = ""

    binds_clause = llm_context.get("binds_whitelist")
    if not binds_clause:
        binds = llm_context.get("allowed_binds")
        if isinstance(binds, (list, tuple)):
            binds_clause = ", ".join(binds)
        elif binds:
            binds_clause = str(binds)
        else:
            binds_clause = ""

    pattern_hint = llm_context.get("unpivot_hint", "")
    errors_blob = json.dumps(errors, ensure_ascii=False)

    lines = [
        "Previous SQL had validation errors:",
        errors_blob,
        "",
        "Repair the SQL. Return Oracle SQL only between <<SQL>> and <<ENDSQL>>. No prose. No comments.",
        "Rules:",
        f'- Table: "{table}"',
    ]
    if allowed_clause:
        lines.append(f"- Allowed columns only: {allowed_clause}")
    else:
        lines.append("- Use only documented Contract columns.")
    lines.extend(
        [
            "- Oracle syntax: NVL(), TRIM(), UPPER(), LISTAGG ... WITHIN GROUP, FETCH FIRST N ROWS ONLY.",
            f"- Allowed binds: {binds_clause}" if binds_clause else "- Use only the approved bind names.",
            "- When a time window is requested, use :date_start and :date_end on the correct date column.",
            f"- Default date column: {default_date_col}.",
        ]
    )
    if pattern_hint:
        lines.append(
            f"- If aggregating by stakeholder, unpivot slots 1..8 via UNION ALL. Pattern: {pattern_hint}"
        )
    if top_n_hint is not None:
        try:
            top_n_value = int(top_n_hint)
        except (TypeError, ValueError):
            top_n_value = None
        if top_n_value:
            lines.append(
                f"- A TOP clause is implied; prefer a literal FETCH FIRST {top_n_value} ROWS ONLY."
            )
    lines.extend(
        [
            "",
            "Question:",
            question,
            "",
            "Previous SQL to repair:",
            "<<SQL>>",
            sql_prev,
            "<<ENDSQL>>",
            "",
            "<<SQL>>",
        ]
    )
    return "\n".join(lines)


def _extract_sql(text: str) -> str:
    if not text:
        return ""
    marker = _SQL_MARKER.search(text)
    if marker:
        return marker.group(1).strip()
    fence = _SQL_FENCE.search(text)
    if fence:
        return fence.group(1).strip()
    start = _SQL_START.search(text)
    if start:
        return text[start.start() :].strip()
    return ""


def nl_to_sql_with_llm(question: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
    debug: Dict[str, Any] = {}
    llm_ctx = dict(ctx or {})
    intent, clar_dbg = clarify_intent(question, llm_ctx)
    debug["clarifier"] = clar_dbg
    intent = clar_dbg.get("intent", intent)

    default_date_col = llm_ctx.get("default_date_col") or "REQUEST_DATE"
    top_n_hint = intent.get("top_n") if isinstance(intent, dict) else None
    use_window = bool(intent.get("has_time_window")) if isinstance(intent, dict) else False

    prompt = _build_sql_prompt(question, llm_ctx, use_window, default_date_col, top_n_hint)
    debug["prompt"] = "sql_prompt_v2"

    mdl = get_model("sql")
    if mdl is None:
        return {"ok": False, "sql": "", "debug": debug, "error": "model_unavailable"}

    raw1 = mdl.generate(prompt, max_new_tokens=480, stop=STOP_SEQUENCES)
    debug["raw1"] = raw1[:1500] if raw1 else ""
    sql1 = _extract_sql(raw1)
    debug["sql1"] = sql1

    from .validator import validate_sql

    validation1 = validate_sql(sql1)
    debug["validation1"] = validation1
    v1_ok = bool(sql1) and bool(validation1.get("ok"))
    v1_errors = validation1.get("errors", [])

    if not v1_ok:
        repair_prompt = _build_repair_prompt(
            question,
            sql1,
            v1_errors,
            llm_ctx,
            default_date_col,
            top_n_hint,
        )
        debug["sql_repair_prompt"] = "sql_prompt_v2"
        raw2 = mdl.generate(repair_prompt, max_new_tokens=480, stop=STOP_SEQUENCES)
        debug["raw2"] = raw2[:1500] if raw2 else ""
        sql2 = _extract_sql(raw2)
        debug["sql2"] = sql2
        validation2 = validate_sql(sql2)
        debug["validation2"] = validation2
        v2_ok = bool(sql2) and bool(validation2.get("ok"))
        if v2_ok:
            return {"ok": True, "sql": sql2, "debug": debug, "used_repair": True}
        if v1_ok:
            return {"ok": True, "sql": sql1, "debug": debug, "used_repair": False}
        return {
            "ok": False,
            "sql": sql2,
            "debug": debug,
            "error": "validation_failed",
            "used_repair": True,
            "errors": validation2.get("errors", []),
        }

    return {"ok": True, "sql": sql1, "debug": debug, "used_repair": False}


__all__ = ["clarify_intent", "nl_to_sql_with_llm"]
