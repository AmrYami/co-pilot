"""DocuWare-specific clarifier helper for follow-up questions."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

from core.model_loader import load_llm

SYS = (
    "You're a concise assistant that writes at most 2 short yes/no clarifying questions "
    "to help convert a business request into SQL over a single table named Contract. "
    "Each bullet must end with a question mark."
)

EXAMPLE = (
    "User: top stakeholders by contract value\n"
    "Assistant:\n"
    "- Should we use REQUEST_DATE or END_DATE for the time filter?\n"
    "- Do you want gross contract value (net + VAT)?"
)


def propose_clarifying_questions(user_question: str) -> List[str]:
    clar = load_llm("clarifier")
    if not clar:
        return [
            "Which date field should we use (REQUEST_DATE or END_DATE) and what time window?",
            "Should value be NET, VAT, or NET+VAT (gross)?",
        ]

    handle = clar.get("handle")

    prompt = f"{SYS}\n\n{EXAMPLE}\n\nUser: {user_question}\nAssistant:\n"
    if handle is not None:
        text = handle.generate(prompt, max_new_tokens=96, temperature=0.2, top_p=0.9)
    else:
        tokenizer = clar.get("tokenizer")
        model = clar.get("model")
        if tokenizer is None or model is None:
            text = ""
        else:
            import torch

            device = getattr(model, "device", None)
            if device is None:
                try:
                    device = next(model.parameters()).device
                except Exception:
                    device = None
            inputs = tokenizer(prompt, return_tensors="pt")
            if device is not None:
                inputs = {k: v.to(device) for k, v in inputs.items()}
            with torch.inference_mode():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=96,
                    do_sample=True,
                    temperature=0.2,
                    top_p=0.9,
                    pad_token_id=tokenizer.eos_token_id,
                )
            text = tokenizer.decode(outputs[0], skip_special_tokens=True)

    tail = text.split("Assistant:")[-1].strip()
    lines = []
    for raw_line in tail.splitlines():
        cleaned = raw_line.strip()
        if not cleaned:
            continue
        cleaned = cleaned.lstrip("-• ").strip()
        if not cleaned.endswith("?"):
            cleaned = f"{cleaned}?"
        lines.append(cleaned)

    if lines:
        return lines[:2]

    return [
        "Should we use REQUEST_DATE or END_DATE for the time filter?",
        "Do you need gross contract value (net + VAT)?",
    ]


_TEXT_TOP = {
    "ten": 10,
    "five": 5,
    "three": 3,
    "twenty": 20,
    "twenty five": 25,
    "thirty": 30,
}

_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.IGNORECASE)
_NEXT_DAYS_RE = re.compile(r"\bnext\s+(\d+)\s+day", re.IGNORECASE)
_YEAR_RE = re.compile(r"\bin\s+(20\d{2})\b", re.IGNORECASE)
_REQUEST_TYPE_RE = re.compile(
    r"request\s*type\s*(?:=|is|:)?\s*['\"]?\s*([A-Za-z0-9_ /-]+)['\"]?",
    re.IGNORECASE,
)


def _default_date_column(question: str, fallback: str) -> str:
    q = (question or "").lower()
    if "end date" in q or "expiry" in q or "expires" in q:
        return "END_DATE"
    if "start date" in q or "begin date" in q:
        return "START_DATE"
    if "request date" in q:
        return "REQUEST_DATE"
    return fallback or "REQUEST_DATE"


def _as_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min)


def _window_next_days(today: date, days: int) -> Dict[str, datetime]:
    start = _as_datetime(today)
    end = _as_datetime(today + timedelta(days=days))
    return {"start": start, "end": end, "label": f"next {days} days"}


def _window_last_month(today: date) -> Dict[str, datetime]:
    first_this = today.replace(day=1)
    last_month_end = first_this
    last_month_start = (first_this - timedelta(days=1)).replace(day=1)
    return {
        "start": _as_datetime(last_month_start),
        "end": _as_datetime(last_month_end),
        "label": "last month",
    }


def _window_year(year: int) -> Dict[str, datetime]:
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    return {"start": start, "end": end, "label": f"in {year}"}


def _detect_window(question: str) -> Optional[Dict[str, datetime]]:
    today = datetime.utcnow().date()
    lowered = (question or "").lower()

    if "last month" in lowered or "previous month" in lowered:
        return _window_last_month(today)

    match = _NEXT_DAYS_RE.search(lowered)
    if match:
        try:
            days = int(match.group(1))
        except ValueError:
            days = 0
        if days > 0:
            return _window_next_days(today, days)

    match = _YEAR_RE.search(lowered)
    if match:
        year = int(match.group(1))
        if 2000 <= year <= 2100:
            return _window_year(year)

    return None


def _extract_top_n(question: str) -> Optional[int]:
    lowered = (question or "").lower()
    match = _TOP_RE.search(lowered)
    if match:
        try:
            return max(1, min(int(match.group(1)), 500))
        except ValueError:
            pass
    for text_value, number in _TEXT_TOP.items():
        if f"top {text_value}" in lowered:
            return number
    return None


def _extract_request_type(question: str) -> Optional[str]:
    match = _REQUEST_TYPE_RE.search(question or "")
    if match:
        value = match.group(1).strip()
        return value if value else None
    return None


def analyze_question_intent(question: str, *, default_date_column: str = "REQUEST_DATE") -> Dict[str, object]:
    """Return a deterministic context dict for downstream SQL generation."""

    intent: Dict[str, object] = {
        "date_column": _default_date_column(question, default_date_column),
        "filters": {},
        "hints": [],
    }

    window = _detect_window(question)
    if window:
        intent["date_window"] = {"start": window["start"], "end": window["end"]}
        intent["window_label"] = window.get("label")

    top_n = _extract_top_n(question)
    if top_n is not None:
        intent["top_n"] = top_n

    req_type = _extract_request_type(question)
    if req_type:
        filters = intent.get("filters") or {}
        filters["request_type"] = req_type
        intent["filters"] = filters

    lowered = (question or "").lower()
    if "stakeholder" in lowered:
        intent.setdefault("hints", []).append("stakeholder_unpivot")

    return intent
