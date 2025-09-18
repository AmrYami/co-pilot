"""Lightweight DocuWare LLM wrapper for Oracle-focused SQL generation."""

from __future__ import annotations

import os
import re
from typing import Optional

from core.settings import Settings
from core.model_loader import load_llm as _load_llm

from .prompts import FEWSHOTS, SYSTEM_INSTRUCTIONS

_SQL_ONLY = re.compile(r"(?is)^\s*(?:--.*\n|\s*)*(with|select)\b")


def _build_prompt(question: str) -> str:
    """Construct a prompt with optional few-shot guidance."""
    q_low = question.lower()
    best: Optional[tuple[str, str]] = None
    for q, sql in FEWSHOTS:
        tokens = q.split()
        if tokens and all(tok in q_low for tok in tokens[:3]):
            best = (q, sql)
            break

    shots = ""
    if best:
        shots = f"\n-- Example for: {best[0]}\n{best[1]}\n"

    return f"{SYSTEM_INSTRUCTIONS}\n{shots}\n-- User question:\n-- {question}\n"


def _enforce_select_only(sql: str) -> str:
    """Ensure generated SQL is a safe SELECT/CTE statement."""
    s = sql.strip().strip(";")
    if not _SQL_ONLY.match(s):
        raise ValueError("Generated SQL is not a SELECT/CTE.")
    banned = ["insert ", "update ", "delete ", "merge ", "alter ", "drop ", "create "]
    low = s.lower()
    if any(b in low for b in banned):
        raise ValueError("Non-SELECT statement detected.")
    return s


class DWLLM:
    """Simple adapter that turns natural language into Oracle SQL."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = _load_llm(settings)

    def nl_to_sql(self, question: str) -> str:
        prompt = _build_prompt(question)
        stop = os.getenv("STOP", "</s>,<|im_end|>").split(",")
        gen_kwargs = {
            "max_new_tokens": int(os.getenv("GENERATION_MAX_NEW_TOKENS", "256")),
            "temperature": float(os.getenv("GENERATION_TEMPERATURE", "0.2")),
            "top_p": float(os.getenv("GENERATION_TOP_P", "0.9")),
            "stop": stop,
        }
        out = self.llm.generate(prompt, **gen_kwargs)
        code = out
        if "```" in out:
            parts = out.split("```")
            for i in range(1, len(parts), 2):
                cand = parts[i]
                if "select" in cand.lower() or "with" in cand.lower():
                    code = cand
                    break
        return _enforce_select_only(code)
