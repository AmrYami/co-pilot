"""Lightweight DocuWare LLM wrapper for Oracle-focused SQL generation."""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Sequence, Tuple

from core.model_loader import load_llm as _load_llm
from core.settings import Settings

from .sql_kit import build_nl2sql_prompt

_SQL_ONLY = re.compile(r"(?is)^\s*(?:--.*\n|\s*)*(with|select)\b")
_LIMIT_END = re.compile(r"(?is)\s+limit\s+([:a-zA-Z0-9_]+)\s*$")
_CODE_BLOCK = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


def _strip_stop_tokens(text: str, stop: Sequence[str]) -> str:
    cleaned = text
    for token in stop:
        if not token:
            continue
        idx = cleaned.find(token)
        if idx >= 0:
            cleaned = cleaned[:idx]
    return cleaned


def _extract_sql_block(output: str) -> str:
    for match in _CODE_BLOCK.finditer(output):
        candidate = match.group(1).strip()
        if candidate:
            return candidate
    return output


def _oracleize_limit(sql: str) -> str:
    match = _LIMIT_END.search(sql)
    if not match:
        return sql
    value = match.group(1)
    replacement = f" FETCH FIRST {value} ROWS ONLY"
    start, end = match.span()
    return sql[: start].rstrip() + replacement


def _enforce_select_only(sql: str) -> str:
    cleaned = sql.strip().strip(";")
    if not _SQL_ONLY.match(cleaned):
        raise ValueError("Generated SQL is not a SELECT/CTE statement.")
    lower = cleaned.lower()
    for banned in ("insert ", "update ", "delete ", "merge ", "alter ", "drop ", "create "):
        if banned in lower:
            raise ValueError("Non-SELECT statement detected.")
    return cleaned


class DWLLM:
    """Simple adapter that turns natural language into Oracle SQL."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = _load_llm(settings)
        stop_tokens = os.getenv("STOP", "</s>,<|im_end|>")
        self.stop: List[str] = [tok for tok in (stop_tokens.split(",") if stop_tokens else []) if tok]
        self.gen_kwargs = {
            "max_new_tokens": int(os.getenv("GENERATION_MAX_NEW_TOKENS", "256")),
            "temperature": float(os.getenv("GENERATION_TEMPERATURE", "0.2")),
            "top_p": float(os.getenv("GENERATION_TOP_P", "0.9")),
        }

    def nl_to_sql(
        self,
        question: str,
        extra_shots: Iterable[Tuple[str, str]] | None = None,
    ) -> str:
        prompt = build_nl2sql_prompt(question, extra_shots)
        generated = self.llm.generate(
            prompt,
            stop=self.stop if self.stop else None,
            **self.gen_kwargs,
        )
        trimmed = _strip_stop_tokens(generated, self.stop)
        sql_candidate = _extract_sql_block(trimmed)
        sql_candidate = _oracleize_limit(sql_candidate)
        return _enforce_select_only(sql_candidate)
