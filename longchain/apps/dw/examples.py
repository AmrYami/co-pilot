"""Examples helpers bridging the legacy in-memory flow with the DB store."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .learning.examples import ExampleStore

_DEFAULT_NAMESPACE = "dw::common"
_STORE = ExampleStore()


def _normalize_q(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


# Fallback in-memory store kept for environments without SQLAlchemy/MEM DB.
_FALLBACK_EXAMPLES: Dict[str, Dict[str, Any]] = {}


def save_example_if_positive(
    inquiry_id: int,
    question: str,
    sql: str,
    rating: Optional[int],
    *,
    namespace: str = _DEFAULT_NAMESPACE,
) -> None:
    """Persist highly-rated examples for lightweight suggestion retrieval."""

    try:
        if rating is None or int(rating) < 4:
            return
        if _STORE.engine:
            _STORE.add_success(namespace, question, sql)
            return
        qn = _normalize_q(question)
        if not qn:
            return
        existing = _FALLBACK_EXAMPLES.get(qn)
        stars = int(rating)
        if not existing or stars > existing.get("stars", 0):
            _FALLBACK_EXAMPLES[qn] = {
                "inquiry_id": inquiry_id,
                "q_norm": qn,
                "sql": sql,
                "stars": stars,
            }
    except Exception:  # pragma: no cover - defensive guard for rating flow
        pass


def _lexical_score(a: str, b: str) -> float:
    set_a = set(a.split())
    set_b = set(b.split())
    if not set_a and not set_b:
        return 0.0
    overlap = set_a & set_b
    union = set_a | set_b
    return round(len(overlap) / max(1, len(union)), 3)


def retrieve_examples_for_question(
    question: str,
    *,
    namespace: str = _DEFAULT_NAMESPACE,
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    qn = _normalize_q(question)
    if not qn:
        return []

    if _STORE.engine:
        rows = _STORE.find_similar(namespace, question, top_k=top_k)
        if rows:
            return [
                {
                    "q": row.get("q_norm") or row.get("q_raw"),
                    "sql": row.get("sql"),
                    "score": None,
                    "stars": row.get("success_count", 0),
                }
                for row in rows
            ]

    results: List[Dict[str, Any]] = []
    for rec in _FALLBACK_EXAMPLES.values():
        score = _lexical_score(qn, rec.get("q_norm", ""))
        results.append(
            {
                "q": rec.get("q_norm"),
                "sql": rec.get("sql"),
                "score": score,
                "stars": rec.get("stars", 0),
            }
        )
    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:top_k]


__all__ = ["save_example_if_positive", "retrieve_examples_for_question"]
