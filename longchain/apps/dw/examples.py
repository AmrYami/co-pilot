from __future__ import annotations

from typing import Any, Dict, List
import re


def _normalize_q(q: str) -> str:
    q = (q or "").strip().lower()
    q = re.sub(r"\s+", " ", q)
    return q


# In-memory examples store used for tests. Keys are normalized questions.
_EXAMPLES: Dict[str, Dict[str, Any]] = {}


def save_example_if_positive(inquiry_id: int, question: str, sql: str, rating: int) -> None:
    """Persist highly-rated examples for lightweight suggestion retrieval."""
    try:
        if rating is None or rating < 4:
            return
        qn = _normalize_q(question)
        if not qn:
            return
        existing = _EXAMPLES.get(qn)
        if not existing or rating > existing.get("stars", 0):
            _EXAMPLES[qn] = {
                "inquiry_id": inquiry_id,
                "q_norm": qn,
                "sql": sql,
                "stars": int(rating),
            }
    except Exception:  # pragma: no cover - defensive, never raise during rating flow
        pass


def _lexical_score(a: str, b: str) -> float:
    set_a = set(a.split())
    set_b = set(b.split())
    if not set_a and not set_b:
        return 0.0
    overlap = set_a & set_b
    union = set_a | set_b
    return round(len(overlap) / max(1, len(union)), 3)


def retrieve_examples_for_question(question: str) -> List[Dict[str, Any]]:
    qn = _normalize_q(question)
    results: List[Dict[str, Any]] = []
    if not qn:
        return results
    for rec in _EXAMPLES.values():
        score = _lexical_score(qn, rec.get("q_norm", ""))
        results.append({
            "q": rec.get("q_norm"),
            "sql": rec.get("sql"),
            "score": score,
            "stars": rec.get("stars", 0),
        })
    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:5]
