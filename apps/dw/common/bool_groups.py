from __future__ import annotations

# English-only comments.
from dataclasses import dataclass
from typing import Dict, List, Literal, Tuple
import re

Op = Literal["eq", "like"]


@dataclass
class Term:
    kind: Literal["fts", "field"]
    column: str | None
    values: List[str]
    op: Op = "eq"


@dataclass
class Group:
    fts_tokens: List[str]
    field_terms: List[Tuple[str, List[str], Op]]


_COL_TOKENS = [
    "ENTITY",
    "REPRESENTATIVE_EMAIL",
    "STAKEHOLDER",
    "STAKEHOLDERS",
    "DEPARTMENT",
    "DEPARTMENTS",
]
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _split_or_list(text: str) -> List[str]:
    tokens: List[str] = []
    for part in re.split(r"\s+or\s+|,", text, flags=re.IGNORECASE):
        cleaned = part.strip()
        if cleaned:
            tokens.append(cleaned)
    return tokens


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def parse_question_into_terms(question: str) -> List[Term]:
    """Extract heuristic terms from the natural-language question."""

    text = " " + _normalize(question) + " "
    terms: List[Term] = []

    for col in _COL_TOKENS:
        pattern = rf"{col}\s+(?:has|have)\s+(.*?)(?=\s+(?:and|or)\s+|$)"
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            values = _split_or_list(match.group(1))
            if values:
                terms.append(Term(kind="field", column=col.upper(), values=values, op="like"))

    for col in _COL_TOKENS:
        pattern = rf"{col}\s*=\s*(.*?)(?=\s+(?:and|or)\s+|$)"
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            values = _split_or_list(match.group(1))
            if values:
                terms.append(Term(kind="field", column=col.upper(), values=values, op="eq"))

    for match in re.finditer(r"has\s+(.*?)(?=\s+(?:and|or)\s+|$)", text, flags=re.IGNORECASE):
        values = _split_or_list(match.group(1))
        if values:
            terms.append(Term(kind="fts", column=None, values=values, op="eq"))

    has_rep_email = any((t.column or "").upper() == "REPRESENTATIVE_EMAIL" for t in terms if t.kind == "field")
    if has_rep_email:
        emails = _EMAIL_RE.findall(text)
        if emails:
            explicit = {
                value.lower()
                for t in terms
                if t.kind == "field" and (t.column or "").upper() == "REPRESENTATIVE_EMAIL"
                for value in t.values
            }
            extra = [email for email in emails if email.lower() not in explicit]
            if extra:
                terms.append(Term(kind="field", column="REPRESENTATIVE_EMAIL", values=extra, op="eq"))

    def _position(term: Term) -> int:
        indexes: List[int] = []
        for value in term.values:
            match = re.search(re.escape(value), text, flags=re.IGNORECASE)
            if match:
                indexes.append(match.start())
        return min(indexes) if indexes else len(text)

    terms.sort(key=_position)
    return terms


def group_by_boolean_ops(question: str, terms: List[Term]) -> List[Group]:
    """Group terms into AND/OR buckets based on connectors in the question."""

    parts = re.split(r"(and|or)", _normalize(question), flags=re.IGNORECASE)
    segments: List[Tuple[str, str]] = []
    for index in range(0, len(parts), 2):
        segment = parts[index].strip()
        op = parts[index + 1].strip().lower() if index + 1 < len(parts) else ""
        if segment:
            segments.append((segment, op))

    iterator = iter(terms)
    current: List[Term] = []
    groups: List[Group] = []

    def _flush() -> None:
        nonlocal current
        if not current:
            return
        fts_tokens: List[str] = []
        field_map: Dict[Tuple[str, Op], List[str]] = {}
        for term in current:
            if term.kind == "fts":
                fts_tokens.extend(term.values)
            else:
                key = ((term.column or "").upper(), term.op)
                field_map.setdefault(key, []).extend(term.values)
        field_terms = [
            (column, list(dict.fromkeys(values)), op)
            for (column, op), values in field_map.items()
            if column
        ]
        groups.append(Group(fts_tokens=fts_tokens, field_terms=field_terms))
        current = []

    for segment, op in segments:
        try:
            term = next(iterator)
        except StopIteration:
            continue
        current.append(term)
        if op == "and":
            continue
        if op == "or":
            try:
                idx = terms.index(term)
                next_term = terms[idx + 1]
            except (ValueError, IndexError):
                next_term = None
            if (
                next_term
                and term.kind == next_term.kind
                and (term.column or "").upper() == (next_term.column or "").upper()
            ):
                continue
            _flush()
    _flush()
    return groups


def infer_boolean_groups(question: str) -> List[Group]:
    terms = parse_question_into_terms(question)
    if not terms:
        return []
    return group_by_boolean_ops(question, terms)
