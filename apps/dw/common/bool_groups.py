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
    start: int = -1
    end: int = -1


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
_COL_PATTERN = "|".join(re.escape(col) for col in _COL_TOKENS)
_JOIN_PATTERN = _COL_PATTERN + "|has|have|contains"
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_OR_SPLIT_RE = re.compile(r"\s+or\s+|\s*\|\s*|,", re.IGNORECASE)
_COMPARATOR_STARTERS = [
    ">=",
    "<=",
    ">",
    "<",
    "=",
    "!=",
    "==",
    "greater than",
    "greater than or equal",
    "less than",
    "less than or equal",
    "more than",
    "at least",
    "at most",
    "between",
    "not in",
    "in",
    "like",
]
_COMPARISON_TAIL_RE = re.compile(
    r"(?i)^(?:"
    r"[A-Z0-9_ ]+\s*(?:>=|<=|=|>|<|between|like|in|not in|greater than|less than|at least|at most)"
    r"|"
    r"(?:>=|<=|=|>|<|between|like|in|not in|greater than|less than|at least|at most)"
    r")"
)


def _split_or_list(text: str) -> List[str]:
    tokens: List[str] = []
    for part in _OR_SPLIT_RE.split(text or ""):
        cleaned = part.strip(" \t\n\r\f\v\"'“”’`“”")
        if cleaned:
            tokens.append(re.sub(r"\s+", " ", cleaned))
    return tokens


def _strip_comparison_tail(value: str) -> str:
    """Trim trailing comparator clauses from an inline value (e.g., AND VAT > 200)."""
    if not value:
        return value
    lower = value.lower()
    idx = lower.find(" and ")
    while idx != -1:
        tail = lower[idx + 5 :].strip()
        if not tail:
            break
        if any(tail.startswith(marker) for marker in _COMPARATOR_STARTERS):
            return value[:idx].strip()
        if _COMPARISON_TAIL_RE.match(tail):
            return value[:idx].strip()
        idx = lower.find(" and ", idx + 5)
    return value.strip()


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


_FIELD_PATTERN = re.compile(
    rf"\b(?P<col>{_COL_PATTERN})\b\s*(?P<op>=|has|have|contains)\s*"
    rf"(?P<body>.*?)(?=(?:\s+(?:and|or)\s+(?:{_JOIN_PATTERN}))|;|$)",
    re.IGNORECASE,
)


def parse_question_into_terms(question: str) -> List[Term]:
    """Extract heuristic terms from the natural-language question."""

    text = _normalize(question)
    if not text:
        return []

    terms: List[Term] = []

    for match in _FIELD_PATTERN.finditer(text):
        column = match.group("col").upper().replace(" ", "_")
        op_token = match.group("op").lower()
        body = match.group("body") or ""
        values = _split_or_list(body)
        if not values:
            continue
        values = [v for v in (_strip_comparison_tail(val) for val in values) if v]
        op: Op = "like" if op_token in {"has", "have", "contains"} else "eq"
        terms.append(
            Term(
                kind="field",
                column=column,
                values=values,
                op=op,
                start=match.start(),
                end=match.end(),
            )
        )

    # Generic "has <fts tokens>" without a specific column
    generic_pattern = re.compile(
        rf"\bhas\s+(?P<body>.*?)(?=(?:\s+(?:and|or)\s+(?:{_JOIN_PATTERN}))|;|$)",
        re.IGNORECASE,
    )
    occupied: List[Tuple[int, int]] = [(t.start, t.end) for t in terms]
    for match in generic_pattern.finditer(text):
        span = (match.start(), match.end())
        if any(span[0] >= s and span[1] <= e for s, e in occupied):
            continue
        # Avoid capturing cases like "ENTITY has" that are already field terms
        prefix = text[: match.start()].rstrip()
        if re.search(rf"\b({_COL_PATTERN})\s*$", prefix, re.IGNORECASE):
            continue
        values = _split_or_list(match.group("body") or "")
        if not values:
            continue
        values = [v for v in (_strip_comparison_tail(val) for val in values) if v]
        terms.append(
            Term(
                kind="fts",
                column=None,
                values=values,
                op="eq",
                start=match.start(),
                end=match.end(),
            )
        )

    # If representative email is present, augment with any email addresses in the text
    has_rep_email = any(
        term.kind == "field" and (term.column or "").upper() == "REPRESENTATIVE_EMAIL"
        for term in terms
    )
    if has_rep_email:
        emails = _EMAIL_RE.findall(text)
        if emails:
            explicit = {
                value.lower()
                for term in terms
                if term.kind == "field" and (term.column or "").upper() == "REPRESENTATIVE_EMAIL"
                for value in term.values
            }
            extra = [email for email in emails if email.lower() not in explicit]
            if extra:
                last_span = max((term.end for term in terms if term.end >= 0), default=len(text))
                terms.append(
                    Term(
                        kind="field",
                        column="REPRESENTATIVE_EMAIL",
                        values=extra,
                        op="eq",
                        start=last_span,
                        end=last_span,
                    )
                )

    terms.sort(key=lambda term: term.start if term.start >= 0 else len(text))
    return terms


def group_by_boolean_ops(question: str, terms: List[Term]) -> List[Group]:
    """Group terms into AND/OR buckets based on connectors in the question."""

    if not terms:
        return []

    text = _normalize(question)
    if not text:
        return []

    ordered = sorted(terms, key=lambda term: term.start if term.start >= 0 else len(text))
    groups: List[Group] = []
    current: List[Term] = []

    def flush() -> None:
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
        groups.append(Group(fts_tokens=list(dict.fromkeys(fts_tokens)), field_terms=field_terms))
        current = []

    for index, term in enumerate(ordered):
        current.append(term)
        next_term = ordered[index + 1] if index + 1 < len(ordered) else None
        if not next_term:
            flush()
            break
        span_text = text[term.end : next_term.start]
        if re.search(r"\band\b", span_text, re.IGNORECASE):
            continue
        if re.search(r"\bor\b", span_text, re.IGNORECASE):
            flush()

    return groups


def infer_boolean_groups(question: str) -> List[Group]:
    terms = parse_question_into_terms(question)
    return group_by_boolean_ops(question, terms)
