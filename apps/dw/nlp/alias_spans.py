from __future__ import annotations

from typing import List, Sequence, Tuple

_ALIAS_CACHE: dict = {"keys": None, "nlp": None, "matcher": None}


def _ensure_matcher(alias_keys: Sequence[str]):
    keys_tuple = tuple(sorted({str(k).strip().lower() for k in alias_keys if str(k).strip()}))
    cache_keys = _ALIAS_CACHE.get("keys")
    if cache_keys == keys_tuple and _ALIAS_CACHE.get("nlp") is not None:
        return _ALIAS_CACHE["nlp"], _ALIAS_CACHE["matcher"]
    try:
        import spacy  # type: ignore
        from spacy.matcher import PhraseMatcher  # type: ignore
    except ImportError:
        _ALIAS_CACHE["keys"] = None
        _ALIAS_CACHE["nlp"] = None
        _ALIAS_CACHE["matcher"] = None
        return None, None

    try:
        nlp = spacy.blank("en")
    except Exception:
        _ALIAS_CACHE["keys"] = None
        _ALIAS_CACHE["nlp"] = None
        _ALIAS_CACHE["matcher"] = None
        return None, None

    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    patterns = [nlp.make_doc(alias) for alias in keys_tuple if alias]
    if patterns:
        matcher.add("ALIAS", patterns)
    _ALIAS_CACHE["keys"] = keys_tuple
    _ALIAS_CACHE["nlp"] = nlp
    _ALIAS_CACHE["matcher"] = matcher
    return nlp, matcher


def detect_alias_spans(text: str, alias_keys: Sequence[str]) -> List[Tuple[int, int, str]]:
    """
    Return a list of spans (start_char, end_char, text) for aliases detected in the text.
    """
    if not text or not alias_keys:
        return []
    nlp, matcher = _ensure_matcher(alias_keys)
    if nlp is None or matcher is None:
        return []
    doc = nlp.make_doc(text)
    matches = matcher(doc)
    spans: List[Tuple[int, int, str]] = []
    for _, start, end in matches:
        span = doc[start:end]
        spans.append((span.start_char, span.end_char, span.text))
    spans.sort(key=lambda item: item[0])
    return spans
