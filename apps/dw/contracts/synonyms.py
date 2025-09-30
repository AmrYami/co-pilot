from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

DEFAULT_REQUEST_TYPE_SYNONYMS: Dict[str, List[str]] = {
    "RENEWAL": ["renew", "renewal", "renew contract", "renewed", "extension"],
    "NEW CONTRACT": ["new", "new contract"],
    "ADDENDUM": ["addendum", "amendment", "appendix"],
}


def _normalize_list(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for v in values:
        if v is None:
            continue
        text = str(v).strip()
        if text:
            out.append(text)
    return out


def normalize_key(val: Optional[str]) -> str:
    return (val or "").strip().upper()


def _load_settings_synonyms(getter: Callable[..., Any]) -> Optional[Dict[str, List[str]]]:
    for kwargs in (
        {"default": None, "scope": "namespace"},
        {"default": None},
        {},
    ):
        try:
            cfg = getter("DW_REQUEST_TYPE_SYNONYMS", **kwargs)
        except TypeError:
            continue
        if isinstance(cfg, dict) and cfg:
            fixed: Dict[str, List[str]] = {}
            for key, arr in cfg.items():
                if isinstance(arr, (list, tuple)):
                    fixed[normalize_key(str(key))] = _normalize_list(arr)
            if fixed:
                return fixed
        break
    return None


def get_request_type_synonyms(
    settings_get_json: Optional[Callable[..., Any]],
) -> Dict[str, List[str]]:
    if callable(settings_get_json):
        loaded = _load_settings_synonyms(settings_get_json)
        if loaded:
            return loaded
    return DEFAULT_REQUEST_TYPE_SYNONYMS


def _prepare_synonyms_map(
    synonyms_map: Dict[str, Iterable[Any]]
) -> Dict[str, List[str]]:
    prepared: Dict[str, List[str]] = {}
    for canon, items in synonyms_map.items():
        prepared[normalize_key(canon)] = _normalize_list(items)
    return prepared


def _match_candidates(
    value_up: str, synonyms_map: Dict[str, List[str]]
) -> Tuple[List[str], Optional[str]]:
    if value_up in synonyms_map:
        return list(synonyms_map[value_up]), value_up
    for canon, syns in synonyms_map.items():
        for term in syns:
            term_up = normalize_key(term)
            if term_up and value_up == term_up:
                return list(syns), canon
    return [], None


def build_request_type_filter_sql(
    value: str,
    synonyms_map: Dict[str, Iterable[Any]],
    *,
    use_like: bool = True,
    bind_prefix: str = "rt",
) -> Tuple[str, Dict[str, Any]]:
    v = (value or "").strip()
    v_up = v.upper()
    normalized_map = _prepare_synonyms_map(synonyms_map)

    candidates, _ = _match_candidates(v_up, normalized_map)

    binds: Dict[str, Any] = {}
    parts: List[str] = []

    def _append_term(term: str, idx: int) -> None:
        key = f"{bind_prefix}_{idx}"
        if use_like:
            binds[key] = f"%{term}%"
            parts.append(f"UPPER(TRIM(REQUEST_TYPE)) LIKE UPPER(:{key})")
        else:
            binds[key] = term.upper()
            parts.append(f"UPPER(TRIM(REQUEST_TYPE)) = :{key}")

    if candidates:
        idx = 0
        for term in candidates:
            if term is None:
                continue
            text = str(term).strip()
            if not text:
                continue
            _append_term(text, idx)
            idx += 1
        if not parts:
            parts.append("(REQUEST_TYPE IS NULL OR TRIM(REQUEST_TYPE)='')")
        fragment = "(" + " OR ".join(parts) + ")"
        return fragment, binds

    _append_term(v, 0)
    fragment = "(" + " OR ".join(parts) + ")" if len(parts) > 1 else parts[0]
    return fragment, binds
