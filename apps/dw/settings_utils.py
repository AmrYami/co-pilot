from __future__ import annotations

from typing import Callable, Iterable, List, Optional, Sequence


def _normalize_columns(raw: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for col in raw:
        if not col:
            continue
        upper = str(col).strip().upper()
        if not upper or upper in seen:
            continue
        result.append(upper)
        seen.add(upper)
    return result


def load_explicit_filter_columns(
    getter: Optional[Callable[..., Sequence[str] | str | None]],
    namespace: str,
    default: Sequence[str],
) -> List[str]:
    """Return the explicit filter column list for ``namespace``.

    ``getter`` is typically ``settings.get_json``. We accept lists/tuples/sets or
    comma-separated strings. Fallback to ``default`` when nothing is configured.
    """
    effective = list(default)
    if not callable(getter):
        return _normalize_columns(effective)

    try:
        raw = getter("DW_EXPLICIT_FILTER_COLUMNS", scope="namespace", namespace=namespace)
    except TypeError:
        raw = getter("DW_EXPLICIT_FILTER_COLUMNS")

    if isinstance(raw, (list, tuple, set)):
        candidates = [str(col).strip() for col in raw if col is not None]
        if candidates:
            effective = candidates
    elif isinstance(raw, str):
        parts = [part.strip() for part in raw.split(",")]
        candidates = [part for part in parts if part]
        if candidates:
            effective = candidates

    return _normalize_columns(effective)
