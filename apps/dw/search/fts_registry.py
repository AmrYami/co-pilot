"""Lightweight registry for DW FTS engines."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Tuple


FTSBuilder = Callable[[Sequence[str], Sequence[Sequence[str]], str], Tuple[str, Dict[str, Any]]]


class _EngineAdapter:
    """Adapter that exposes a ``build`` method expected by legacy callers."""

    def __init__(self, name: str, builder: Callable[..., Tuple[str, Dict[str, Any]]]) -> None:
        self.name = name
        self._builder = builder

    def build(
        self,
        groups: Sequence[Sequence[str]],
        columns: Sequence[str],
        operator: str,
    ) -> Tuple[str, Dict[str, Any]]:
        return self._builder(columns, groups, operator=operator)


def _call_builder(
    fn: Callable[..., Tuple[str, Dict[str, Any]]],
    columns: Sequence[str],
    groups: Sequence[Sequence[str]],
    *,
    operator: str,
) -> Tuple[str, Dict[str, Any]]:
    try:
        return fn(columns, groups, operator=operator)
    except TypeError:
        # Fallback to builders that accept only (columns, groups).
        return fn(columns, groups)


def _normalize_groups(groups: Iterable[Iterable[Any]]) -> Tuple[Tuple[str, ...], ...]:
    normalized: list[tuple[str, ...]] = []
    for group in groups or []:
        cleaned = []
        for token in group or []:
            text = str(token or "").strip()
            if text:
                cleaned.append(text)
        if cleaned:
            normalized.append(tuple(cleaned))
    return tuple(normalized)


_REGISTRY: Dict[str, Callable[[Sequence[str], Sequence[Sequence[str]], str], Tuple[str, Dict[str, Any]]]] = {}


def _ensure_default_engines() -> None:
    if "like" in _REGISTRY:
        return
    try:  # pragma: no cover - defensive import for optional dependencies
        import apps.dw.fts  # noqa: F401
    except Exception:
        pass


def register_engine(name: str, engine: Optional[Any] = None):
    """Register an FTS engine.

    ``engine`` can either be a callable accepting ``(columns, groups, operator=...)``
    or an object exposing a ``build`` method compatible with ``QueryBuilder``.
    When used as a decorator ``engine`` should be omitted.
    """

    if not name:
        raise ValueError("Engine name must be provided")

    key = str(name).strip().lower()

    def _store(fn: Any) -> Any:
        if fn is None:
            raise ValueError(f"Cannot register empty engine for {name!r}")

        if callable(fn):
            def _builder(columns: Sequence[str], groups: Sequence[Sequence[str]], *, operator: str = "OR"):
                normalized_groups = _normalize_groups(groups)
                return _call_builder(fn, columns, normalized_groups, operator=operator)

            _REGISTRY[key] = _builder
            return fn

        build = getattr(fn, "build", None)
        if callable(build):
            def _builder(columns: Sequence[str], groups: Sequence[Sequence[str]], *, operator: str = "OR"):
                normalized_groups = _normalize_groups(groups)
                return build(normalized_groups, columns, operator)

            _REGISTRY[key] = _builder
            return fn

        raise TypeError(f"Unsupported engine type for {name!r}: {type(fn)!r}")

    if engine is not None:
        return _store(engine)

    def _decorator(fn: Callable[..., Tuple[str, Dict[str, Any]]]):
        return _store(fn)

    return _decorator


def get_engine(name: Optional[str]) -> Optional[Callable[[Sequence[str], Sequence[Sequence[str]], str], Tuple[str, Dict[str, Any]]]]:
    _ensure_default_engines()
    if not name:
        return None
    return _REGISTRY.get(str(name).strip().lower())


def resolve_engine(name: Optional[str]):
    _ensure_default_engines()
    key = (name or "").strip().lower()
    builder = _REGISTRY.get(key) if key else None
    if not builder:
        builder = _REGISTRY.get("like")
        key = key or "like"
    if not builder:
        raise RuntimeError(f"FTS engine not registered: {name!r}")
    return _EngineAdapter(key, builder)


__all__ = ["get_engine", "register_engine", "resolve_engine"]
