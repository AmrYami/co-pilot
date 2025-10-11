"""Registry of available FTS engines for DW query building."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from apps.dw.fts_like import build_fts_where


class LikeEngine:
    """Simple LIKE-based FTS engine."""

    name = "like"

    def build(
        self,
        groups: Sequence[Sequence[str]],
        columns: Sequence[str],
        operator: str,
        bind_prefix: str = "fts",
    ) -> Tuple[str, Dict[str, Any]]:
        token_groups: List[List[str]] = []
        for group in groups:
            cleaned = [
                (token or "").strip()
                for token in group
                if isinstance(token, str) and token.strip()
            ]
            if cleaned:
                token_groups.append(cleaned)
        sql, binds = build_fts_where(token_groups, columns, operator, bind_prefix)
        return sql, binds


REGISTRY: Dict[str, Any] = {"like": LikeEngine()}


def register_engine(name: str, engine: Any) -> None:
    if not name or engine is None:
        return
    REGISTRY[str(name).lower()] = engine


def resolve_engine(name: str | None):
    key = (name or "").strip().lower()
    if key in {"", "like"}:
        engine = REGISTRY.get("like")
    else:
        engine = REGISTRY.get(key)
    if not engine:
        engine = REGISTRY.get("like")
        if not engine:
            raise RuntimeError(f"FTS engine not registered: {name!r}")
    return engine


__all__ = ["resolve_engine", "register_engine"]
