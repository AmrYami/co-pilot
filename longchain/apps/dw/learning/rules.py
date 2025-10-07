"""Simple rules engine for DW planning tweaks."""
from __future__ import annotations

import random
from typing import Any, Dict, List

try:  # pragma: no cover - optional dependency for tests
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover
    create_engine = None  # type: ignore[assignment]

    def text(sql: str):  # type: ignore
        return sql


class RulesEngine:
    """Minimal rules store backed by the MEM DB."""

    def __init__(self, url: str | None) -> None:
        self.url = url
        self.engine = create_engine(url) if (url and create_engine) else None
        self._ensure_schema()

    # ------------------------------------------------------------------
    def _ensure_schema(self) -> None:
        if not self.engine:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS dw_rules (
                          id SERIAL PRIMARY KEY,
                          namespace TEXT,
                          name TEXT,
                          pattern JSONB,
                          payload JSONB,
                          status TEXT DEFAULT 'disabled',
                          rollout_pct INT DEFAULT 10,
                          version INT DEFAULT 1,
                          created_at TIMESTAMPTZ DEFAULT now(),
                          updated_at TIMESTAMPTZ DEFAULT now()
                        )
                        """
                    )
                )
        except Exception:
            pass

    # ------------------------------------------------------------------
    def find_applicable(self, namespace: str, intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not self.engine:
            return []
        try:
            with self.engine.begin() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                            SELECT * FROM dw_rules
                            WHERE namespace = :ns AND status IN ('active','canary','shadow')
                            ORDER BY updated_at DESC
                            """
                        ),
                        {"ns": namespace},
                    )
                    .mappings()
                    .all()
                )
        except Exception:
            return []
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    def select_to_apply(self, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        for rule in rules:
            status = (rule.get("status") or "").lower()
            if status == "active":
                selected.append(rule)
            elif status == "canary":
                pct = int(rule.get("rollout_pct") or 10)
                pct = max(0, min(100, pct))
                if pct and random.randint(1, 100) <= pct:
                    selected.append(rule)
            elif status == "shadow":
                # Shadows are not applied, but callers may log/inspect them.
                continue
        return selected


__all__ = ["RulesEngine"]
