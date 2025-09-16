from __future__ import annotations

from typing import Any

from core.agents import PlannerAgent, ValidatorAgent


def get_planner(llm_handle: Any, settings: Any | None = None) -> PlannerAgent:
    """Return a default planner that delegates to the core implementation."""
    return PlannerAgent(llm_handle)


def get_validator(engine, settings: Any | None = None) -> ValidatorAgent:
    """Return the stock validator."""
    return ValidatorAgent(engine, settings)


def normalize_admin_reply(text: str) -> dict:
    """Fallback normaliser when no app-specific implementation is available."""
    return {}
