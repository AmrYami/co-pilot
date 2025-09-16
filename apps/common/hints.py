from __future__ import annotations

"""Fallback hint helpers when no app-specific module is available."""

MISSING_FIELD_QUESTIONS: dict[str, str] = {}
DOMAIN_HINTS: dict[str, dict] = {}


def make_fa_hints(*_args, **_kwargs) -> dict:
    """Return an empty hint payload."""
    return {}


def parse_admin_answer(_text: str) -> dict:
    """Default parser for legacy admin answers."""
    return {}


def parse_admin_reply_to_hints(_text: str) -> dict:
    """Default parser for admin replies when no specific mapping exists."""
    return {}


def seed_namespace(_settings, _namespace: str) -> dict:
    """No-op namespace seeding."""
    return {"join_graph": 0, "metrics": 0}
