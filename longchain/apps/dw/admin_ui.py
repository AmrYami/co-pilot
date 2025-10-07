"""Minimal admin UI for DW feedback, examples, and rules."""
from __future__ import annotations

import os
from typing import Any, List

try:  # pragma: no cover - optional import for tests
    from flask import Blueprint, render_template
except Exception:  # pragma: no cover - provide lightweight stubs
    Blueprint = None  # type: ignore[assignment]

    def render_template(template_name: str, **context: Any):  # type: ignore
        return {"template": template_name, "context": context}

try:  # pragma: no cover - optional dependency
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover
    create_engine = None  # type: ignore[assignment]

    def text(sql: str):  # type: ignore
        return sql


def _engine():
    url = os.getenv("MEMORY_DB_URL")
    if not (url and create_engine):
        return None
    try:
        return create_engine(url)
    except Exception:  # pragma: no cover - defensive
        return None


def _fetch_rows(query: str) -> List[Any]:
    eng = _engine()
    if not eng:
        return []
    try:
        with eng.begin() as conn:
            return conn.execute(text(query)).mappings().all()
    except Exception:
        return []


dw_admin_ui = None
if Blueprint:  # pragma: no cover - skip when Flask is unavailable
    dw_admin_ui = Blueprint("dw_admin_ui", __name__, template_folder="../../templates")

    @dw_admin_ui.route("/admin/ui/feedback")
    def ui_feedback():  # pragma: no cover - exercised via integration tests
        rows = _fetch_rows(
            """
            SELECT id, inquiry_id, rating, comment, question, sql, created_at
            FROM dw_feedback ORDER BY created_at DESC LIMIT 200
            """
        )
        return render_template("dw/admin/feedback.html", rows=rows)

    @dw_admin_ui.route("/admin/ui/examples")
    def ui_examples():  # pragma: no cover - exercised via integration tests
        rows = _fetch_rows(
            """
            SELECT id, namespace, q_raw, sql, success_count, created_at
            FROM dw_examples ORDER BY created_at DESC LIMIT 200
            """
        )
        return render_template("dw/admin/examples.html", rows=rows)

    @dw_admin_ui.route("/admin/ui/rules")
    def ui_rules():  # pragma: no cover - exercised via integration tests
        rows = _fetch_rows(
            """
            SELECT id, name, status, rollout_pct, version, pattern, payload, updated_at
            FROM dw_rules ORDER BY updated_at DESC LIMIT 200
            """
        )
        return render_template("dw/admin/rules.html", rows=rows)


__all__ = ["dw_admin_ui"]
