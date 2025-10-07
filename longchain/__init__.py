"""Minimal Flask application exposing the lightweight DW blueprint."""

from __future__ import annotations

try:  # pragma: no cover - allow environments without Flask
    from flask import Flask
except Exception:  # pragma: no cover
    Flask = None  # type: ignore[assignment]
else:
    from longchain.apps.dw.app import dw_bp


def create_app():  # pragma: no cover - exercised via tests
    if Flask is None:
        raise RuntimeError("Flask is required to create the longchain app")
    app = Flask(__name__, template_folder="templates")
    app.register_blueprint(dw_bp, url_prefix="/dw")
    return app


app = create_app() if "Flask" in globals() and Flask is not None else None

__all__ = ["app", "create_app"]
