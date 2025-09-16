"""Flask application entry point for DocuWare endpoints."""

from flask import Flask

from core.pipeline import Pipeline
from core.settings import Settings


def create_app():
    app = Flask(__name__)
    settings = Settings(namespace="dw::common")

    # Preload pipeline to surface configuration issues early.
    try:
        pipeline = Pipeline(settings=settings, namespace="dw::common")
        app.config["PIPELINE"] = pipeline
    except Exception as exc:  # pragma: no cover - startup diagnostics only
        print("[startup] Failed to initialise pipeline:", exc)

    # Register DocuWare blueprint
    try:
        from apps.dw import dw_bp

        app.register_blueprint(dw_bp)
    except Exception as exc:  # pragma: no cover - startup diagnostics only
        print("[startup] Failed to register DocuWare blueprint:", exc)

    @app.get("/healthz")
    def health():
        return {"ok": True}

    return app
