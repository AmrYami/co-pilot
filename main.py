from __future__ import annotations

from flask import Flask

from core.admin_api import admin_bp
from core.pipeline import Pipeline
from core.settings import Settings
from core.sql_exec import init_mem_engine


def create_app() -> Flask:
    app = Flask(__name__)

    settings = Settings()
    active_app = (
        settings.get("ACTIVE_APP", "dw", scope="namespace") or "dw"
    ).strip() or "dw"
    active_ns = (
        settings.get("ACTIVE_NAMESPACE", f"{active_app}::common", scope="namespace")
        or f"{active_app}::common"
    )

    pipeline = Pipeline(settings=settings, namespace=active_ns)
    init_mem_engine(settings)

    app.extensions = getattr(app, "extensions", {})
    app.extensions["pipeline"] = pipeline
    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    if app.config["MEM_ENGINE"] is None:
        raise RuntimeError("pipeline.mem_engine is None – check MEMORY_DB_URL setting")
    app.config["SETTINGS"] = pipeline.settings

    app.register_blueprint(admin_bp)

    if active_app == "dw":
        from apps.dw.app import bp as dw_bp

        app.register_blueprint(dw_bp, url_prefix="/dw")
    else:
        raise RuntimeError(f"Unsupported ACTIVE_APP '{active_app}'")

    @app.get("/__routes")
    def _routes():
        return {"routes": sorted([str(r.rule) for r in app.url_map.iter_rules()])}

    @app.get("/health")
    def health():
        return {"ok": True, "namespace": active_ns, "app": active_app}

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "namespace": active_ns, "app": active_app}

    @app.get("/model/info")
    def model_info():
        llm = app.config["PIPELINE"].llm
        meta = getattr(llm, "meta", {}) or {}
        return {"ok": True, "backend": llm.backend, "meta": meta}

    return app


# DO NOT instantiate the app here.
# Run with: FLASK_APP=main:create_app flask run
