# main.py
from __future__ import annotations
from flask import Flask
from core.settings import Settings
from core.pipeline import Pipeline
from apps.fa.app import fa_bp
from apps.docuware import docuware_bp
from core.admin_api import admin_bp
from core.sql_exec import init_mem_engine

def create_app() -> Flask:
    app = Flask(__name__)

    settings = Settings()
    pipeline = Pipeline(settings=settings, namespace="fa::common")
    init_mem_engine(settings)

    app.extensions = getattr(app, "extensions", {})
    app.extensions["pipeline"] = pipeline
    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    if app.config["MEM_ENGINE"] is None:
        raise RuntimeError("pipeline.mem_engine is None – check MEMORY_DB_URL setting")
    app.config["SETTINGS"] = pipeline.settings

    # admin_bp already has url_prefix="/admin"
    app.register_blueprint(admin_bp)

    app.register_blueprint(fa_bp, url_prefix="/fa")
    app.register_blueprint(docuware_bp)

    @app.get("/__routes")
    def _routes():
        return {
            "routes": sorted([str(r.rule) for r in app.url_map.iter_rules()])
        }

    @app.get("/health")
    def health():
        return {"ok": True, "namespace": "fa::common"}

    @app.get("/model/info")
    def model_info():
        llm = app.config["PIPELINE"].llm
        meta = getattr(llm, "meta", {}) or {}
        return {
            "ok": True,
            "backend": llm.backend,
            "meta": meta,
        }

    return app

# DO NOT instantiate the app here.
# Run with: FLASK_APP=main:create_app flask run
