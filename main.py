# main.py
from __future__ import annotations
from flask import Flask
from core.settings import Settings
from core.pipeline import Pipeline
from apps.fa.app import fa_bp
from core.admin_api import admin_bp

def create_app() -> Flask:
    app = Flask(__name__)

    settings = Settings()  # namespace set later per-request by FA routes
    pipeline = Pipeline(settings=settings, namespace="fa::common")

    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    app.config["SETTINGS"]  = pipeline.settings

    app.register_blueprint(admin_bp)
    app.register_blueprint(fa_bp, url_prefix="/fa")

    @app.get("/health")
    def health():
        return {"ok": True, "namespace": "fa::common"}

    return app

# DO NOT instantiate the app here.
# Run with: FLASK_APP=main:create_app flask run
