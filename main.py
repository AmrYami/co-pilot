"""Flask application entry point for DocuWare endpoints."""

import os

from flask import Flask, jsonify

from core.pipeline import Pipeline
from core.settings import Settings


def create_app():
    app = Flask(__name__)

    namespace = os.getenv("ACTIVE_NAMESPACE", "dw::common")
    settings = Settings(namespace=namespace)
    pipeline = Pipeline(settings=settings, namespace=namespace)

    app.config["ACTIVE_NAMESPACE"] = namespace
    app.config["SETTINGS"] = settings
    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    app.pipeline = pipeline

    from apps.dw.app import dw_bp
    from core.admin_api import admin_bp

    app.register_blueprint(dw_bp)
    app.register_blueprint(admin_bp)

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True, "app": "dw", "namespace": namespace})

    return app
