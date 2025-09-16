"""Flask application entry point for DocuWare endpoints."""

from flask import Flask

from core.pipeline import Pipeline
from core.settings import Settings
from apps.dw.app import dw_bp


def create_app():
    app = Flask(__name__)

    settings = Settings(namespace="dw::common")
    pipeline = Pipeline(settings=settings, namespace="dw::common")

    app.register_blueprint(dw_bp)

    app.pipeline = pipeline
    return app
