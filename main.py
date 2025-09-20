import logging
import os
from logging.handlers import TimedRotatingFileHandler

from flask import Flask, jsonify

from apps.common.admin import admin_bp as admin_common_bp
from apps.dw.app import create_dw_blueprint
from core.admin_api import admin_bp as core_admin_bp
from core.logging_setup import init_logging
from core.model_loader import ensure_model, model_info
from core.pipeline import Pipeline
from core.settings import Settings


def create_app():
    app = Flask(__name__)

    # Initialize logging before other components emit logs
    init_logging(app)

    os.makedirs("logs", exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        "logs/dw.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if os.getenv("DW_DEBUG") == "1" else logging.INFO)
    console_handler.setFormatter(formatter)
    app.logger.addHandler(console_handler)

    # Warm up SQL model (already works)
    ensure_model(role="sql")

    # NEW: warm up clarifier if it isn't explicitly disabled
    # The loader will read CLARIFIER_* from the environment.
    try:
        ensure_model(role="clarifier")  # safe no-op if unavailable / disabled
    except Exception as e:  # pragma: no cover - best effort log
        app.logger.warning(f"[clarifier] load failed: {e}")

    settings = Settings()
    pipeline = Pipeline(settings=settings, namespace="dw::common")

    app.config["SETTINGS"] = settings
    app.config["pipeline"] = pipeline

    dw_bp = create_dw_blueprint(settings=settings, pipeline=pipeline)

    app.register_blueprint(dw_bp, url_prefix="/dw")
    app.register_blueprint(core_admin_bp, url_prefix="/admin")
    app.register_blueprint(admin_common_bp)

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/model/info")
    def model_info_endpoint():
        return jsonify(model_info())

    @app.get("/__routes")
    def list_routes():
        rows = []
        for rule in app.url_map.iter_rules():
            rows.append(
                {
                    "rule": str(rule),
                    "endpoint": rule.endpoint,
                    "methods": sorted(list(rule.methods - {"HEAD", "OPTIONS"})),
                }
            )
        return {"routes": rows}

    return app


app = create_app()
