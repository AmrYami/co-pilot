from flask import Flask, jsonify

from apps.common.admin import admin_bp as admin_common_bp
from apps.dw.app import create_dw_blueprint
from apps.dw.tests.routes import tests_bp
from core.admin_api import admin_bp as core_admin_bp
from core.logging_utils import get_logger, log_event, setup_logging
from core.model_loader import ensure_model, model_info
from core.pipeline import Pipeline
from core.settings import Settings


def create_app():
    settings = Settings()
    setup_logging(settings)
    log = get_logger("main")

    app = Flask(__name__)
    try:
        app.logger.handlers.clear()
        app.logger.propagate = True
    except Exception:  # pragma: no cover - defensive
        pass

    log_event(log, "boot", "app_boot", {"message": "registering blueprints"})

    # Warm up SQL model (already works)
    ensure_model(role="sql")

    # NEW: warm up clarifier if it isn't explicitly disabled
    # The loader will read CLARIFIER_* from the environment.
    try:
        ensure_model(role="clarifier")  # safe no-op if unavailable / disabled
    except Exception as e:  # pragma: no cover - best effort log
        log.warning("[clarifier] load failed: %s", e)

    pipeline = Pipeline(settings=settings, namespace="dw::common")

    app.config["SETTINGS"] = settings
    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    app.config["pipeline"] = pipeline  # backwards compatibility

    dw_bp = create_dw_blueprint(settings=settings, pipeline=pipeline)

    app.register_blueprint(dw_bp, url_prefix="/dw")
    app.register_blueprint(tests_bp)
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
