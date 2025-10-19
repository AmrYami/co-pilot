import logging
import os

from flask import Flask, jsonify, request
from sqlalchemy import create_engine

from apps.common.admin import admin_bp as admin_common_bp
from apps.dw.app import create_dw_blueprint
from apps.dw.admin_api import bp as dw_admin_bp
from apps.dw.routes import debug_bp
from apps.dw.tests.routes import golden_bp
from core.admin_api import admin_bp as core_admin_bp
from core.corr import set_corr_id
from core.logging_setup import setup_logging
from core.logging_utils import get_logger, log_event
from core.memdb import ensure_dw_feedback_schema, get_mem_engine
from core.model_loader import ensure_model, model_info
from core.pipeline import Pipeline
from core.settings import Settings


def make_engine(url: str, echo_env: str):
    """Create a SQLAlchemy engine honouring environment echo toggles."""

    if not url:
        raise RuntimeError("Database URL must be provided")

    echo = str(os.getenv(echo_env, "false")).lower() in {"1", "true", "yes", "y"}
    return create_engine(url, pool_pre_ping=True, future=True, echo=echo)


def boot_app(app: Flask, settings: Settings, pipeline: Pipeline | None = None) -> None:
    """Initialise database engines and attach them to the Flask app."""

    app_db_url = app.config.get("APP_DB_URL") or settings.get_app_db_url(namespace="dw::common")
    if not app_db_url:
        raise RuntimeError("APP_DB_URL must be configured before booting the app")

    memory_url = (
        os.getenv("MEMORY_DB_URL")
        or app.config.get("MEMORY_DB_URL")
        or settings.get("MEMORY_DB_URL", scope="global")
    )
    if not memory_url:
        raise RuntimeError("MEMORY_DB_URL must be configured before booting the app")

    app_engine = make_engine(app_db_url, "APP_SQL_ECHO")

    mem_engine = None
    if pipeline is not None:
        existing = getattr(pipeline, "mem_engine", None)
        if existing is not None:
            existing_url = existing.url.render_as_string(hide_password=False)
            if existing_url == memory_url:
                mem_engine = existing

    if mem_engine is None:
        mem_engine = make_engine(memory_url, "MEM_SQL_ECHO")
        if pipeline is not None:
            setattr(pipeline, "mem_engine", mem_engine)

    app.app_engine = app_engine
    app.mem_engine = mem_engine

    app.config["APP_ENGINE"] = app_engine
    app.config["MEM_ENGINE"] = mem_engine

    app.logger.info(
        {
            "event": "boot.db_urls",
            "app_db": str(app_db_url).split("://")[0],
            "mem_db": str(memory_url).split("://")[0],
        }
    )

def create_app():
    debug_flag = str(os.getenv("COPILOT_DEBUG", "0")).strip().lower()
    debug = debug_flag in {"1", "true", "yes", "on"}
    setup_logging(debug=debug, preserve_handlers=True)
    settings = Settings()
    log = get_logger("main")

    app = Flask(__name__)

    logging.getLogger("dw").info(
        {
            "event": "boot.settings.snapshot",
            "env": {
                "COPILOT_DEBUG": os.getenv("COPILOT_DEBUG"),
                "DW_INCLUDE_DEBUG": os.getenv("DW_INCLUDE_DEBUG"),
                "SQL_TRACE": os.getenv("SQL_TRACE"),
                "ACTIVE_APP": os.getenv("ACTIVE_APP"),
                "FLASK_APP": os.getenv("FLASK_APP"),
            },
        }
    )
    try:
        app.logger.handlers.clear()
        app.logger.propagate = True
    except Exception:  # pragma: no cover - defensive
        pass

    @app.before_request
    def _inject_corr_id():
        try:
            payload = request.get_json(silent=True)
        except Exception:
            payload = None
        iid = payload.get("inquiry_id") if isinstance(payload, dict) else None
        set_corr_id(f"inq:{iid}" if iid else None)

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

    app.config["APP_DB_URL"] = settings.get_app_db_url(namespace="dw::common")
    app.config["MEMORY_DB_URL"] = (
        os.getenv("MEMORY_DB_URL") or settings.get("MEMORY_DB_URL", scope="global")
    )

    boot_app(app, settings, pipeline)

    app.config["pipeline"] = pipeline  # backwards compatibility

    dw_bp = create_dw_blueprint(settings=settings, pipeline=pipeline)

    app.register_blueprint(dw_bp, url_prefix="/dw")
    app.register_blueprint(debug_bp)
    app.register_blueprint(dw_admin_bp, url_prefix="/dw/admin")
    app.register_blueprint(golden_bp)
    app.register_blueprint(core_admin_bp, url_prefix="/admin")
    app.register_blueprint(admin_common_bp)

    try:
        mem_engine = get_mem_engine(app)
        ensure_dw_feedback_schema(mem_engine)
    except Exception as exc:  # pragma: no cover - defensive logging
        app.logger.exception("memdb.bootstrap.fail: %s", exc)

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
