import logging
import os
import time

from flask import Flask, jsonify, g, request
from sqlalchemy import create_engine

from apps.common.admin import admin_bp as admin_common_bp
from apps.dw.app import create_dw_blueprint
from apps.dw.admin_api import bp as dw_admin_bp
from apps.dw.routes import debug_bp
from apps.dw.tests.routes import golden_bp
from core.admin_api import admin_bp as core_admin_bp
from core.logging_utils import get_logger, log_event, setup_logging
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

    # Warm up SQL model unless explicitly disabled
    import os as _os
    _disable_sql = str(_os.getenv("DISABLE_SQL_MODEL", "0")).strip().lower() in {"1", "true", "yes"}
    if not _disable_sql:
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

    _install_dw_answer_trace(app)
    _log_dw_answer_binding(app)
    _install_dw_answer_trace_wrappers(app)

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


def _install_dw_answer_trace(app: Flask) -> None:
    logger = logging.getLogger("dw")

    def _should_log() -> bool:
        return str(os.getenv("DW_LOG_ANSWER", "1")).lower() in {"1", "true", "yes"}

    @app.before_request
    def _dw_answer_trace_before():  # pragma: no cover - request hooks
        if request.path == "/dw/answer" and request.method == "POST":
            if not _should_log():
                return
            g._dw_answer_t0 = time.time()
            payload = request.get_json(silent=True) or {}
            try:
                logger.info(
                    {
                        "event": "answer.receive",
                        "auth_email": payload.get("auth_email"),
                        "full_text_search": bool(payload.get("full_text_search")),
                        "question_len": len((payload.get("question") or "").strip()),
                    }
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.info({"event": "answer.receive"})

    @app.after_request
    def _dw_answer_trace_after(resp):  # pragma: no cover - request hooks
        if request.path == "/dw/answer" and request.method == "POST":
            if not _should_log():
                return resp
            try:
                ms = int((time.time() - g.get("_dw_answer_t0", time.time())) * 1000)
            except Exception:  # pragma: no cover - defensive logging
                ms = None
            try:
                data = resp.get_json(silent=True)
            except Exception:  # pragma: no cover - defensive logging
                data = None
            inq_id = (data or {}).get("inquiry_id") if isinstance(data, dict) else None
            meta = {}
            debug = {}
            fts_meta = {}
            fts_debug = {}
            fts_enabled = None
            fts_error = None
            fts_tokens = []
            fts_cols = []
            fts_engine = None
            try:
                meta = (data or {}).get("meta") or {}
                debug = (data or {}).get("debug") or {}
                fts_meta = meta.get("fts") or {}
                fts_debug = (debug.get("fts") or {})
                fts_enabled = fts_meta.get("enabled")
                fts_error = fts_meta.get("error") or None
                fts_tokens = (
                    fts_meta.get("tokens")
                    or fts_debug.get("tokens")
                    or []
                )
                fts_cols = fts_meta.get("columns") or []
                fts_engine = fts_debug.get("engine")
                logger.info(
                    {
                        "event": "answer.meta",
                        "inquiry_id": inq_id,
                        "fts": {
                            "enabled": fts_enabled,
                            "error": fts_error,
                            "tokens": fts_tokens,
                            "columns_count": (
                                len(fts_cols)
                                if isinstance(fts_cols, list)
                                else None
                            ),
                            "engine": fts_engine,
                        },
                        "rows": meta.get("rows"),
                        "duration_ms": meta.get("duration_ms"),
                    }
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.info({"event": "answer.meta"})

            sql = (data or {}).get("sql")
            ob = None
            try:
                if isinstance(sql, str):
                    up = sql.upper()
                    if "ORDER BY" in up:
                        ob = sql[up.index("ORDER BY") :].splitlines()[0]
                logger.info(
                    {
                        "event": "answer.sql.preview",
                        "preview": (
                            (sql[:500] + "…")
                            if isinstance(sql, str) and len(sql) > 500
                            else sql
                        ),
                        "order_by": ob,
                    }
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.info({"event": "answer.sql.preview"})
            try:
                logger.info(
                    {
                        "event": "answer.response",
                        "inquiry_id": inq_id,
                        "fts_enabled": fts_enabled,
                        "ms": ms,
                    }
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.info({"event": "answer.response"})
        return resp


def _log_dw_answer_binding(app: Flask) -> None:
    logger = logging.getLogger("dw")
    try:  # pragma: no cover - boot logging only
        for rule in app.url_map.iter_rules():
            if rule.rule == "/dw/answer" and "POST" in (rule.methods or set()):
                fn = app.view_functions.get(rule.endpoint)
                logger.info(
                    {
                        "event": "boot.answer.endpoint",
                        "endpoint": rule.endpoint,
                        "module": getattr(fn, "__module__", None),
                        "qualname": getattr(fn, "__qualname__", None),
                    }
                )
                break
    except Exception:  # pragma: no cover - defensive logging
        logger.info({"event": "boot.answer.endpoint"})


def _install_dw_answer_trace_wrappers(app: Flask) -> None:
    """Best-effort: instrument core functions if available without changing behavior."""

    logger = logging.getLogger("dw")

    def _wrap(mod_name: str, attr_path: str, before=None, after=None) -> None:
        try:
            import importlib

            mod = importlib.import_module(mod_name)
            target = mod
            parent = mod
            name = None
            for part in attr_path.split("."):
                parent = target
                target = getattr(target, part)
                name = part
            if getattr(target, "_dw_traced", False):
                return

            def wrapper(*args, **kwargs):
                if callable(before):
                    try:
                        before(args, kwargs)
                    except Exception:  # pragma: no cover - defensive logging
                        logger.info(
                            {
                                "event": "answer.trace.before",
                                "fn": f"{mod_name}.{attr_path}",
                            }
                        )
                t0 = time.time()
                out = target(*args, **kwargs)
                if callable(after):
                    try:
                        after(args, kwargs, out, int((time.time() - t0) * 1000))
                    except Exception:  # pragma: no cover - defensive logging
                        logger.info(
                            {
                                "event": "answer.trace.after",
                                "fn": f"{mod_name}.{attr_path}",
                            }
                        )
                return out

            wrapper._dw_traced = True
            setattr(parent, name, wrapper)
            logger.info({"event": "boot.answer.trace.wrap", "fn": f"{mod_name}.{attr_path}"})
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.info(
                {
                    "event": "boot.answer.trace.wrap.skip",
                    "fn": f"{mod_name}.{attr_path}",
                    "err": str(exc),
                }
            )

    _wrap(
        "apps.dw.rules_loader",
        "load_for",
        before=lambda args, kwargs: logger.info({"event": "rules.load.start"}),
        after=lambda args, kwargs, out, ms: logger.info(
            {
                "event": "rules.load.ok",
                "rules_count": (len(out) if hasattr(out, "__len__") else None),
                "ms": ms,
            }
        ),
    )

    _wrap(
        "apps.dw.engine",
        "evaluate_fts",
        before=lambda args, kwargs: None,
        after=lambda args, kwargs, out, ms: logger.info(
            {
                "event": "fts.eval",
                "enabled": bool((out or {}).get("enabled")),
                "error": (out or {}).get("error"),
                "columns_count": len((out or {}).get("columns") or []),
                "ms": ms,
            }
        ),
    )

    _wrap(
        "apps.dw.contracts.builder",
        "build_sql",
        before=lambda args, kwargs: logger.info({"event": "sql.exec"}),
        after=lambda args, kwargs, out, ms: (
            (lambda sql, binds: logger.info(
                {
                    "event": "sql.done",
                    "bind_names": list((binds or {}).keys()),
                    "preview": (
                        (sql[:500] + "…")
                        if isinstance(sql, str) and len(sql) > 500
                        else sql
                    ),
                    "ms": ms,
                }
            ))(*out)
            if isinstance(out, tuple) and len(out) >= 2
            else logger.info({"event": "sql.done", "ms": ms})
        ),
    )

    try:
        import importlib

        builder_mod = importlib.import_module("apps.dw.contracts.builder")
        if hasattr(builder_mod, "normalize_order_by"):
            _wrap(
                "apps.dw.contracts.builder",
                "normalize_order_by",
                before=lambda args, kwargs: None,
                after=lambda args, kwargs, out, ms: logger.info(
                    {"event": "builder.order.guard", "order_by": out}
                ),
            )
    except Exception:
        pass


app = create_app()
