from flask import Flask

from apps.dw.app import create_dw_blueprint
from core.admin_api import admin_bp
from core.pipeline import Pipeline
from core.settings import Settings


def create_app():
    app = Flask(__name__)

    settings = Settings()
    pipeline = Pipeline(settings=settings, namespace="dw::common")

    app.config["SETTINGS"] = settings
    app.config["pipeline"] = pipeline

    dw_bp = create_dw_blueprint(settings=settings, pipeline=pipeline)

    app.register_blueprint(dw_bp, url_prefix="/dw")
    app.register_blueprint(admin_bp, url_prefix="/admin")

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/model/info")
    def model_info():
        llm_meta = pipeline.llm.meta if getattr(pipeline, "llm", None) else {}
        return {
            "llm": llm_meta.get("model_name") or llm_meta.get("path") or "unknown",
            "clarifier": "disabled",
            "mode": "dw-pipeline",
        }

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
