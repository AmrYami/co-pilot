from flask import Flask

from apps.dw.app import dw_bp
from core.admin_api import admin_bp


def create_app():
    app = Flask(__name__)

    app.register_blueprint(dw_bp, url_prefix="/dw")
    app.register_blueprint(admin_bp, url_prefix="/admin")

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/model/info")
    def model_info():
        return {
            "llm": "disabled",
            "clarifier": "disabled",
            "mode": "dw-simplified",
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
