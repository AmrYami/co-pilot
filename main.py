"""
Flask entrypoint wiring the core pipeline and FA blueprint.

Env you likely need:
  MEMORY_DB_URL=postgresql+psycopg2://copilot:pass@localhost/copilot_mem_dev
  FA_DB_URL=mysql+pymysql://fa_ro:***@localhost/frontaccounting?charset=utf8mb4
  ENVIRONMENT=local|server
  MODEL_BACKEND=exllama|hf-fp16|hf-8bit|hf-4bit
  MODEL_PATH=/models/SQLCoder-70B-EXL2 (or HF model id/path)

Run:
  export FLASK_APP=main:app
  flask run --reload
"""
from __future__ import annotations

from flask import Flask

from core.settings import Settings
from core.pipeline import Pipeline
from apps.fa.app import fa_bp
from core.admin_api import admin_bp




def create_app() -> Flask:
    # Namespace is set per-request by the FA routes based on prefixes.
    settings = Settings(namespace="fa::common")
    pipeline = Pipeline(settings=settings, namespace="fa::common")

    app = Flask(__name__)
    # Store pipeline so blueprints can use it
    app.config["PIPELINE"] = pipeline
    app.config["MEM_ENGINE"] = pipeline.mem_engine
    app.config["SETTINGS"] = pipeline.settings
    app.register_blueprint(admin_bp)

    app.register_blueprint(fa_bp, url_prefix="/fa")

    @app.get("/health")
    def health():  # type: ignore[no-redef]
        return {"ok": True, "namespace": "fa::common"}

    return app


# For `flask run`
# app = create_app()
