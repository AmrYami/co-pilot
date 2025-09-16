from flask import Flask, jsonify
from core.settings import Settings
from core.pipeline import Pipeline


def create_app():
    app = Flask(__name__)
    settings = Settings()

    # Register Admin API (bulk settings, etc.)
    admin_blueprint = None
    try:
        from core.admin_api import admin_bp as _admin_bp
        admin_blueprint = _admin_bp
    except Exception:
        try:
            from core.admin_api import create_admin_blueprint as _create_admin_blueprint
        except Exception:
            _create_admin_blueprint = None
        if _create_admin_blueprint is not None:
            admin_blueprint = _create_admin_blueprint(settings)

    if admin_blueprint is not None:
        app.register_blueprint(admin_blueprint, url_prefix=getattr(admin_blueprint, "url_prefix", None) or "/admin")

    # Build pipeline for DW
    pipeline = Pipeline(settings=settings, namespace="dw::common")
    app.config["pipeline"] = pipeline

    # Register DW app
    from apps.dw import create_dw_blueprint
    app.register_blueprint(create_dw_blueprint(settings))

    # Diagnostics
    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    @app.get("/__routes")
    def routes():
        return jsonify(sorted([str(r) for r in app.url_map.iter_rules()]))

    return app


# For flask CLI
app = create_app()
