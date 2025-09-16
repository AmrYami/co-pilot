from flask import Flask

from core.settings import Settings
from core.pipeline import Pipeline
from core.routes import register_common_routes
from core.admin_api import create_admin_blueprint
from apps.dw.app import create_dw_blueprint


def create_app() -> Flask:
    settings = Settings()

    app = Flask(__name__)

    # Health and route inspection endpoints
    register_common_routes(app)

    # Build pipeline with DocuWare namespace by default
    active_ns = settings.get("ACTIVE_NAMESPACE", "dw::common")
    pipeline = Pipeline(settings=settings, namespace=active_ns)

    # Register blueprints
    app.register_blueprint(create_admin_blueprint(settings), url_prefix="/admin")
    app.register_blueprint(create_dw_blueprint(pipeline), url_prefix="/dw")

    return app
