from flask import Flask

from apps.dw.app import dw_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(dw_bp)
    return app


app = create_app()
