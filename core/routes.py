from flask import jsonify


def register_common_routes(app) -> None:
    @app.get("/health")
    def health():
        return jsonify(ok=True, status="healthy")

    @app.get("/__routes")
    def list_routes():
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append(
                {
                    "endpoint": rule.endpoint,
                    "methods": sorted(list(rule.methods - {"HEAD", "OPTIONS"})),
                    "rule": str(rule),
                }
            )
        return jsonify(routes=sorted(routes, key=lambda r: r["rule"]))
