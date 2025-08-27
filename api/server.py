"""API server implementation"""

import os
from flask import Flask, request, jsonify
from typing import Dict, Any
from time import perf_counter

def create_app(orchestrator) -> Flask:
    """Create Flask application"""
    app = Flask(__name__)

    # Store orchestrator in app config
    app.config['orchestrator'] = orchestrator

    # CORS setup - use config directly if orchestrator exists
    if orchestrator and hasattr(orchestrator, 'config'):
        cors_origins = orchestrator.config.API_CORS_ORIGINS
    else:
        cors_origins = []

    @app.after_request
    def _cors(resp):
        if cors_origins:
            origin = request.headers.get("Origin", "")
            if origin in cors_origins or "*" in cors_origins:
                resp.headers["Access-Control-Allow-Origin"] = origin or "*"
                resp.headers["Vary"] = "Origin"
                resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-API-Key"
                resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    # Auth setup
    import hmac
    if orchestrator and hasattr(orchestrator, 'config'):
        cfg = orchestrator.config.get_auth_config()
    else:
        cfg = {"mode": "off"}  # Default to no auth if not initialized

    @app.before_request
    def _auth():
        # Ensure orchestrator is initialized for non-OPTIONS requests
        if request.method != "OPTIONS" and request.endpoint not in ['health']:
            orch = app.config.get('orchestrator')
            if orch and not orch.is_initialized():
                print("⚠️ Late initialization triggered by request")
                orch.initialize_system()

        if request.method == "OPTIONS":
            return
        if cfg["mode"] == "off":
            return

        def _is_valid(token: str) -> bool:
            return token is not None and any(hmac.compare_digest(token, k) for k in cfg["keys"])

        sent = None
        if cfg["mode"] in ("header", "both"):
            for h in cfg["header_names"]:
                v = request.headers.get(h)
                if v:
                    sent = v.strip()
                    break
            if not sent and cfg["allow_query"]:
                sent = (request.args.get("api_key") or "").strip() or None

        if not sent and cfg["mode"] in ("bearer", "both") and cfg["accept_bearer"]:
            auth = request.headers.get("Authorization", "")
            prefix = cfg["bearer_prefix"] + " "
            if auth.startswith(prefix):
                sent = auth[len(prefix):].strip()

        if cfg.get("keys") and not _is_valid(sent):
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    @app.get("/health")
    def health():
        orch = app.config.get('orchestrator')
        return jsonify({
            "ok": True,
            "initialized": orch.is_initialized() if orch else False,
            "db": os.getenv("DB_NAME"),
            "chat_model": "llama-3.1-8b",
            "sql_model": ("sqlcoder" if orch and orch.sqlgen is not None else "llama")
        })

    @app.post("/ask")
    def ask():
        orch = app.config.get('orchestrator')
        if not orch or not orch.is_initialized():
            return jsonify({"ok": False, "error": "system_not_initialized"}), 503

        t0 = perf_counter()
        try:
            data = request.get_json(force=True, silent=False) or {}
        except Exception:
            return jsonify({"ok": False, "error": "invalid_json"}), 400

        q = (data.get("q") or "").strip()
        if not q:
            return jsonify({"ok": False, "error": "missing_q"}), 400

        execute = bool(data.get("execute", True))
        limit = data.get("limit")
        limit = int(limit) if str(limit or "").isdigit() else None
        allow_sensitive = bool(data.get("allow_sensitive", False)) or orch.config.ALLOW_SENSITIVE_BY_DEFAULT

        with orch.gen_lock:
            result = orch.answer_question(q, execute=execute, limit=limit)

        # Check sensitive fields
        if not allow_sensitive and orch.security_validator.has_sensitive(result.get("sql", "")):
            import re
            if not re.search(r"\bnational[_\s]?id\b|\biban\b", q, re.IGNORECASE):
                return jsonify({
                    "ok": False,
                    "error": "sensitive_fields_blocked",
                    "message": "Query touches sensitive fields. Set allow_sensitive=true or mention fields explicitly."
                }), 400

        result["took_ms"] = int((perf_counter() - t0) * 1000)
        result["used_sqlcoder"] = orch.config.ASK_USES_SQLCODER and orch.sqlgen is not None

        return jsonify(result)

    @app.post("/sql")
    def sqlcoder_endpoint():
        t0 = perf_counter()
        data = request.get_json(force=True, silent=False) or {}
        q = (data.get("q") or "").strip()
        if not q:
            return jsonify({"ok": False, "error": "missing_q"}), 400

        execute = bool(data.get("execute", True))
        limit = data.get("limit")
        limit = int(limit) if str(limit or "").isdigit() else None
        allow_sensitive = bool(data.get("allow_sensitive", False)) or orchestrator.config.ALLOW_SENSITIVE_BY_DEFAULT

        with orchestrator.gen_lock:
            analysis = orchestrator.intent_analyzer.analyze_question_intent(q)
            refined = orchestrator.chat_interface.refine_user_question(q)

            try:
                sql = orchestrator.sql_generator.generate_sql_with_sqlcoder(refined, analysis)
                used_sqlcoder = True
            except Exception as e:
                # Fallback to Llama
                sql = orchestrator.sql_generator.generate_intelligent_sql(
                    refined, orchestrator.documentation, analysis
                )
                used_sqlcoder = False

        # Apply defaults and limits
        from hcm.sqlgen import apply_user_defaults_from_env, apply_default_user_columns
        sql = apply_user_defaults_from_env(sql)
        sql = apply_default_user_columns(sql, orchestrator.engine)

        if limit:
            sql = orchestrator.security_validator.enforce_limit(sql, limit)
        elif orchestrator.config.SQL_MAX_LIMIT:
            sql = orchestrator.security_validator.enforce_limit(sql, orchestrator.config.SQL_MAX_LIMIT)

        # Check sensitive fields
        if not allow_sensitive and orchestrator.security_validator.has_sensitive(sql):
            import re
            if not re.search(r"\bnational[_\s]?id\b|\biban\b", q, re.I):
                return jsonify({
                    "ok": False,
                    "error": "sensitive_fields_blocked",
                    "message": "Query touches sensitive fields."
                }), 400

        cols, rows_json = [], []
        if execute:
            cols, rows = orchestrator.db_manager.execute_sql_query(sql)
            rows_json = _json_rows(cols, rows)

        return jsonify({
            "ok": True,
            "mode": "sqlcoder" if used_sqlcoder else "fallback_llama",
            "sql": sql,
            "analysis": analysis,
            "columns": cols,
            "rows": rows_json,
            "took_ms": int((perf_counter() - t0) * 1000),
        })

    @app.post("/chat")
    def chat_endpoint():
        data = request.get_json(force=True, silent=False) or {}
        q = (data.get("q") or "").strip()
        if not q:
            return jsonify({"ok": False, "error": "missing_q"}), 400

        if orchestrator.generator is None or orchestrator.tokenizer is None:
            return jsonify({"ok": False, "error": "chat_model_unavailable"}), 503

        with orchestrator.gen_lock:
            system_msg = "You are a helpful assistant for HCM. Be concise and accurate."
            answer = orchestrator.chat_interface.generate_chat_response(system_msg, q)

        return jsonify({"ok": True, "mode": "chat", "answer": answer})

    return app


def run_api_server(app: Flask, config):
    """Run the API server"""
    host = config.API_HOST
    port = config.API_PORT

    # Check if we're in debug mode from environment
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1" or os.getenv("DEBUG", "0") == "1"

    # Simpler approach: just disable reloader if specified
    use_reloader = debug_mode and os.getenv("DISABLE_RELOAD", "0") != "1"

    if debug_mode:
        if use_reloader:
            print("⚠️ Running in debug mode with reloader - models will load twice")
            print("   To avoid this, set DISABLE_RELOAD=1 in your .env file")
        else:
            print("ℹ️ Running in debug mode with reloader disabled")

    print(f"\n🌐 Serving API on http://{host}:{port}  (POST /ask)")

    # Run the server
    app.run(
        host=host,
        port=port,
        threaded=True,
        debug=debug_mode,
        use_reloader=use_reloader
    )


def _json_rows(columns, rows):
    """Convert rows to JSON-safe format"""
    out = []
    for r in rows:
        item = {}
        for i, c in enumerate(columns):
            v = r[i]
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            item[c] = v
        out.append(item)
    return out