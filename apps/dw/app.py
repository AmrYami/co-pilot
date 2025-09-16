"""Flask blueprint exposing DocuWare-specific endpoints."""

from __future__ import annotations

from typing import Iterable

from flask import Blueprint, jsonify, request

from core.pipeline import Pipeline
from core.settings import Settings
from core.sql_exec import get_mem_engine

from .seed import seed_dw_knowledge


dw_bp = Blueprint("dw", __name__, url_prefix="/dw")


def _coerce_prefixes(values: Iterable | None) -> list[str]:
    if not values:
        return []
    if isinstance(values, (list, tuple, set)):
        return [str(v) for v in values if v is not None]
    return [str(values)]


@dw_bp.route("/seed", methods=["POST"])
def seed():
    """Seed DocuWare metrics and join graph into the in-memory store."""

    data = request.get_json(force=True, silent=True) or {}
    namespace = data.get("namespace", "dw::common")
    force = bool(data.get("force", False))

    settings = Settings(namespace=namespace)
    mem_engine = get_mem_engine(settings)
    result = seed_dw_knowledge(mem_engine, namespace, force=force)
    payload = {"ok": True, "namespace": namespace, **result}
    return jsonify(payload), 200


@dw_bp.route("/answer", methods=["POST"])
def answer():
    """Forward DocuWare questions to the core pipeline."""

    body = request.get_json(force=True, silent=True) or {}
    prefixes = _coerce_prefixes(body.get("prefixes"))
    question = (body.get("question") or "").strip()
    auth_email = body.get("auth_email")

    settings = Settings(namespace="dw::common")
    pipeline = Pipeline(settings=settings, namespace="dw::common")

    context = {
        "namespace": "dw::common",
        "prefixes": prefixes,
        "auth_email": auth_email,
    }
    hints = {"datasource": "docuware"}

    result = pipeline.answer(question, context, hints=hints)
    return jsonify(result)
