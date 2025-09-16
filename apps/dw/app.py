from __future__ import annotations

from typing import Any, List

from flask import Blueprint, jsonify, request

from core.pipeline import Pipeline
from core.settings import Settings

bp = Blueprint("dw", __name__)


def _coerce_prefixes(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        return [str(p) for p in raw if p is not None]
    return [str(raw)]


@bp.post("/answer")
def answer():
    payload = request.get_json(force=True, silent=True) or {}
    question = (payload.get("question") or "").strip()
    prefixes = _coerce_prefixes(payload.get("prefixes"))
    auth_email = payload.get("auth_email")

    settings = Settings()
    namespace = (
        settings.get("ACTIVE_NAMESPACE", "dw::common", scope="namespace")
        or "dw::common"
    )
    pipeline = Pipeline(settings=settings, namespace=namespace)

    context = {"namespace": namespace, "prefixes": prefixes, "auth_email": auth_email}
    result = pipeline.answer(question, context)
    return jsonify(result)
