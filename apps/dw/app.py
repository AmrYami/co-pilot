"""DocuWare blueprint exposing seeding and answering endpoints."""

from __future__ import annotations

import logging
from typing import Iterable, List

from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import text

from core.pipeline import Pipeline
from core.seed import upsert_metrics
from core.settings import Settings
from core.snippets import save_snippet
from core.sql_exec import get_mem_engine, run_sql

from .derive import route_question_to_sql


logger = logging.getLogger(__name__)

dw_bp = Blueprint("dw", __name__, url_prefix="/dw")

DEFAULT_NAMESPACE = "dw::common"


def _current_pipeline() -> Pipeline:
    """Return the process-wide pipeline instance, creating it if required."""

    pipe = getattr(current_app, "pipeline", None)
    if isinstance(pipe, Pipeline):
        return pipe

    settings = Settings(namespace=DEFAULT_NAMESPACE)
    pipe = Pipeline(settings=settings, namespace=DEFAULT_NAMESPACE)
    current_app.pipeline = pipe
    return pipe


def _seed_metrics(mem_engine, namespace: str, metrics: Iterable[dict], *, force: bool) -> int:
    """Insert or refresh DocuWare metrics for the provided namespace."""

    if force:
        with mem_engine.begin() as conn:
            conn.execute(
                text("DELETE FROM mem_metrics WHERE namespace = :ns"),
                {"ns": namespace},
            )

    result = upsert_metrics(mem_engine, namespace=namespace, metrics=list(metrics))
    return result.count


def _reset_join_graph(mem_engine, namespace: str, *, force: bool) -> None:
    """Clear join graph entries when force=True (DocuWare currently single-table)."""

    if not force:
        return

    with mem_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM mem_join_graph WHERE namespace = :ns"),
            {"ns": namespace},
        )


@dw_bp.route("/seed", methods=["POST"])
def seed() -> tuple:
    """Seed minimal DocuWare knowledge into the in-memory metadata store."""

    payload = request.get_json(force=True, silent=True) or {}
    namespace = (payload.get("namespace") or DEFAULT_NAMESPACE).strip()
    force = bool(payload.get("force"))

    settings = Settings(namespace=namespace)
    mem_engine = get_mem_engine(settings)
    settings.attach_mem_engine(mem_engine)

    metrics: List[dict] = [
        {
            "metric_key": "contract_value_gross",
            "metric_name": "Contract Gross Value",
            "description": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
            "calculation_sql": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
            "required_tables": ["Contract"],
            "required_columns": ["CONTRACT_VALUE_NET_OF_VAT", "VAT"],
            "category": "contracts",
        },
        {
            "metric_key": "contract_count_active",
            "metric_name": "Active Contracts Count",
            "description": "Count of contracts with END_DATE >= SYSDATE",
            "calculation_sql": "CASE WHEN END_DATE IS NULL OR END_DATE >= SYSDATE THEN 1 ELSE 0 END",
            "required_tables": ["Contract"],
            "required_columns": ["END_DATE"],
            "category": "contracts",
        },
    ]

    metric_count = _seed_metrics(mem_engine, namespace, metrics, force=force)
    _reset_join_graph(mem_engine, namespace, force=force)

    return jsonify({"ok": True, "namespace": namespace, "metrics": metric_count}), 200


def _autosave_snippet(pipe: Pipeline, question: str, sql: str) -> None:
    """Persist a reusable snippet when autosave is enabled for the namespace."""

    try:
        enabled = pipe.settings.get_bool(
            "SNIPPETS_AUTOSAVE",
            scope="namespace",
            namespace=pipe.namespace,
            default=True,
        )
    except Exception:  # pragma: no cover - defensive fallback
        enabled = False

    if not enabled:
        return

    try:
        save_snippet(
            pipe.mem_engine,
            pipe.namespace,
            question or "Auto snippet",
            sql,
            tags=[pipe.active_app or "dw", "auto", "snippet"],
        )
    except Exception as exc:  # pragma: no cover - logging only
        logger.exception("SNIPPETS_AUTOSAVE failed: %s", exc)


@dw_bp.route("/answer", methods=["POST"])
def answer():
    """Handle DocuWare questions by routing to a simple SQL generator."""

    payload = request.get_json(force=True, silent=True) or {}
    question = (payload.get("question") or "").strip()

    if not question:
        return jsonify({"ok": False, "error": "missing_question"}), 400

    pipeline = _current_pipeline()

    sql = route_question_to_sql(question)
    if not sql:
        clarifiers = [
            "Should I show top departments or top stakeholders by contract value?",
            "Which date range should I use (e.g., last month, last 3 months, last year)?",
        ]
        return (
            jsonify(
                {
                    "ok": False,
                    "status": "needs_clarification",
                    "questions": clarifiers,
                }
            ),
            200,
        )

    result = run_sql(pipeline.app_engine, sql)

    if result.ok:
        _autosave_snippet(pipeline, question, sql)
        payload = {
            "ok": True,
            "status": "answered",
            "sql": sql.strip(),
            "columns": result.columns,
            "rows": result.rows,
            "rowcount": result.rowcount,
        }
        return jsonify(payload), 200

    return (
        jsonify(
            {
                "ok": False,
                "status": "failed",
                "sql": sql.strip(),
                "error": result.error,
            }
        ),
        400,
    )
