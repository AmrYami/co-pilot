from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from core.agents import PlannerAgent, ValidatorAgent
from core.datasources import DatasourceRegistry
from core.intent import IntentRouter
from core.inquiries import (
    create_or_update_inquiry,
    set_inquiry_status,
    update_inquiry_status_run,
)
from core.model_loader import load_llm_from_settings, model_info as loader_model_info
from core.research import load_researcher
from core.settings import Settings
from core.snippets import autosave_snippet
from core.sql_exec import SQLExecutionResult, get_mem_engine, run_sql
from core.sql_utils import extract_sql, extract_sql_one_stmt
from core.logging_utils import get_logger, log_event

try:  # pragma: no cover - optional hints module
    from apps.dw.hints import (
        get_date_columns,
        get_join_hints,
        get_metric_hints,
        get_reserved_terms,
    )
except Exception:  # pragma: no cover - fallback if hints unavailable

    def get_join_hints(*args, **kwargs):
        return []

    def get_metric_hints(*args, **kwargs):
        return {}

    def get_reserved_terms(*args, **kwargs):
        return {}

    def get_date_columns(*args, **kwargs):
        return {}


try:  # pragma: no cover - deterministic fallback templates (optional)
    from apps.dw.answerer import AnswerError, StakeholderAnswerer
except Exception:  # pragma: no cover - fallback when templates unavailable

    class AnswerError(Exception):
        pass

    StakeholderAnswerer = None  # type: ignore[assignment]


log = get_logger(__name__)


class Pipeline:
    """LLM-assisted SQL pipeline used by the DocuWare app."""

    def __init__(self, settings: Settings | None = None, namespace: str = "dw::common") -> None:
        self.settings = settings or Settings(namespace=namespace)
        self.namespace = namespace or getattr(self.settings, "namespace", "dw::common")

        try:
            self.settings.set_namespace(self.namespace)
        except AttributeError:
            pass

        # Memory DB (Postgres) for metadata and bookkeeping
        self.mem = get_mem_engine(self.settings)
        self.mem_engine = self.mem
        try:
            self.settings.attach_mem_engine(self.mem)
        except AttributeError:
            pass

        # Datasource registry (Oracle for DocuWare)
        self.ds = DatasourceRegistry(self.settings, namespace=self.namespace)

        try:
            self.app_engine = self.ds.engine(None)
        except Exception:
            self.app_engine = None

        self.intent_router = IntentRouter()

        # Clarifier stays disabled per environment request
        self.clarifier_llm = None

        # Load the SQL generation model (SQLCoder via ExLlama2 or configured backend)
        self.llm, self.llm_info = load_llm_from_settings(self.settings)
        if not self.llm:
            log.warning(
                "Base SQL LLM disabled or unavailable; relying on deterministic fallbacks."
            )

        self.validator = (
            ValidatorAgent(self.app_engine, self.settings) if self.app_engine else None
        )

        try:
            self.researcher = load_researcher(self.settings)
        except Exception:
            self.researcher = None

        self.active_app = (
            (self.settings.get("ACTIVE_APP", scope="namespace") or "dw").strip() or "dw"
        )

    # ------------------------------------------------------------------
    def model_info(self) -> Dict[str, Any]:
        return loader_model_info()

    # ------------------------------------------------------------------
    def answer_dw(
        self,
        question: str,
        prefixes: Optional[Sequence[str]] = None,
        auth_email: Optional[str] = None,
        datasource: Optional[str] = None,
    ) -> Dict[str, Any]:
        prefixes_list = list(prefixes or [])
        question_text = (question or "").strip()
        channel = "dw.pipeline"
        if not question_text:
            log_event(log, channel, "status", {"status": "error", "reason": "question_empty"})
            return {"ok": False, "status": "error", "error": "question_empty"}

        datasource_name = datasource or self._default_datasource() or "docuware"
        log_event(
            log,
            channel,
            "pipeline_start",
            {"question": question_text, "datasource": datasource_name, "auth_email": auth_email},
        )
        inquiry_id = self._record_inquiry(question_text, prefixes_list, auth_email, datasource_name)

        try:
            result = self.answer(
                question=question_text,
                prefixes=prefixes_list,
                auth_email=auth_email,
                namespace=self.namespace,
                datasource=datasource_name,
                dialect="oracle",
                app_tag="dw",
            )
        except Exception as exc:
            log.exception("DocuWare pipeline error: %s", exc)
            log_event(log, channel, "pipeline_exception", {"error": str(exc)})
            fallback = self._deterministic_fallback(question_text)
            if fallback:
                self._mark_inquiry_answered(inquiry_id, fallback.get("run_id"), "dw_template")
                meta = fallback.setdefault("meta", {})
                meta.setdefault("fallback_reason", "pipeline_exception")
                fallback["inquiry_id"] = inquiry_id
                log_event(log, channel, "status", {"status": "fallback", "reason": "pipeline_exception"})
                return fallback

            self._mark_inquiry_needs_clarification(inquiry_id, question_text, status="failed")
            log_event(log, channel, "status", {"status": "failed", "reason": str(exc)})
            return {
                "ok": False,
                "status": "error",
                "error": str(exc),
                "inquiry_id": inquiry_id,
            }

        result = dict(result or {})
        result["inquiry_id"] = inquiry_id

        if result.get("ok"):
            self._mark_inquiry_answered(inquiry_id, result.get("run_id"), "pipeline")
            if result.get("rowcount", 0) == 0:
                fallback = self._deterministic_fallback(question_text)
                if fallback:
                    self._mark_inquiry_answered(
                        inquiry_id, fallback.get("run_id"), "dw_template"
                    )
                    meta = fallback.setdefault("meta", {})
                    meta.setdefault("fallback_reason", "llm_zero_rows")
                    fallback["inquiry_id"] = inquiry_id
                    log_event(log, channel, "status", {"status": "fallback", "reason": "llm_zero_rows"})
                    return fallback
            log_event(log, channel, "status", {"status": "ok", "rowcount": result.get("rowcount")})
            return result

        fallback = self._deterministic_fallback(question_text)
        if fallback:
            self._mark_inquiry_answered(inquiry_id, fallback.get("run_id"), "dw_template")
            meta = fallback.setdefault("meta", {})
            meta.setdefault("fallback_reason", "llm_plan_failed")
            fallback["inquiry_id"] = inquiry_id
            log_event(log, channel, "status", {"status": "fallback", "reason": "llm_plan_failed"})
            return fallback

        status = result.get("status") or "needs_clarification"
        if status == "needs_clarification" and not result.get("questions"):
            result["questions"] = [
                "Could you clarify the tables, filters, or date range you expect in the answer?",
            ]
        self._mark_inquiry_needs_clarification(inquiry_id, question_text, status=status)
        result["status"] = status
        log_event(log, channel, "status", {"status": status, "inquiry_id": inquiry_id})
        return result

    # ------------------------------------------------------------------
    def answer(
        self,
        *,
        question: str,
        prefixes: Optional[Sequence[str]] = None,
        auth_email: Optional[str] = None,
        namespace: Optional[str] = None,
        datasource: Optional[str] = None,
        dialect: str = "oracle",
        app_tag: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        hints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        prefixes = list(prefixes or [])
        namespace = namespace or self.namespace
        question_text = (question or "").strip()
        base_channel = (app_tag or "pipeline") or "pipeline"
        channel = f"{base_channel}.pipeline" if app_tag else "pipeline"

        log_event(
            log,
            channel,
            "inquiry_start",
            {
                "question": question_text,
                "namespace": namespace,
                "prefixes": prefixes,
                "auth_email": auth_email,
                "datasource": datasource,
            },
        )

        if not question_text:
            log_event(log, channel, "status", {"status": "error", "reason": "question_empty"})
            return {"ok": False, "status": "error", "error": "question_empty"}

        intent = self.intent_router.classify(question_text)
        log_event(log, channel, "intent_detected", {"kind": intent.kind})
        if intent.kind == "smalltalk":
            log_event(log, channel, "status", {"status": "smalltalk"})
            return {
                "ok": False,
                "status": "smalltalk",
                "message": "Please ask a DocuWare data question.",
            }
        if intent.kind == "help":
            log_event(log, channel, "status", {"status": "help"})
            return {
                "ok": False,
                "status": "help",
                "message": "Try asking about DocuWare contracts or stakeholders.",
            }

        ds_engine = self._resolve_engine(datasource)

        if intent.kind == "raw_sql":
            log_event(log, channel, "mode", {"mode": "raw_sql"})
            return self._run_raw_sql(
                question_text,
                engine=ds_engine,
                namespace=namespace,
                prefixes=prefixes,
                auth_email=auth_email,
                datasource=datasource,
                app_tag=app_tag,
            )

        base_context = self._build_context(namespace)
        if context:
            base_context.update(context)
        base_context["prefixes"] = prefixes
        base_context["dialect"] = dialect

        log_event(
            log,
            channel,
            "plan_request",
            {"has_context": bool(context), "has_hints": bool(hints)},
        )
        plan = self._plan_sql(question_text, base_context, hints or {})
        log_event(
            log,
            channel,
            "plan_result",
            {
                "status": plan.get("status"),
                "has_sql": bool(plan.get("sql")),
                "rationale": (plan.get("rationale") or "")[:200],
            },
        )
        if plan.get("status") != "ok" or not plan.get("sql"):
            log_event(
                log,
                channel,
                "status",
                {
                    "status": plan.get("status", "needs_clarification"),
                    "reason": plan.get("error"),
                },
            )
            return {
                "ok": False,
                "status": plan.get("status", "needs_clarification"),
                "questions": plan.get("questions"),
                "context": {"namespace": namespace, "prefixes": prefixes},
                "rationale": plan.get("rationale"),
                "error": plan.get("error"),
            }

        sql_text = plan["sql"]
        raw_sql_initial = plan.get("raw_sql") or sql_text
        rationale = plan.get("rationale")

        if self.validator and getattr(self.validator, "fa", None):
            log_event(log, channel, "validation_quick", {"enabled": True})
            valid, info = self.validator.quick_validate(sql_text)
            log_event(
                log,
                channel,
                "validation_quick_result",
                {"ok": bool(valid), "error": info.get("error")},
            )
            if not valid:
                log_event(log, channel, "status", {"status": "validation_failed"})
                return {
                    "ok": False,
                    "status": "validation_failed",
                    "error": info.get("error"),
                    "details": info,
                }

        log_event(
            log,
            channel,
            "execution_start",
            {"sql_size": len(sql_text)},
        )
        try:
            exec_result = self._execute_sql(ds_engine, sql_text)
        except Exception as exc:
            log_event(log, channel, "execution_error", {"error": str(exc)})
            raise
        log_event(
            log,
            channel,
            "execution_result",
            {
                "rows": exec_result.rowcount,
                "columns": list(exec_result.columns),
            },
        )
        final_sql = sql_text
        final_raw_sql = raw_sql_initial
        final_result = exec_result
        attempts = [
            {
                "sql": sql_text,
                "rowcount": exec_result.rowcount,
                "type": "initial",
                "raw_sql": raw_sql_initial,
            }
        ]

        if exec_result.rowcount == 0:
            log_event(log, channel, "auto_retry_check", {"rowcount": exec_result.rowcount})
            retry = self._maybe_autoretry(
                question_text,
                base_context,
                hints or {},
                ds_engine,
            )
            if retry is not None:
                final_sql = retry["sql"]
                final_raw_sql = retry.get("raw_sql") or final_sql
                final_result = retry["result"]
                rationale = retry.get("rationale") or rationale
                attempts.append(
                    {
                        "sql": retry["sql"],
                        "rowcount": retry["result"].rowcount,
                        "type": "auto_retry",
                        "raw_sql": retry.get("raw_sql") or retry["sql"],
                    }
                )
                log_event(
                    log,
                    channel,
                    "auto_retry",
                    {"rowcount": retry["result"].rowcount},
                )

        rows = list(final_result.rows)
        columns = list(final_result.columns)
        rowcount = final_result.rowcount

        meta: Dict[str, Any] = {
            "namespace": namespace,
            "datasource": datasource or self._default_datasource(),
            "dialect": dialect,
            "rationale": rationale,
            "attempts": attempts,
            "prefixes": prefixes,
            "raw_sql_initial": raw_sql_initial,
            "raw_sql_final": final_raw_sql,
        }

        response: Dict[str, Any] = {
            "ok": True,
            "status": "ok",
            "sql": final_sql,
            "rows": rows,
            "columns": columns,
            "rowcount": rowcount,
            "meta": meta,
        }

        if rowcount == 0:
            response["hint"] = self._friendly_empty_hint()

        run_id = self._record_run(
            namespace=namespace,
            question=question_text,
            sql=final_sql,
            rows=rows,
            datasource=datasource,
            auth_email=auth_email,
        )
        if run_id is not None:
            response["run_id"] = run_id

        try:
            tags = self._build_tags(app_tag, prefixes)
            autosave_snippet(self.mem_engine, namespace, datasource, final_sql, tags=tags)
        except Exception as exc:  # pragma: no cover - best effort
            log.debug("autosave snippet failed: %s", exc)

        log_event(
            log,
            channel,
            "status",
            {"status": response.get("status"), "rowcount": rowcount},
        )
        return response

    # ------------------------------------------------------------------
    def _record_inquiry(
        self,
        question: str,
        prefixes: Sequence[str],
        auth_email: Optional[str],
        datasource: str,
    ) -> Optional[int]:
        if not getattr(self, "mem_engine", None):
            return None
        try:
            return create_or_update_inquiry(
                self.mem_engine,
                namespace=self.namespace,
                prefixes=list(prefixes),
                question=question,
                auth_email=auth_email,
                run_id=None,
                research_enabled=bool(self.researcher),
                datasource=datasource,
                status="open",
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("create_or_update_inquiry failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    def _mark_inquiry_answered(
        self,
        inquiry_id: Optional[int],
        run_id: Optional[int],
        answered_by: str,
    ) -> None:
        if not inquiry_id or not getattr(self, "mem_engine", None):
            return
        try:
            update_inquiry_status_run(
                self.mem_engine,
                inquiry_id,
                status="answered",
                run_id=run_id,
                answered_by=answered_by,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("update_inquiry_status_run failed: %s", exc)

    # ------------------------------------------------------------------
    def _mark_inquiry_needs_clarification(
        self,
        inquiry_id: Optional[int],
        question: str,
        *,
        status: str = "needs_clarification",
    ) -> None:
        if not inquiry_id or not getattr(self, "mem_engine", None):
            return
        try:
            set_inquiry_status(
                self.mem_engine,
                inquiry_id,
                status,
                last_question=question,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("set_inquiry_status failed: %s", exc)

    # ------------------------------------------------------------------
    def _deterministic_fallback(self, question: str) -> Optional[Dict[str, Any]]:
        if StakeholderAnswerer is None or not getattr(self, "mem_engine", None):
            return None
        try:
            answerer = StakeholderAnswerer(self.settings, self.mem_engine, self.ds)
            result = answerer.answer(question)
        except AnswerError:
            return None
        except Exception as exc:  # pragma: no cover - defensive logging
            log.debug("Deterministic fallback failed: %s", exc)
            return None

        rows = list(result.rows)
        columns = list(rows[0].keys()) if rows else []
        meta = {
            "mode": "deterministic_template",
            "top_n": result.top_n,
            "date_start": result.date_start.isoformat(),
            "date_end": result.date_end.isoformat(),
            "tags": list(result.tags or []),
        }
        payload: Dict[str, Any] = {
            "ok": True,
            "status": "ok",
            "sql": result.sql,
            "rows": rows,
            "columns": columns,
            "rowcount": len(rows),
            "meta": meta,
        }
        if not rows:
            payload["hint"] = self._friendly_empty_hint()
        if result.run_id is not None:
            payload["run_id"] = result.run_id
        return payload

    # ------------------------------------------------------------------
    def _default_datasource(self) -> Optional[str]:
        return self.settings.get_string("DEFAULT_DATASOURCE", scope="namespace")

    # ------------------------------------------------------------------
    def _resolve_engine(self, datasource: Optional[str]):
        try:
            return self.ds.engine(datasource)
        except Exception:
            return self.ds.engine(None)

    # ------------------------------------------------------------------
    def _run_raw_sql(
        self,
        sql_text: str,
        *,
        engine,
        namespace: str,
        prefixes: Sequence[str],
        auth_email: Optional[str],
        datasource: Optional[str],
        app_tag: Optional[str],
    ) -> Dict[str, Any]:
        try:
            result = self._execute_sql(engine, sql_text)
        except Exception as exc:
            return {
                "ok": False,
                "status": "raw_sql_error",
                "error": str(exc),
            }

        rows = list(result.rows)
        response = {
            "ok": True,
            "status": "ok",
            "sql": sql_text,
            "rows": rows,
            "columns": list(result.columns),
            "rowcount": result.rowcount,
            "meta": {
                "namespace": namespace,
                "datasource": datasource or self._default_datasource(),
                "prefixes": list(prefixes),
                "mode": "raw_sql",
            },
        }

        run_id = self._record_run(
            namespace=namespace,
            question=sql_text,
            sql=sql_text,
            rows=rows,
            datasource=datasource,
            auth_email=auth_email,
        )
        if run_id is not None:
            response["run_id"] = run_id

        try:
            tags = self._build_tags(app_tag, prefixes)
            autosave_snippet(self.mem_engine, namespace, datasource, sql_text, tags=tags)
        except Exception:
            pass

        return response

    # ------------------------------------------------------------------
    def _build_context(self, namespace: str) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "tables": [],
            "columns": [],
            "metrics": {},
            "join_hints": [],
            "reserved_terms": {},
            "date_columns": {},
        }

        with self.mem_engine.connect() as conn:
            tables = conn.execute(
                text(
                    """
                    SELECT table_name, schema_name, table_comment
                      FROM mem_tables
                     WHERE namespace = :ns
                  ORDER BY table_name
                    """
                ),
                {"ns": namespace},
            ).mappings().all()
            columns = conn.execute(
                text(
                    """
                    SELECT t.table_name, c.column_name, c.data_type, c.is_nullable
                      FROM mem_columns c
                      JOIN mem_tables t ON t.id = c.table_id
                     WHERE t.namespace = :ns
                  ORDER BY t.table_name, c.column_name
                    """
                ),
                {"ns": namespace},
            ).mappings().all()
            metrics = conn.execute(
                text(
                    """
                    SELECT metric_key, metric_name, calculation_sql, description
                      FROM mem_metrics
                     WHERE namespace = :ns
                  ORDER BY metric_key
                    """
                ),
                {"ns": namespace},
            ).mappings().all()

        context["tables"] = [dict(row) for row in tables]
        context["columns"] = [dict(row) for row in columns]
        context["metrics"] = {row["metric_key"]: dict(row) for row in metrics if row.get("metric_key")}

        try:
            context["join_hints"] = get_join_hints(namespace)
        except Exception:
            context["join_hints"] = []
        try:
            context["reserved_terms"] = get_reserved_terms(namespace)
        except Exception:
            context["reserved_terms"] = {}
        try:
            context["date_columns"] = get_date_columns(namespace)
        except Exception:
            context["date_columns"] = {}
        try:
            metric_hints = get_metric_hints(namespace)
            if metric_hints:
                context.setdefault("metrics", {})
                for key, expr in metric_hints.items():
                    context["metrics"].setdefault(key, {"calculation_sql": expr})
        except Exception:
            pass

        return context

    # ------------------------------------------------------------------
    def _plan_sql(self, question: str, context: Dict[str, Any], hints: Dict[str, Any]) -> Dict[str, Any]:
        if not self.llm:
            return {
                "status": "llm_unavailable",
                "questions": [
                    "The SQL generator is temporarily unavailable. Please contact an administrator or try again later.",
                ],
                "rationale": "Base LLM disabled",
            }

        planner = PlannerAgent(self.llm)
        sql_text, rationale = planner.plan(question, context, hints)
        raw_candidate = extract_sql(sql_text) or sql_text.strip()
        dialect = str(context.get("dialect") or "generic")
        sql_candidate = extract_sql_one_stmt(sql_text, dialect=dialect)

        if not sql_candidate:
            return {
                "status": "needs_clarification",
                "questions": planner.fallback_clarifying_question(question, context, hints),
                "rationale": rationale,
                "raw_sql": raw_candidate,
                "error": "empty_or_invalid_sql_after_sanitize",
            }

        return {
            "status": "ok",
            "sql": sql_candidate,
            "rationale": rationale,
            "raw_sql": raw_candidate,
        }

    # ------------------------------------------------------------------
    def _execute_sql(self, engine, sql_text: str) -> SQLExecutionResult:
        dialect = str(getattr(getattr(engine, "dialect", None), "name", "generic"))
        cleaned = extract_sql_one_stmt(sql_text, dialect=dialect)
        if not cleaned:
            raise RuntimeError("empty_or_invalid_sql_after_sanitize")
        result = run_sql(engine, cleaned)
        if not result.ok:
            raise RuntimeError(result.error or "SQL execution failed")
        return result

    # ------------------------------------------------------------------
    def _maybe_autoretry(
        self,
        question: str,
        context: Dict[str, Any],
        hints: Dict[str, Any],
        engine,
    ) -> Optional[Dict[str, Any]]:
        enabled = bool(self.settings.get_bool("EMPTY_RESULT_AUTORETRY", default=False))
        if not enabled:
            return None

        widen_days = self.settings.get_int("EMPTY_RESULT_AUTORETRY_DAYS", default=90) or 90
        retry_question = (
            f"{question}\n\nIf no rows are returned, widen the window to roughly the last {widen_days} days."
        )
        retry_hints = dict(hints)
        retry_hints["autoretry_days"] = widen_days

        retry_plan = self._plan_sql(retry_question, context, retry_hints)
        if retry_plan.get("status") != "ok" or not retry_plan.get("sql"):
            return None

        try:
            retry_result = self._execute_sql(engine, retry_plan["sql"])
        except Exception:
            return None

        if retry_result.rowcount <= 0:
            return None

        return {
            "sql": retry_plan["sql"],
            "result": retry_result,
            "rationale": retry_plan.get("rationale"),
            "raw_sql": retry_plan.get("raw_sql"),
        }

    # ------------------------------------------------------------------
    def _record_run(
        self,
        *,
        namespace: str,
        question: str,
        sql: str,
        rows: List[Dict[str, Any]],
        datasource: Optional[str],
        auth_email: Optional[str],
    ) -> Optional[int]:
        sample_json = json.dumps(rows[:5], default=str)
        attempts = [
            (
                """
                INSERT INTO mem_runs(namespace, run_type, datasource, input_query, sql_text, row_count, result_sample)
                VALUES (:ns, :rtype, :ds, :query, :sql, :count, CAST(:sample AS jsonb))
                RETURNING id
                """.strip(),
                {
                    "ns": namespace,
                    "rtype": "dw_pipeline",
                    "ds": datasource,
                    "query": question,
                    "sql": sql,
                    "count": len(rows),
                    "sample": sample_json,
                },
            ),
            (
                """
                INSERT INTO mem_runs(namespace, run_type, input_query, sql_text, row_count, result_sample)
                VALUES (:ns, :rtype, :query, :sql, :count, CAST(:sample AS jsonb))
                RETURNING id
                """.strip(),
                {
                    "ns": namespace,
                    "rtype": "dw_pipeline",
                    "query": question,
                    "sql": sql,
                    "count": len(rows),
                    "sample": sample_json,
                },
            ),
            (
                """
                INSERT INTO mem_runs(namespace, input_query, sql_text, row_count)
                VALUES (:ns, :query, :sql, :count)
                RETURNING id
                """.strip(),
                {
                    "ns": namespace,
                    "query": question,
                    "sql": sql,
                    "count": len(rows),
                },
            ),
            (
                """
                INSERT INTO mem_runs(namespace, input_query, sql_text)
                VALUES (:ns, :query, :sql)
                RETURNING id
                """.strip(),
                {
                    "ns": namespace,
                    "query": question,
                    "sql": sql,
                },
            ),
        ]

        for stmt_text, params in attempts:
            try:
                with self.mem_engine.begin() as conn:
                    row = conn.execute(text(stmt_text), params).fetchone()
                if row and row[0] is not None:
                    return int(row[0])
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    def _friendly_empty_hint(self) -> str:
        return (
            "No results returned. Try widening the date range or removing filters such as "
            "department or stakeholder."
        )

    # ------------------------------------------------------------------
    def _build_tags(
        self, app_tag: Optional[str], prefixes: Sequence[str]
    ) -> List[str]:
        tags = [self.active_app]
        if app_tag:
            tags.append(app_tag)
        tags.extend(f"prefix:{p}" for p in prefixes if p)
        return sorted({t for t in tags if t})
