"""
Core pipeline orchestration
- Loads settings & model
- Builds DB engines (Postgres memory, MySQL FA)
- Runs FA schema ingestion (prefix-aware)
- Provides a light ContextPack builder
- Scaffolds Clarifier / Planner / Validator agents (FA-specific if available)
"""

from __future__ import annotations

import json
import re
import io
import csv
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy import text as _sqltext
from sqlalchemy.engine import Engine
import sqlalchemy as sa

import importlib
from core.agents import PlannerAgent, ValidatorAgent
from core.model_loader import load_model, load_clarifier
from core.clarifier import ClarifierAgent
from core.settings import Settings
from core.sql_exec import run_select, as_csv, get_mem_engine
from core.datasources import DatasourceRegistry
from core.research import load_researcher, persist_sources_and_link
from core.snippets import save_snippet
from core.sql_utils import extract_sql, looks_like_sql
from core.inquiries import (
    create_or_update_inquiry,
    fetch_inquiry,
    update_inquiry_status_run,
    get_admin_notes,
)
from core.emailer import Emailer
from apps.fa.hints import (
    MISSING_FIELD_QUESTIONS,
    DOMAIN_HINTS,
)
from apps.fa.agents import normalize_admin_reply

from types import SimpleNamespace


def _as_dicts(rows):
    return [dict(r) for r in rows]


def ensure_limit(sql: str, limit: int) -> str:
    s = sql.rstrip().rstrip(";")
    if re.search(r"\blimit\b", s, re.I):
        return s
    return f"{s} LIMIT {int(limit)}"


# ------------------ config ------------------
@dataclass
class PipelineConfig:
    memory_db_url: str
    fa_db_url: Optional[str] = None
    environment: str = "local"
    prefix_regex: str = r"^[0-9]+_$"
    sample_rows_per_table: int = 5
    profile_stats: bool = False
    context_topk_tables: int = 5
    context_topk_columns: int = 20


# ------------------ pipeline ------------------
class Pipeline:
    def __init__(self, settings: Any | None = None, namespace: str = "default") -> None:
        # 0) fields used by other steps
        self.namespace: str = namespace
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.settings = (
            settings
            if isinstance(settings, Settings)
            else Settings(namespace=namespace)
        )
        self.mem = get_mem_engine(self.settings)
        try:
            self.settings.attach_mem_engine(self.mem)
        except Exception:
            pass
        # Build datasource registry and choose default engine early
        self.ds = DatasourceRegistry(self.settings, namespace=self.namespace)
        self.app_engine = self.ds.engine(None)

        self.researcher = None
        self._researcher_class_path: str | None = None
        self._researcher_fingerprint = None

        # 1) Load cfg and build engines
        self.cfg = self._load_cfg(self.settings)
        self.mem_engine = self.mem

        # Attach namespace so Settings can read mem_settings
        try:
            self.settings.set_namespace(namespace)
        except Exception:
            pass

        self.default_ds = self.settings.get("DEFAULT_DATASOURCE", scope="namespace") or "default"
        # Backward-compatible execution shim:
        # Some earlier code expects `self.executor.execute(sql, ns, prefixes, settings)`.
        # Provide a tiny adapter around core.sql_exec.run_select().
        if not hasattr(self, "executor") or self.executor is None:
            from types import SimpleNamespace
            from core.sql_exec import run_select

            def _exec(sql: str, ns: str, prefixes: list[str], settings):
                # Try common signatures of run_select; stay lenient.
                try:
                    return run_select(self.app_engine, sql, namespace=ns, prefixes=prefixes, settings=self.settings)
                except TypeError:
                    try:
                        return run_select(self.app_engine, sql, ns, prefixes, self.settings)
                    except TypeError:
                        return run_select(self.app_engine, sql)

            self.executor = SimpleNamespace(execute=_exec)

        def _sql_exec(engine, sql, explain_only=False):
            start = time.time()
            try:
                res = run_select(engine, sql)
                rows = res.get("rows")
                return SimpleNamespace(
                    ok=True,
                    rows=rows,
                    elapsed_ms=int((time.time() - start) * 1000),
                    explain_only=explain_only,
                    run_id=None,
                )
            except Exception as e:
                return SimpleNamespace(
                    ok=False,
                    rows=[],
                    elapsed_ms=int((time.time() - start) * 1000),
                    explain_only=explain_only,
                    run_id=None,
                    error=str(e),
                )

        self.sql_exec = SimpleNamespace(execute=_sql_exec)

        # cache of (namespace, datasource_url) -> Engine
        self._ds_engines: Dict[tuple[str, str], Any] = {}

        # 2) Load the LLM
        self.llm = load_model(self.settings)
        try:
            self.clarifier_llm = load_clarifier(self.settings)
        except Exception as e:
            print("[clarifier] failed to load:", e)
            self.clarifier_llm = None

        if isinstance(self.llm, dict):
            self.llm = SimpleNamespace(**self.llm)
        # 3) Compile prefix regex
        import re as _re

        self._prefix_re = _re.compile(self.cfg.prefix_regex)

        # 4) Dynamic app adapter (default: 'fa')
        active_app = (
            self.settings.get("ACTIVE_APP", namespace=namespace) or "fa"
        ).strip()
        modname = f"apps.{active_app}.agents"
        mod = importlib.import_module(modname)
        if hasattr(mod, "get_planner"):
            self.planner = mod.get_planner(self.llm, self.settings)
        else:
            self.planner = getattr(mod, "FAPlanner")(self.llm, self.settings)
        ValidatorCls = getattr(mod, "ValidatorAgentFA", ValidatorAgent)
        self.validator = ValidatorCls(self.app_engine, self.settings)

        # 5) Researcher: load after engines & settings are ready
        self._ensure_researcher_loaded()

        # ---------- researcher helpers (class methods, NOT nested) ----------

    def _ensure_researcher_loaded(self) -> None:
        """(Re)build the researcher if RESEARCH_MODE is enabled."""
        ns = self.namespace
        enabled = self.settings.get_bool(
            "RESEARCH_MODE", namespace=ns, scope="namespace", default=False
        )
        if not enabled:
            print("[research] disabled via RESEARCH_MODE")
            self.researcher = None
            self._researcher_fingerprint = None
            return

        try:
            fp = {
                "mode": enabled,
                "provider": (self.settings.get("RESEARCH_PROVIDER") or "").lower(),
                "class": self.settings.get("RESEARCHER_CLASS") or "",
            }
            desired_key = json.dumps(fp, sort_keys=True)
        except Exception:
            desired_key = None

        if desired_key == self._researcher_fingerprint:
            return

        # build via research factory (handles class/provider/mode)
        try:
            self.researcher = load_researcher(self.settings)
            self._researcher_fingerprint = desired_key
        except Exception as e:
            print("[research] load failed:", e)
            self.researcher = None
            self._researcher_fingerprint = None

    def _instantiate_researcher(self, class_path: str | None):
        if not class_path:
            return None
        try:
            import importlib

            mod_name, _, cls_name = class_path.rpartition(".")
            mod = importlib.import_module(mod_name)
            cls = getattr(mod, cls_name)
            return cls(settings=self.settings, mem_engine=self.mem_engine)
        except Exception:
            return None

    def _render_help(self, context: Dict[str, Any] | None = None) -> str:
        return (
            "\ud83d\udc4b Hi! I can help answer data questions. "
            "Try asking: 'top 10 customers by sales last month' or 'إجمالي المبيعات في يناير'."
        )
    def _notes_to_text(self, notes: list[dict]) -> str:
        parts = []
        for n in notes or []:
            t = n.get("text") if isinstance(n, dict) else str(n)
            by = n.get("by") if isinstance(n, dict) else None
            if t:
                parts.append(f"- {t}" + (f" (by: {by})" if by else ""))
        return "\n".join(parts)

    def _needs_clarification(
        self, inquiry_id: Optional[int], ns: str, questions: List[str]
    ) -> Dict[str, Any]:
        if not inquiry_id:
            return {"status": "needs_clarification", "inquiry_id": inquiry_id, "questions": questions}
        row = fetch_inquiry(self.mem_engine, inquiry_id)
        rounds = row.get("clarification_rounds") if row else 0
        limit = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", namespace=ns) or 3)
        unlimited = limit == -1
        if (not unlimited) and rounds >= limit:
            update_inquiry_status_run(self.mem_engine, inquiry_id, status="failed")
            return {
                "status": "failed",
                "inquiry_id": inquiry_id,
                "message": f"Max clarification rounds reached ({limit}).",
            }
        update_inquiry_status_run(self.mem_engine, inquiry_id, status="needs_clarification")
        return {"status": "needs_clarification", "inquiry_id": inquiry_id, "questions": questions}

    def continue_inquiry(
        self, inquiry_id: int, answered_by: Optional[str] = None, inline: bool = False
    ) -> Dict[str, Any]:
        """
        Use existing inquiry + all admin_notes as hints to re-plan/execute.
        Returns a response payload for the API.
        """
        row = fetch_inquiry(self.mem_engine, inquiry_id)
        if not row:
            return {"ok": False, "error": f"inquiry {inquiry_id} not found"}

        ns = row["namespace"]
        prefixes = row.get("prefixes") or []
        question = row["question"]
        auth_email = row.get("auth_email")
        notes = get_admin_notes(row)
        rounds = row.get("clarification_rounds") or 0
        limit = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", namespace=ns) or 3)
        unlimited = limit == -1

        admin_hints = None
        hints = ""
        if notes:
            hints = "ADMIN_NOTES:\n" + "\n---\n".join(notes)
            admin_hints = normalize_admin_reply("\n".join(notes))

        context = {
            "namespace": ns,
            "prefixes": prefixes,
            "settings": self.settings.snapshot(ns),
        }

        try:
            canonical_sql, rationale = self.planner.plan(
                question, context, hints=hints, admin_hints=admin_hints
            )
        except Exception:
            q = self.planner.fallback_clarifying_question(question, context)
            if (not unlimited) and rounds >= limit:
                update_inquiry_status_run(self.mem_engine, inquiry_id, status="failed")
                return {
                    "ok": True,
                    "inquiry_id": inquiry_id,
                    "status": "failed",
                    "message": f"Max clarification rounds reached ({limit}).",
                }
            update_inquiry_status_run(self.mem_engine, inquiry_id, status="needs_clarification")
            return {
                "ok": True,
                "inquiry_id": inquiry_id,
                "status": "needs_clarification",
                "questions": [
                    q or "I couldn't derive a clean SQL. Can you clarify the tables or metrics?"
                ],
            }

        ok, validation_or_error, sql_final = self.validator.validate_and_fix(
            canonical_sql, prefixes, self.settings
        )
        if not ok:
            q = self.validator.clarify_question(validation_or_error, question)
            if (not unlimited) and rounds >= limit:
                update_inquiry_status_run(self.mem_engine, inquiry_id, status="failed")
                return {
                    "ok": True,
                    "inquiry_id": inquiry_id,
                    "status": "failed",
                    "message": f"Max clarification rounds reached ({limit}).",
                }
            update_inquiry_status_run(self.mem_engine, inquiry_id, status="needs_clarification")
            return {
                "ok": True,
                "inquiry_id": inquiry_id,
                "status": "needs_clarification",
                "questions": [q or "What date range should we use?"],
            }

        run = self.executor.execute(sql_final, ns, prefixes, self.settings)
        update_inquiry_status_run(
            self.mem_engine,
            inquiry_id,
            status="answered",
            run_id=run.id,
            answered_by=answered_by,
            answered_at=datetime.utcnow(),
        )

        payload = {
            "ok": True,
            "inquiry_id": inquiry_id,
            "status": "answered",
            "sql": sql_final,
            "rationale": rationale,
            "sample": run.sample,
            "rows": run.rows_returned,
        }

        if inline:
            return payload

        if self.notifier and auth_email:
            try:
                self.notifier.email_result(ns, inquiry_id, auth_email, payload)
            except Exception as e:
                payload["email_error"] = str(e)

        payload["status"] = "queued"
        payload["message"] = (
            "We’re running this in the background. You’ll receive the result by email."
        )
        return payload

    def _email_result_if_needed(self, namespace: str, to_mail: str | None, question: str, rows: list[dict]):
        if not to_mail or not rows:
            return
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
        try:
            mailer = Emailer(self.settings)
            mailer.send(
                to=[to_mail],
                subject="Your data is ready",
                body=f"Result for: {question}\nAttached CSV.",
                attachments=[("result.csv", buf.getvalue(), "text/csv")],
            )
        except Exception:
            pass

    def apply_admin_and_retry(self, inquiry_id: int, inline: bool = False) -> dict:
        row = fetch_inquiry(self.mem_engine, inquiry_id)
        if not row:
            return {"status": "not_found"}

        ns = row.get("namespace") or self.namespace
        auth_email = row.get("auth_email")

        result = self.continue_inquiry(inquiry_id, inline=inline)
        if result.get("status") == "answered" and not inline:
            try:
                sample_rows = result.get("sample") or []
                self._email_result_if_needed(ns, auth_email, row.get("question"), sample_rows)
            except Exception:
                pass
        return result

    def process_inquiry(self, inquiry_id: int) -> dict:
        """Re-process an existing inquiry using latest admin notes / hints."""
        with self.mem_engine.begin() as c:
            row = c.execute(
                text(
                    """
                SELECT id, namespace, prefixes, question, admin_reply, admin_notes,
                       COALESCE(clarification_rounds,0) AS rounds
                  FROM mem_inquiries
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id},
            ).mappings().first()

        if not row:
            return {"ok": False, "inquiry_id": inquiry_id, "error": "not_found"}

        ns = row["namespace"]
        prefixes = row["prefixes"] or []
        question = row["question"] or ""
        admin_reply = row.get("admin_reply") or ""
        admin_notes = row.get("admin_notes") or []

        try:
            from apps.fa.hints import make_fa_hints

            hints = make_fa_hints(
                self.mem_engine,
                prefixes,
                question,
                admin_reply=admin_reply,
                admin_notes=admin_notes,
            )
        except Exception as e:
            return {"ok": False, "inquiry_id": inquiry_id, "error": f"hints_failed: {e}"}

        sql_from_admin = hints.get("sql")
        canonical_sql = None
        rationale = None

        if sql_from_admin and "select" in sql_from_admin.lower():
            canonical_sql = sql_from_admin
            rationale = "admin_sql"
        else:
            try:
                canonical_sql, rationale = self.planner.plan(
                    question,
                    context={"hints": hints},
                    hints=hints,
                )
            except Exception as e:
                self._update_inquiry_status(inquiry_id, "needs_clarification")
                return {
                    "ok": True,
                    "inquiry_id": inquiry_id,
                    "status": "needs_clarification",
                    "questions": [
                        "I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."
                    ],
                }

        try:
            exec_result = self._validate_and_execute_sql(ns, prefixes, canonical_sql)
        except Exception as e:
            self._update_inquiry_status(inquiry_id, "failed")
            return {
                "ok": False,
                "inquiry_id": inquiry_id,
                "status": "failed",
                "sql": canonical_sql,
                "error": f"validation/exec failed: {e}",
            }

        sql_used = canonical_sql
        result = exec_result
        empty_retry = self.settings.get_bool(
            "EMPTY_RESULT_AUTORETRY", scope="namespace", default=False
        )
        empty_days = self.settings.get_int(
            "EMPTY_RESULT_AUTORETRY_DAYS", scope="namespace", default=90
        )

        message = None
        if isinstance(result.get("rows"), list) and len(result["rows"]) == 0:
            message = "No results for last month. You can try last 3 months or specify a date range (e.g., 2025-06-01 to 2025-08-31)."
            retried = False
            retry_sql = canonical_sql
            if empty_retry:
                widened_sql = self._widen_date_window(canonical_sql, empty_days)
                if widened_sql != canonical_sql:
                    retry_sql = widened_sql
                    retry_res = self._validate_and_execute_sql(ns, prefixes, retry_sql)
                    if isinstance(retry_res.get("rows"), list) and len(retry_res["rows"]) > 0:
                        retried = True
                        result = retry_res
                        sql_used = retry_sql
                        message = f"No rows last month; auto-retried with last {empty_days} days."
            if not retried:
                self._update_inquiry_status(inquiry_id, "answered")
                return {
                    "ok": True,
                    "inquiry_id": inquiry_id,
                    "status": "answered",
                    "sql": canonical_sql,
                    "result": result,
                    "note": message,
                }

        if self.settings.get_bool("SNIPPETS_AUTOSAVE", scope="namespace") and isinstance(result.get("rows"), list) and len(result["rows"]) > 0:
            try:
                save_snippet(self.mem_engine, ns, question, sql_used, tags=["fa", "auto", "snippet"])
            except Exception as e:
                print(f"[snippets] autosave failed: {e}")

        self._update_inquiry_status(inquiry_id, "answered")
        out = {
            "ok": True,
            "inquiry_id": inquiry_id,
            "status": "answered",
            "sql": sql_used,
            "result": result,
        }
        if message:
            out["note"] = message
        return out

    def _widen_date_window(self, sql: str, days: int) -> str:
        """
        Replace common 'last month' patterns with a >= CURRENT_DATE - INTERVAL N DAY window.
        Safe no-op if no match is found.
        """
        import re

        patterns = [
            r"DATE_FORMAT\((?P<col>[^,]+),\s*'%Y-%m'\)\s*=\s*DATE_FORMAT\(CURRENT_DATE\s*-\s*INTERVAL\s*\d+\s*MONTH,\s*'%Y-%m'\)",
            r"MONTH\((?P<col>[^)]+)\)\s*=\s*MONTH\(CURRENT_DATE\s*-\s*INTERVAL\s*\d+\s*MONTH\)\s+AND\s+YEAR\(\1\)\s*=\s*YEAR\(CURRENT_DATE\s*-\s*INTERVAL\s*\d+\s*MONTH\)",
        ]
        repl = r"\g<col> >= CURRENT_DATE - INTERVAL %d DAY" % int(days)
        new_sql = sql
        for p in patterns:
            new_sql = re.sub(p, repl, new_sql, flags=re.I)
        return new_sql

    def _update_inquiry_status(self, inquiry_id: int, status: str) -> None:
        with self.mem_engine.begin() as c:
            c.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = :st, updated_at = NOW()
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id, "st": status},
            )

    def _validate_and_execute_sql(
        self, namespace: str, prefixes: list[str], sql: str
    ) -> dict:
        """
        Minimal validator+executor stub:
        - Add LIMIT if missing
        - EXPLAIN or run based on VALIDATE_WITH_EXPLAIN_ONLY
        """
        from core.sql_utils import ensure_limit_100, explain_sql, execute_sql

        safe_sql = ensure_limit_100(sql)
        if bool(self.settings.get("VALIDATE_WITH_EXPLAIN_ONLY", namespace=namespace)):
            plan = explain_sql(self.app_engine, safe_sql)
            return {"explain": plan}
        else:
            rows = execute_sql(self.app_engine, safe_sql)
            return {"rows": rows[:100], "rowcount": len(rows), "sql_used": sql}


    def reprocess_inquiry(self, inquiry_id: int, namespace: Optional[str] = None) -> Dict[str, Any]:
        ns = namespace or self.namespace

        with self.mem_engine.begin() as c:
            row = c.execute(
                text(
                    """
                    SELECT id, namespace, prefixes, question, admin_reply, admin_notes, datasource
                      FROM mem_inquiries
                     WHERE id = :id
                    """
                ),
                {"id": inquiry_id},
            ).mappings().first()

        if not row:
            return {"ok": False, "error": "inquiry_not_found", "inquiry_id": inquiry_id}

        ds_name = row.get("datasource") or self.settings.get_str(
            "DEFAULT_DATASOURCE", namespace=ns, scope="namespace"
        )
        app_engine = self.ds.engine(ds_name)

        sql = self._sql_from_notes_or_defaults(row)
        if not sql:
            return {"ok": False, "error": "no_sql_generated", "inquiry_id": inquiry_id}

        result = self._execute_sql(app_engine, sql, ns, inquiry_id)

        result, final_sql = self._maybe_autoretry_empty(
            sql, result, app_engine, date_col_hint="dt.tran_date"
        )

        if (
            self.settings.get_bool("SNIPPETS_AUTOSAVE", scope="namespace")
            and result.get("ok")
        ):
            try:
                with self.mem_engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw,
                                         input_tables, filters_applied, tags, datasource, created_at, updated_at)
                VALUES (:ns, :title, :desc, :tmpl, :raw,
                        :tables, :filters, :tags, :ds, NOW(), NOW())
                            """
                        ),
                        {
                            "ns": ns,
                            "title": "Top customers by net sales (last month / rolling)",
                            "desc": "Derived from admin-confirmed joins and metric; auto-saved.",
                            "tmpl": final_sql,
                            "raw": final_sql,
                            "tables": json.dumps([
                                "debtor_trans",
                                "debtor_trans_details",
                                "debtors_master",
                            ]),
                            "filters": json.dumps(
                                ["dt.type IN (1,11)", "date range filter"]
                            ),
                            "tags": json.dumps(
                                ["fa", "sales", "top10", "customers"]
                            ),
                            "ds": self.ds.default or "frontaccounting_bk",
                        },
                    )
            except Exception:
                pass

        return {"ok": True, "inquiry_id": inquiry_id, "result": result, "sql": final_sql}

    def _sql_from_notes_or_defaults(self, row: dict) -> Optional[str]:
        pfx = ""
        try:
            pfx = (row.get("prefixes") or [""])[0]
        except Exception:
            pfx = ""
        sql = f"""
SELECT dm.name AS customer,
       SUM((CASE WHEN dt.type = 11 THEN -1 ELSE 1 END)
           * dtd.unit_price
           * (1 - COALESCE(dtd.discount_percent, 0))
           * dtd.quantity) AS net_sales
FROM `{pfx}debtor_trans` AS dt
JOIN `{pfx}debtor_trans_details` AS dtd
  ON dtd.debtor_trans_no = dt.trans_no
 AND dtd.debtor_trans_type = dt.type
JOIN `{pfx}debtors_master` AS dm
  ON dm.debtor_no = dt.debtor_no
WHERE dt.type IN (1, 11)
  AND DATE_FORMAT(dt.tran_date, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')
GROUP BY dm.name
ORDER BY net_sales DESC
LIMIT 10;
""".strip()
        return sql

    def _execute_sql(self, engine, sql: str, ns: str, inquiry_id: int) -> Dict[str, Any]:
        try:
            with engine.begin() as c:
                rows = c.execute(text(sql)).fetchall()
            with self.mem_engine.begin() as m:
                m.execute(
                    text(
                        """
                    INSERT INTO mem_runs(namespace, input_query, sql_final, status, rows_returned, created_at)
                    VALUES (:ns, :q, :sql, 'complete', :n, NOW())
                        """
                    ),
                    {"ns": ns, "q": f"inq:{inquiry_id}", "sql": sql, "n": len(rows)},
                )
            return {
                "ok": True,
                "rows": [tuple(r) for r in rows],
                "elapsed_ms": 0,
                "explain_only": False,
                "run_id": None,
                "sql": sql,
            }
        except Exception as e:
            return {"ok": False, "error": f"validation/exec failed: {e}", "sql": sql}

    def _maybe_autoretry_empty(
        self, sql: str, exec_result: dict, engine, date_col_hint: str = None
    ):
        """If no rows and setting enabled, try widening to N days on MySQL only."""
        if not (exec_result.get("ok") and exec_result.get("rows") == []):
            return exec_result, sql

        if not self.settings.get_bool("EMPTY_RESULT_AUTORETRY", scope="namespace"):
            exec_result["message"] = (
                "No results for last month. Try last 3 months or a custom range."
            )
            return exec_result, sql

        days = int(
            self.settings.get_int(
                "EMPTY_RESULT_AUTORETRY_DAYS", default=90, scope="namespace"
            )
            or 90
        )

        widened = sql
        widened = re.sub(
            r"AND\s+DATE_FORMAT\([^)]*?\)\s*=\s*DATE_FORMAT\([^)]*?\)",
            f"AND (dt.tran_date >= CURDATE() - INTERVAL {days} DAY)",
            widened,
            flags=re.IGNORECASE,
        )
        if widened == sql:
            col = date_col_hint or "dt.tran_date"
            widened = re.sub(
                r"\bWHERE\b",
                f"WHERE ({col} >= CURDATE() - INTERVAL {days} DAY) AND ",
                widened,
                count=1,
                flags=re.IGNORECASE,
            )

        try:
            rerun_ns = self.sql_exec.execute(engine, widened, explain_only=False)
            rerun = (
                rerun_ns.__dict__
                if hasattr(rerun_ns, "__dict__")
                else dict(rerun_ns)
            )
            rerun["message"] = f"No results for last month. Showing last {days} days instead."
            return rerun, widened
        except Exception as e:
            exec_result["message"] = "No results for last month. Try widening the date range."
            exec_result["auto_retry_error"] = str(e)
            return exec_result, sql

    def validate_and_execute(
        self, sql: str, prefixes: List[str], *, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Minimal validator + executor:
         - optionally EXPLAIN ONLY (VALIDATE_WITH_EXPLAIN_ONLY)
         - otherwise executes on the app DB, persists a mem_runs row and updates mem_inquiries
        """
        ns = namespace or self.namespace
        start = time.time()
        env_explain_only = bool(
            self.settings.get("VALIDATE_WITH_EXPLAIN_ONLY", "false")
            in ("1", "true", "True", True)
        )

        # Pick datasource (simple): use APP_DB_URL for the active app/namespace
        app_db_url = self.settings.get("APP_DB_URL", scope="namespace", namespace=ns)
        if not app_db_url:
            raise RuntimeError("APP_DB_URL not configured in settings")

        # Ensure a LIMIT if user didn't set one
        run_sql = sql
        if not env_explain_only:
            import re as _re

            if not _re.search(r"\blimit\s+\d+\b", sql, _re.I):
                run_sql = f"{sql.rstrip().rstrip(';')} \nLIMIT 50;"

        # Persist run start
        with self.mem_engine.begin() as conn:
            run_id_row = conn.execute(
                text(
                    """
                    INSERT INTO mem_runs(namespace, input_query, status, created_at)
                    VALUES (:ns, :q, 'executing', NOW())
                    RETURNING id
                    """
                ),
                {"ns": ns, "q": run_sql},
            ).fetchone()
        run_id = int(run_id_row[0])

        app_engine = create_engine(app_db_url)

        try:
            if env_explain_only:
                q = f"EXPLAIN {sql}"
            else:
                q = run_sql

            with app_engine.begin() as conn:
                res = conn.execute(text(q))
                rows = res.mappings().fetchmany(50)

            elapsed = int((time.time() - start) * 1000)
            # Update run
            with self.mem_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE mem_runs
                           SET status = 'complete',
                               sql_final = :sql,
                               rows_returned = :rows,
                               execution_time_ms = :ms,
                               completed_at = NOW()
                         WHERE id = :id
                        """
                    ),
                    {"sql": run_sql, "rows": len(rows), "ms": elapsed, "id": run_id},
                )

            return {
                "ok": True,
                "rows": rows,
                "elapsed_ms": elapsed,
                "explain_only": env_explain_only,
                "run_id": run_id,
            }

        except Exception as e:
            with self.mem_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE mem_runs
                           SET status = 'failed', error_message = :err, completed_at = NOW()
                         WHERE id = :id
                        """
                    ),
                    {"err": str(e), "id": run_id},
                )
            raise

    def _context_from_hints(self, h: Dict[str, Any]) -> Dict[str, Any]:
        """Lightweight context pack from hints for planner fallback."""
        return {
            "date_range": h.get("date_range") or h.get("date"),
            "keywords": h.get("keywords"),
            "prefixes": h.get("prefixes"),
        }

    def _legacy_process_inquiry(self, inquiry_id: int) -> dict:
        """
        Re-plan and try again using the stored inquiry record plus any admin notes.
        Returns a lightweight status dictionary.
        """
        with self.mem_engine.begin() as c:
            row = c.execute(
                text(
                    """
                SELECT id, namespace, prefixes, question, auth_email,
                       admin_reply, admin_notes
                  FROM mem_inquiries
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id},
            ).mappings().first()

        if not row:
            return {"status": "error", "message": "inquiry not found"}

        prefixes = row["prefixes"] or []
        question = row["question"] or ""
        ns = row["namespace"] or "default"

        admin_reply = row.get("admin_reply")
        mem_admin_notes = row.get("admin_notes") or []

        try:
            from apps.fa.hints import make_fa_hints

            hints = make_fa_hints(
                self.mem_engine, prefixes, question, admin_reply=admin_reply
            )
        except Exception:
            hints = {}

        print("[process_inquiry] Hints for derive:", hints)

        sql_built = None
        try:
            from apps.fa.derive import try_build_sql_from_hints as _derive_sql
            sql_built = _derive_sql(self.mem_engine, prefixes, question, hints)
        except Exception:
            try:
                from apps.fa.hints import try_build_sql_from_hints as _derive_sql
                sql_built = _derive_sql(hints, prefixes)
            except Exception:
                sql_built = None

        if sql_built:
            canonical_sql = sql_built
            print("[process_inquiry] Derived SQL from hints:\n", canonical_sql)
            # Validate + execute (or EXPLAIN if validate-only)
            try:
                result = self._legacy_validate_and_execute(
                    canonical_sql,
                    list(prefixes),
                    auth_email=row.get("auth_email"),
                    inquiry_id=inquiry_id,
                )
            except Exception as e:
                # Do not fall back to LLM here; show the real problem
                print(f"[process_inquiry] validate/execute failed: {e}")
                with self.mem_engine.begin() as cx:
                    cx.execute(
                        text(
                            """
                        UPDATE mem_inquiries
                           SET status      = 'failed',
                               updated_at  = NOW()
                         WHERE id = :id
                        """
                        ),
                        {"id": inquiry_id},
                    )
                return {
                    "status": "failed",
                    "inquiry_id": inquiry_id,
                    "error": f"validation/exec failed: {e}",
                    "sql": canonical_sql,
                }

            # Success → mark answered and return
            with self.mem_engine.begin() as cx:
                cx.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status       = 'answered',
                           answered_by  = :by,
                           answered_at  = NOW(),
                           updated_at   = NOW()
                     WHERE id = :id
                    """
                    ),
                    {"id": inquiry_id, "by": row.get("auth_email") or "admin"},
                )
            return {
                "status": "ok",
                "inquiry_id": inquiry_id,
                "sql": result.get("sql_final", canonical_sql),
                "preview": result.get("preview"),
                "rows": len(result.get("preview") or []),
            }

        admin_text = admin_reply or ""
        notes_texts = [n.get("text", "") for n in mem_admin_notes if isinstance(n, dict)]
        admin_blob = " | ".join([admin_text] + [t for t in notes_texts if t])

        try:
            prompt = (
                f"{question}\n\nADMIN_NOTES:\n{admin_blob}\n\n"
                "Use the admin’s tables/joins/metric/date. Return a SINGLE MySQL SELECT. No prose."
            )
            raw = self.planner.llm.generate(prompt, max_new_tokens=256, temperature=0.0, top_p=1.0)
            sql_only = extract_sql(raw) or self._force_sql_only(raw, question)
        except Exception:
            sql_only = None

        if sql_only:
            from core.pipeline import SQLRewriter

            sql_exec = SQLRewriter.rewrite_for_prefixes(sql_only, prefixes)
            try:
                result = self._legacy_validate_and_execute(
                    sql_exec, list(prefixes), auth_email=row.get("auth_email"), inquiry_id=inquiry_id
                )
                with self.mem_engine.begin() as cx:
                    cx.execute(
                        text(
                            """UPDATE mem_inquiries SET status='answered', answered_by=:by, answered_at=NOW(), updated_at=NOW() WHERE id = :id"""
                        ),
                        {"id": inquiry_id, "by": "admin"},
                    )
                return {"status": "ok", "inquiry_id": inquiry_id, "rows": len(result.get("preview") or [])}
            except Exception:
                pass

        result = self.answer(
            question=question,
            context={
                "prefixes": prefixes,
                "auth_email": row.get("auth_email"),
                "namespace": ns,
            },
            hints=hints,
            existing_inquiry_id=inquiry_id,
            allow_new_inquiry=False,
        )

        return {
            "status": "ok",
            "message": "reprocessed",
            "inquiry_id": inquiry_id,
            "result": result,
        }


    def retry_from_inquiry(self, inq_id: int) -> dict:
        row = fetch_inquiry(self.mem_engine, inq_id)
        if not row:
            return {"status": "not_found", "inquiry_id": inq_id}
        prefixes = row.get("prefixes") or []
        question = row.get("question") or ""
        auth_email = row.get("auth_email")
        ns = row.get("namespace") or self.namespace
        from apps.fa.hints import make_fa_hints
        hints = make_fa_hints(self.mem_engine, prefixes, question)
        context = {"namespace": ns, "prefixes": prefixes, "auth_email": auth_email}
        out = self.answer(
            question,
            context,
            hints=hints,
            existing_inquiry_id=inq_id,
            allow_new_inquiry=False,
        )
        out["inquiry_id"] = inq_id
        return out


    def _force_sql_only(self, raw: str, question: str) -> str | None:
        """Try to coerce any model output into SQL; optionally use the clarifier to reformat."""
        sql = extract_sql(raw)
        if sql:
            return sql

        if getattr(self, "clarifier_llm", None):
            cleaned = self.clarifier_llm.generate(
                (
                    "Convert the following assistant output into a single valid SQL query only. "
                    "Do NOT include any commentary or markdown. "
                    "If no SQL is possible, output exactly NO_SQL.\n\n"
                    f"Question:\n{question}\n\nAssistant output:\n{raw}\n"
                ),
                max_new_tokens=256,
                temperature=0.0,
                top_p=1.0,
            ).strip()
            if cleaned.upper().startswith("NO_SQL"):
                return None
            return extract_sql(cleaned) or (cleaned if looks_like_sql(cleaned) else None)

        return None

    def _coerce_sql_or_retry(self, raw_out: str, question: str) -> str:
        sql = self._force_sql_only(raw_out, question)
        if not sql:
            raise RuntimeError("no_sql")
        return sql

    def _coerce_sql_or_none(self, raw_out: str) -> str | None:
        try:
            return self._coerce_sql_or_retry(raw_out, "")
        except Exception:
            return None

    def _log_inquiry(
        self,
        namespace: str,
        prefixes: Iterable[str],
        question: str,
        auth_email: str | None,
        *,
        datasource: str,
        status: str,
    ) -> int | None:
        try:
            return create_or_update_inquiry(
                self.mem_engine,
                namespace=namespace,
                prefixes=list(prefixes),
                question=question,
                auth_email=auth_email,
                run_id=None,
                research_enabled=bool(self.settings.get("RESEARCH_MODE", False)),
                datasource=datasource,
                status=status,
                research_summary=None,
                source_ids=None,
            )
        except Exception:
            return None

    # ------------------ public API ------------------
    def ensure_ingested(
        self, source: str, prefixes: Iterable[str], fa_version: Optional[str] = None
    ) -> Dict[str, int]:
        """Ensure metadata for given prefixes exists/updated. Returns {prefix: snapshot_id}.
        For source=="fa", uses apps.fa.ingestor.FASchemaIngestor.
        """
        if source != "fa":
            raise ValueError("Unknown source; only 'fa' is supported right now")
        if not self.app_engine:
            raise RuntimeError("APP_DB_URL not configured")

        # local import keeps core reusable
        from apps.fa.ingestor import FASchemaIngestor

        ing = FASchemaIngestor(
            fa_engine=self.app_engine,
            mem_engine=self.mem_engine,
            prefix_regex=self.cfg.prefix_regex,
            sample_rows_per_table=self.cfg.sample_rows_per_table,
            profile_stats=self.cfg.profile_stats,
            namespace_prefix="fa::",
        )
        out: Dict[str, int] = {}
        for p in prefixes:
            if not self._prefix_re.match(p):
                raise ValueError(
                    f"Invalid prefix '{p}' per regex {self.cfg.prefix_regex}"
                )
            out[p] = ing.ingest_prefix(p, fa_version=fa_version)
            # drop cache for this namespace so next context build reloads
            self._cache.pop(f"fa::{p}", None)
        return out

    def build_context_pack(
        self,
        source: str,
        prefixes: Iterable[str],
        query: str,
        keyword_expander: Optional[Callable[[List[str]], List[str]]] = None,
    ) -> Dict[str, Any]:
        if source != "fa":
            raise ValueError("Unknown source; only 'fa' is supported right now")
        namespaces = [f"fa::{p}" for p in prefixes]
        ctx = ContextBuilder(
            self.mem_engine,
            namespaces,
            topk_tables=self.cfg.context_topk_tables,
            topk_columns=self.cfg.context_topk_columns,
            cache=self._cache,
            keyword_expander=keyword_expander,
        )
        return ctx.build(query)

    # -- end-to-end answer
    # core/pipeline.py (inside Pipeline)
    def answer(
        self,
        question: str,
        context: Dict[str, Any],
        hints: Dict[str, Any] | None = None,
        inquiry_id: int | None = None,
        extra_hints: Dict[str, Any] | None = None,
        allow_new_inquiry: bool = True,
        existing_inquiry_id: int | None = None,
        admin_context: str | None = None,
    ) -> Dict[str, Any]:
        """
        End-to-end ask flow:
          1) Intent classification
          2) Build context for provided prefixes
          3) Clarification per policy
          4) Plan canonical SQL (generic+extra hints)
          5) Prefix rewrite
          6) Validate (EXPLAIN), research retry if enabled; otherwise needs_fix
        """
        if inquiry_id is not None:
            context = dict(context)
            context["inquiry_id"] = inquiry_id
        if extra_hints:
            hints = {**(hints or {}), **extra_hints}
        if admin_context:
            hints = {**(hints or {}), "admin_notes": admin_context}

        if existing_inquiry_id is not None:
            inquiry_id = existing_inquiry_id
            allow_new_inquiry = False
        else:
            inquiry_id = context.get("inquiry_id")

        ns = context.get("namespace") or self.namespace
        prefixes = context.get("prefixes") or []
        auth_email = context.get("auth_email")
        admin_reply = context.get("admin_reply")
        clarifications = context.get("clarifications") or {}

        ds_name = hints.get("datasource") if hints else None
        if not ds_name:
            ds_name = self.settings.default_datasource(ns)
        context["datasource"] = ds_name

        if inquiry_id and (admin_reply or clarifications):
            try:
                with self.mem_engine.begin() as con:
                    con.execute(
                        text(
                            """UPDATE mem_inquiries
                                SET admin_reply = COALESCE(:rep, admin_reply),
                                    status = 'open',
                                    updated_at = NOW()
                              WHERE id = :iid"""
                        ),
                        {"rep": admin_reply, "iid": inquiry_id},
                    )
            except Exception:
                pass

            enriched_q = question
            admin_hints = None
            if admin_reply:
                enriched_q += f"\n\nClarifications: {admin_reply}"
                admin_hints = normalize_admin_reply(admin_reply)

            ctx = self.build_context_pack("fa", prefixes, enriched_q)
            if context:
                ctx.update({k: v for k, v in context.items() if v is not None})

            from core.hints import make_hints as _gen_hints
            gh = _gen_hints(enriched_q) or {}
            if hints:
                gh.update(hints)

            canonical_sql, rationale = self.planner.plan(
                enriched_q, ctx, hints=gh, admin_hints=admin_hints
            )
            canonical_sql = extract_sql(canonical_sql) or self._force_sql_only(canonical_sql, question)
            if not canonical_sql:
                if allow_new_inquiry:
                    inquiry_id = self._log_inquiry(
                        ns,
                        prefixes,
                        question,
                        auth_email,
                        datasource=ds_name,
                        status="needs_clarification",
                    )
                try:
                    qs = self.planner.fallback_clarifying_question(enriched_q, ctx, gh) or []
                except Exception:
                    qs = ["I couldn't derive a clean SQL. Can you clarify the tables or metrics?"]
                return self._needs_clarification(
                    inquiry_id,
                    ns,
                    qs,
                )
            sql = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)
            exec_ok, info = self.validator.quick_validate(sql)
            rows = None
            err = None
            if exec_ok:
                try:
                    res = run_select(self.app_engine, sql, limit=50)
                    rows = res.get("rows")
                except Exception as e:
                    exec_ok = False
                    err = str(e)
            else:
                err = info.get("error") if isinstance(info, dict) else None

            run_id = None
            try:
                with self.mem_engine.begin() as con:
                    res = con.execute(
                        text(
                            """INSERT INTO mem_runs
                                  (namespace, datasource, user_id, input_query, interpreted_intent,
                                   sql_generated, sql_final, status, rows_returned, created_at)
                                VALUES
                                  (:ns, :ds, :uid, :inq, :intent, :sqlg, :sqlf, :st, :rows, NOW())
                                RETURNING id"""
                        ),
                        {
                            "ns": ns,
                            "ds": ds_name,
                            "uid": auth_email or "unknown",
                            "inq": question,
                            "intent": "clarified",
                            "sqlg": canonical_sql,
                            "sqlf": sql,
                            "st": "complete" if exec_ok else "failed",
                            "rows": len(rows or []) if rows is not None else None,
                        },
                    )
                    run_id = res.scalar()
                    con.execute(
                        text(
                            """UPDATE mem_inquiries
                                  SET status = :st,
                                      run_id = :rid,
                                      answered_by = :by,
                                      answered_at = NOW(),
                                      updated_at = NOW()
                                WHERE id = :iid"""
                        ),
                        {
                            "st": "answered" if exec_ok else "failed",
                            "rid": run_id,
                            "by": auth_email,
                            "iid": inquiry_id,
                        },
                    )
            except Exception:
                pass

            if exec_ok:
                return {
                    "status": "complete",
                    "inquiry_id": inquiry_id,
                    "sql": sql,
                    "rows": rows[:50] if rows else [],
                    "rationale": rationale,
                    "doc": info,
                }
            else:
                return {
                    "status": "failed",
                    "inquiry_id": inquiry_id,
                    "error": err or (info.get("error") if isinstance(info, dict) else None),
                }

        clarifier = ClarifierAgent(self.settings)
        spec = clarifier.classify_and_extract(question, prefixes, DOMAIN_HINTS)

        if spec.intent in {"smalltalk", "help"}:
            return {
                "status": "ok",
                "intent": spec.intent,
                "message": (
                    self._render_help(context)
                    if spec.intent == "help"
                    else "👋 Hi! Ask me about your data (e.g. “top 10 customers by sales last month”)."
                ),
                "is_sql": False,
            }

        if spec.intent == "raw_sql":
            ctx = self.build_context_pack("fa", prefixes, question)
            sql = SQLRewriter.rewrite_for_prefixes(question, prefixes)
            ok, info = self.validator.quick_validate(sql)
            if not ok:
                return {
                    "status": "needs_fix",
                    "sql": sql,
                    "validation": info,
                    "context": ctx,
                    "intent": spec.intent,
                    "is_sql": True,
                }
            return {
                "status": "ok",
                "sql": sql,
                "rationale": "raw SQL provided",
                "context": ctx,
                "intent": spec.intent,
                "is_sql": True,
            }

        missing = spec.missing_fields()
        if missing:
            if allow_new_inquiry:
                inquiry_id = self._log_inquiry(
                    ns,
                    prefixes,
                    question,
                    auth_email,
                    datasource=ds_name,
                    status="needs_clarification",
                )
            questions = [
                MISSING_FIELD_QUESTIONS.get(m, f"Please clarify: {m}") for m in missing
            ]
            return self._needs_clarification(inquiry_id, ns, questions)

        # -- 1) context
        ctx = self.build_context_pack("fa", prefixes, question)
        if context:
            ctx.update({k: v for k, v in context.items() if v is not None})

        # -- 3) hints (generic + extras)
        from core.hints import make_hints as _gen_hints

        gh = _gen_hints(question) or {}
        if hints:
            gh.update(hints)

        canonical_sql, rationale = self.planner.plan(question, ctx, hints=gh)
        canonical_sql = extract_sql(canonical_sql) or self._force_sql_only(canonical_sql, question)
        if not canonical_sql:
            if allow_new_inquiry:
                inquiry_id = self._log_inquiry(
                    ns,
                    prefixes,
                    question,
                    auth_email,
                    datasource=ds_name,
                    status="needs_clarification",
                )
            try:
                qs = self.planner.fallback_clarifying_question(question, ctx, gh) or []
            except Exception:
                qs = ["I couldn't derive a clean SQL. Can you clarify the tables or metrics?"]
            resp = self._needs_clarification(
                inquiry_id,
                ns,
                qs,
            )
            resp.update({"context": ctx, "intent": spec.intent, "is_sql": True})
            return resp

        # -- 4) rewrite
        sql = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)

        # -- 5) validate, research retry if configured
        ok, info = self.validator.quick_validate(sql)
        if not ok:
            # refresh researcher per settings each call
            self._ensure_researcher_loaded()
            prefixes = context.get("prefixes") or []
            ns_for_settings = (
                f"fa::{prefixes[0]}" if prefixes else getattr(self, "namespace", "fa::common")
            )
            if ds_name and self.settings.research_allowed(ds_name, ns_for_settings) and self.researcher:
                summary, source_ids = self.researcher.search(question, ctx)
                ctx.setdefault("research", {})
                ctx["research"]["summary"] = summary
                ctx["research"]["source_ids"] = source_ids

                canonical_sql, rationale = self.planner.plan(question, ctx, hints=gh)
                canonical_sql = extract_sql(canonical_sql) or self._force_sql_only(canonical_sql, question)
                if canonical_sql:
                    sql = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)
                    ok, info = self.validator.quick_validate(sql)
                else:
                    ok = False
                    info = {"error": "no_sql_after_research"}

        if not ok:
            return {
                "status": "needs_fix",
                "sql": sql,
                "rationale": rationale,
                "validation": info,
                "context": ctx,
                "intent": spec.intent,
                "is_sql": True,
            }

        return {
            "status": "ok",
            "sql": sql,
            "rationale": rationale,
            "context": ctx,
            "intent": spec.intent,
            "is_sql": True,
        }

    # ------------------ internals ------------------
    def _load_cfg(self, s: Any | None) -> PipelineConfig:
        def g(key: str, default: Optional[str] = None) -> Optional[str]:
            if s is not None:
                try:
                    return s.get(key)
                except Exception:
                    pass
            from os import getenv

            return getenv(key, default)

        return PipelineConfig(
            memory_db_url=g("MEMORY_DB_URL")
            or "postgresql+psycopg2://copilot:pass@localhost/copilot_mem_dev",
            fa_db_url=g("FA_DB_URL"),
            environment=(g("ENVIRONMENT", "local") or "local").lower(),
            prefix_regex=g("FA_PREFIX_REGEX", r"^[0-9]+_$") or r"^[0-9]+_$",
            sample_rows_per_table=int(g("FA_SAMPLE_ROWS_PER_TABLE", "5") or 5),
            profile_stats=(g("FA_PROFILE_STATS", "false") or "false").lower()
            in {"1", "true", "t", "yes", "y"},
            context_topk_tables=int(g("CONTEXT_TOPK_TABLES", "5") or 5),
            context_topk_columns=int(g("CONTEXT_TOPK_COLUMNS", "20") or 20),
        )

    def _make_engine(self, url: Optional[str], pool_name: str) -> Engine:
        if not url:
            raise RuntimeError(f"Missing DB URL for {pool_name}")
        return create_engine(url, pool_pre_ping=True, pool_recycle=1800)

    def _load_inquiry_notes(self, inquiry_id: int) -> list[dict]:
        with self.mem_engine.connect() as c:
            row = c.execute(_sqltext("SELECT admin_notes FROM mem_inquiries WHERE id=:id"),
                            {"id": inquiry_id}).mappings().first()
            if not row:
                return []
            notes = row["admin_notes"] or []
        # normalize: each item is {"text": "...", "by": "...", "ts": "..."}
        out = []
        for n in notes:
            try:
                out.append({"text": n.get("text",""), "by": n.get("by",""), "ts": n.get("ts","")})
            except Exception:
                out.append({"text": str(n), "by": "", "ts": ""})
        return out

    def _hints_from_notes(self, notes: list[dict], question: str) -> dict:
        # Concatenate notes + question, then reuse core.hints to extract date ranges & simple filters
        from core.hints import make_hints as _gen_hints
        blob = " ".join([n.get("text","") for n in notes] + [question])
        return _gen_hints(blob) or {}

    def process_admin_reply(self, inquiry_id: int) -> dict:
        """After we append an admin note, try again using the accumulated notes."""
        with self.mem_engine.begin() as cx:
            row = cx.execute(
                text(
                    """
                SELECT id, namespace, question, prefixes, admin_notes,
                       COALESCE(clarification_rounds,0) AS rounds
                FROM mem_inquiries WHERE id = :id
                """
                ),
                {"id": inquiry_id},
            ).mappings().first()

        if not row:
            return {"inquiry_id": inquiry_id, "status": "not_found"}

        prefixes = row["prefixes"] or []
        if isinstance(prefixes, str):
            try:
                prefixes = json.loads(prefixes)
            except Exception:
                prefixes = []
        question = row["question"] or ""
        notes = row.get("admin_notes") or []

        notes_txt = "\n".join(
            f"- {n.get('note')}" for n in notes if isinstance(n, dict) and n.get("note")
        ).strip()

        from apps.fa.hints import make_fa_hints, parse_admin_answer

        overrides = parse_admin_answer(notes_txt) if notes_txt else None
        fa_hints = make_fa_hints(self.mem_engine, prefixes, question, None, overrides)

        context = self.build_context_pack("fa", prefixes, question)
        raw_out = self.planner.plan(question, context, hints=fa_hints)

        sql_or_none = self._coerce_sql_or_none(raw_out)

        if not sql_or_none:
            limit = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", "3") or 3)
            unlimited = limit == -1
            status = "needs_clarification"
            msg = (
                "I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."
            )
            if (not unlimited) and row["rounds"] + 1 >= limit:
                status = "failed"
                msg = "I could not derive a valid SQL after multiple clarifications."
            with self.mem_engine.begin() as cx:
                cx.execute(
                    text(
                        """UPDATE mem_inquiries SET status = :st, updated_at = NOW() WHERE id = :id"""
                    ),
                    {"id": inquiry_id, "st": status},
                )
            return {"inquiry_id": inquiry_id, "status": status, "message": msg}

        canonical_sql = sql_or_none
        from core.pipeline import SQLRewriter

        sql_exec = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)
        result = self._legacy_validate_and_execute(
            sql_exec, list(prefixes), auth_email=None, inquiry_id=inquiry_id
        )

        with self.mem_engine.begin() as cx:
            cx.execute(
                text(
                    """
                UPDATE mem_inquiries
                SET status='answered', answered_by = :by, answered_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
                """
                ),
                {"id": inquiry_id, "by": "admin"},
            )
        return {"inquiry_id": inquiry_id, "status": "answered", "result": result}

    def replan_from_admin_notes(self, inquiry_id: int, answered_by: str = "") -> dict:
        # Fetch original question, prefixes, and all notes for this inquiry
        with self.mem_engine.begin() as c:
            r = c.execute(text(
                """
                SELECT namespace, prefixes, question,
                       COALESCE(admin_notes, '[]'::jsonb) AS notes
                  FROM mem_inquiries
                 WHERE id = :id
                """
            ), {"id": inquiry_id}).mappings().first()

        if not r:
            return {"inquiry_id": inquiry_id, "status": "failed", "error": "inquiry not found"}

        namespace = r["namespace"]
        prefixes = r["prefixes"]
        if isinstance(prefixes, str):
            try:
                prefixes = json.loads(prefixes)
            except Exception:
                prefixes = []
        question = r["question"] or ""
        notes = r["notes"] or []
        hint_text = " ; ".join([n.get("text", "") for n in notes if n and isinstance(n, dict)])

        context = self.build_context_pack(namespace, prefixes, question)
        hints = {"admin_notes": notes, "free_text": hint_text}

        canonical_sql, rationale = self.planner.plan(question, context, hints=hints)
        canonical_sql = extract_sql(canonical_sql) or self._force_sql_only(canonical_sql, question)

        if not canonical_sql:
            followups = self._ask_one_more(question, context)
            with self.mem_engine.begin() as c:
                c.execute(text(
                    """
                    UPDATE mem_inquiries
                       SET status     = 'needs_clarification',
                           updated_at = NOW()
                     WHERE id = :id
                    """
                ), {"id": inquiry_id})
            return {"inquiry_id": inquiry_id, "status": "needs_clarification", "questions": followups}

        from core.pipeline import SQLRewriter
        sql_exec = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)

        try:
            result = self._legacy_validate_and_execute(sql_exec, list(prefixes), auth_email=None, inquiry_id=inquiry_id)
        except Exception:
            followups = self._ask_one_more(question, context)
            with self.mem_engine.begin() as c:
                c.execute(text(
                    """
                    UPDATE mem_inquiries
                       SET status     = 'needs_clarification',
                           updated_at = NOW()
                     WHERE id = :id
                    """
                ), {"id": inquiry_id})
            return {"inquiry_id": inquiry_id, "status": "needs_clarification", "questions": followups}

        with self.mem_engine.begin() as c:
            c.execute(text(
                """
                UPDATE mem_inquiries
                   SET status      = 'answered',
                       run_id      = :run_id,
                       answered_by = :by,
                       answered_at = NOW(),
                       updated_at  = NOW()
                 WHERE id = :id
                """
            ), {"id": inquiry_id, "run_id": result.get("run_id"), "by": answered_by or ""})

        return {"inquiry_id": inquiry_id, "status": "answered", **result}

    def _ask_one_more(self, question: str, context: dict) -> list[str]:
        return ["Can you confirm the tables/metrics?"]

    def retry_from_admin(self, *, inquiry_id: int, source: str, prefixes: Iterable[str],
                         question: str, answered_by: str) -> Dict[str, Any]:
        """
        Use accumulated admin notes for this inquiry to try again.
        1) Build context
        2) Convert notes → hints
        3) Try direct derivation from admin notes (derive.py)
        4) Otherwise re-plan with hints
        5) Validate; on failure, ask for the next minimal clarification
        """
        context = self.build_context_pack(source, prefixes, question)

        notes = self._load_inquiry_notes(inquiry_id)
        extra_hints = self._hints_from_notes(notes, question)

        # 3) First attempt: direct derivation from admin notes (if any)
        try:
            from derive import derive_sql_from_admin_reply
            tables = [t["table_name"] for t in context.get("tables", [])]
            cols = [f"{c['table_name']}.{c['column_name']}" for c in context.get("columns", [])]
            admin_blob = "\n".join([n.get("text","") for n in notes])
            if admin_blob.strip():
                sql0, why0 = derive_sql_from_admin_reply(
                    self.llm,
                    question=question,
                    admin_reply=admin_blob,
                    tables=tables,
                    columns=cols,
                    metrics=(context.get("metrics") or {}).keys(),
                )
            else:
                sql0, why0 = None, None
        except Exception:
            sql0, why0 = None, None

        # canonical → prefixed
        if sql0:
            from core.pipeline import SQLRewriter
            sql_exec = SQLRewriter.rewrite_for_prefixes(sql0, prefixes)
            ok, info = self.validator.quick_validate(sql_exec)
            if ok:
                return {"status": "ok", "sql": sql_exec, "rationale": why0 or "Derived from admin notes", "context": context}
            # fallthrough to LLM plan with hints

        # 4) LLM plan with hints
        canonical_sql, rationale = self.planner.plan(question, context, hints=extra_hints)
        from core.pipeline import SQLRewriter
        sql_exec = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)

        # 5) Validate; if still not ok, ask next best question and keep the loop alive
        ok, info = self.validator.quick_validate(sql_exec)
        if ok:
            return {"status": "ok", "sql": sql_exec, "rationale": rationale, "context": context}

        # Next clarifying question (short & actionable)
        next_qs = [
            "I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."
        ]

        # Respect MAX_CLARIFICATION_ROUNDS but do not hard-stop; caller holds the loop
        try:
            cap = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", "5") or 5)
        except Exception:
            cap = 5
        with self.mem_engine.begin() as c:
            c.execute(_sqltext("""
                UPDATE mem_inquiries
                SET status = 'needs_clarification',
                    updated_at = NOW()
                WHERE id = :id
            """), {"id": inquiry_id})
        return {"status": "needs_clarification", "questions": next_qs, "context": context}


# ------------------ context builder ------------------
class ContextBuilder:
    def __init__(
        self,
        mem_engine: Engine,
        namespaces: List[str],
        topk_tables: int,
        topk_columns: int,
        cache: Dict[str, Dict[str, Any]],
        keyword_expander: Optional[Callable[[List[str]], List[str]]] = None,
    ):
        self.db = mem_engine
        self.namespaces = namespaces
        self.topk_tables = topk_tables
        self.topk_columns = topk_columns
        self.cache = cache
        self.keyword_expander = keyword_expander  # optional FA synonym expander

    def build(self, user_text: str) -> Dict[str, Any]:
        base = self._keywords(user_text)  # generic split only
        keywords = (
            list(set(self.keyword_expander(base))) if self.keyword_expander else base
        )  # NEW
        tables: List[Dict[str, Any]] = []
        columns: List[Dict[str, Any]] = []
        glossary: List[Dict[str, Any]] = []
        rules: List[Dict[str, Any]] = []

        for ns in self.namespaces:
            ns_cache = self.cache.get(ns)
            if not ns_cache:
                ns_cache = self._load_ns(ns)
                self.cache[ns] = ns_cache
            tables += self._match_tables(ns_cache["tables"], keywords, self.topk_tables)
            columns += self._match_columns(
                ns_cache["columns"], keywords, self.topk_columns
            )
            glossary += ns_cache.get("glossary_top", [])
            rules += ns_cache.get("rules_top", [])
            # NEW: accumulate join hints from each namespace (dedupe later)
            join_hints = ns_cache.get("preferred_joins", [])
            if join_hints:
                # stash into a temp list on the builder
                columns.append(
                    {
                        "table_name": "__JOIN_HINTS__",
                        "column_name": "\n".join(join_hints),
                        "data_type": "",
                        "schema_name": "",
                    }
                )

        # NEW: extract join_hints back out (kept separate from real columns)
        join_hints_out = []
        real_columns = []
        for c in columns:
            if c["table_name"] == "__JOIN_HINTS__":
                join_hints_out += c["column_name"].splitlines()
            else:
                real_columns.append(c)

        return {
            "namespaces": self.namespaces,
            "keywords": keywords,
            "tables": tables[: self.topk_tables],
            "columns": real_columns[: self.topk_columns],  # <-- use real_columns
            "glossary": glossary,
            "rules": rules,
            "join_hints": join_hints_out,  # <-- optional: helpful to planner/ERD
        }

    def _load_ns(self, namespace: str) -> Dict[str, Any]:
        with self.db.connect() as c:
            tables_rows = (
                c.execute(
                    text(
                        """
                    SELECT id, table_name, schema_name, table_comment, primary_key, date_columns
                    FROM mem_tables WHERE namespace=:ns
                """
                    ),
                    {"ns": namespace},
                )
                .mappings()
                .all()
            )
            tables = _as_dicts(tables_rows)

            cols_rows = (
                c.execute(
                    text(
                        """
                    SELECT c.table_id, c.column_name, c.data_type, c.is_nullable
                    FROM mem_columns c
                    JOIN mem_tables t ON t.id=c.table_id AND t.namespace=:ns
                """
                    ),
                    {"ns": namespace},
                )
                .mappings()
                .all()
            )
            cols = _as_dicts(cols_rows)

        # group by table_id
        cols_by_table: Dict[int, List[Dict[str, Any]]] = {}
        for r in cols:
            cols_by_table.setdefault(r["table_id"], []).append(r)

        # flatten for keyword matching (already dicts)
        flat_cols: List[Dict[str, Any]] = []
        for t in tables:
            for c in cols_by_table.get(t["id"], []):
                flat_cols.append(
                    {
                        "table_name": t["table_name"],
                        "schema_name": t["schema_name"],
                        "column_name": c["column_name"],
                        "data_type": c["data_type"],
                    }
                )

        # glossary / rules → dicts too
        with self.db.connect() as c:
            glossary = _as_dicts(
                c.execute(
                    text(
                        "SELECT term, definition FROM mem_glossary WHERE namespace=:ns LIMIT 20"
                    ),
                    {"ns": namespace},
                )
                .mappings()
                .all()
            )
            rules = _as_dicts(
                c.execute(
                    text(
                        "SELECT rule_name, rule_type, scope FROM mem_rules WHERE namespace=:ns LIMIT 20"
                    ),
                    {"ns": namespace},
                )
                .mappings()
                .all()
            )

        return {
            "tables": tables,
            "columns": flat_cols,
            "glossary_top": glossary,
            "rules_top": rules,
        }

    def _keywords(self, s: str) -> List[str]:
        base = [w.lower() for w in re.findall(r"[A-Za-z0-9_]+", s)]
        if self.keyword_expander:
            try:
                expanded = self.keyword_expander(base)
                if expanded:
                    return list(dict.fromkeys(expanded))  # preserve order, unique
            except Exception:
                pass
        return base

    def _match_tables(
        self, tables: List[Dict[str, Any]], keywords: List[str], k: int
    ) -> List[Dict[str, Any]]:
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for t in tables:
            # ensure dict (in case anything slips through)
            td = dict(t)
            name = td["table_name"].lower()
            comment = (td.get("table_comment") or "").lower()
            score = sum(1 for kw in keywords if kw in name or kw in comment)
            if score:
                scored.append((score, td))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in scored[:k]]

    def _match_columns(
        self, columns: List[Dict[str, Any]], keywords: List[str], k: int
    ) -> List[Dict[str, Any]]:
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for c in columns:
            tname_raw = (c["table_name"] or "").lower()
            tname = re.sub(r"^\d+_", "", tname_raw)
            name = f"{tname}.{c['column_name'].lower()}"
            score = sum(1 for kw in keywords if kw in name)
            if score:
                scored.append((score, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored[:k]]



    def _resolve_app_dsn(self, namespace: str) -> str:
        default_ds = self.settings.get("DEFAULT_DATASOURCE", namespace=namespace)
        conns = self.settings.get("DB_CONNECTIONS", namespace=namespace) or []
        if default_ds and isinstance(conns, list):
            for c in conns:
                if c.get("name") == default_ds and c.get("url"):
                    return c["url"]
        app_url = self.settings.get("APP_DB_URL", namespace=namespace)
        if app_url:
            return app_url
        fa_url = self.settings.get("FA_DB_URL", namespace=namespace)
        if fa_url:
            return fa_url
        raise RuntimeError("No datasource URL configured")

    def _set_inquiry_needs_clarification(self, inquiry_id: int, questions: list[str]) -> None:
        from sqlalchemy import text
        with self.mem_engine.begin() as c:
            c.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'needs_clarification',
                       questions = to_jsonb(:qs),
                       updated_at = NOW()
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id, "qs": questions},
            )

    def _set_inquiry_failed(self, inquiry_id: int, error_msg: str) -> None:
        from sqlalchemy import text
        with self.mem_engine.begin() as c:
            c.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'failed',
                       error_message = :err,
                       updated_at = NOW()
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id, "err": error_msg[:1000]},
            )

    def _mark_inquiry_answered(self, inquiry_id: int, answered_by: str) -> None:
        from sqlalchemy import text
        with self.mem_engine.begin() as c:
            c.execute(
                text(
                    """
                UPDATE mem_inquiries
                   SET status = 'answered',
                       answered_by = :by,
                       answered_at = NOW(),
                       updated_at = NOW()
                 WHERE id = :id
                """
                ),
                {"id": inquiry_id, "by": answered_by},
            )

    def _ensure_limit(self, sql: str, default_limit: int = 100) -> str:
        s = sql.strip()
        if re.search(r'\blimit\s+\d+\b', s, flags=re.I):
            return s
        if re.match(r'^\s*select\b', s, flags=re.I):
            return f"{s.rstrip().rstrip(';')} LIMIT {default_limit};"
        return s

    def _legacy_validate_and_execute(
        self,
        sql: str,
        prefixes: list[str],
        auth_email: str | None,
        inquiry_id: int,
        notes: dict | None = None,
    ) -> dict:
        """
        Legacy validation/execution path used by older code paths.
        """
        ok, info = self.validator.quick_validate(sql)
        if not ok:
            err = info.get("error") if isinstance(info, dict) else str(info)
            raise RuntimeError(err or "validation failed")

        res = run_select(self.app_engine, sql, limit=50)

        notify = str(self.settings.get("AUTO_NOTIFY_ON_SUCCESS", "false")).lower()
        if auth_email and notify in ("1", "true", "yes", "on"):
            try:
                mailer = Emailer(self.settings)
                csv_bytes = as_csv(res)
                body_html = "<p>Query executed successfully.</p>"
                mailer.send(
                    to=[auth_email],
                    subject=f"Inquiry #{inquiry_id} results",
                    html=body_html,
                    attachments=[("result.csv", csv_bytes, "text/csv")],
                )
            except Exception:
                pass

        return {
            "sql_final": sql,
            "rows": res.get("rowcount"),
            "preview": res.get("rows"),
        }

# ------------------ simple SQL rewriter ------------------
class SQLRewriter:
    @staticmethod
    def rewrite_for_prefixes(canonical_sql: str, prefixes: Iterable[str]) -> str:
        ps = list(prefixes)
        if len(ps) == 1:
            return SQLRewriter._prepend_prefix(canonical_sql, ps[0])
        parts = []
        for p in ps:
            sql_p = SQLRewriter._prepend_prefix(canonical_sql, p)
            parts.append(f"SELECT '{p}' AS tenant, * FROM ( {sql_p} ) t")
        return "\nUNION ALL\n".join(parts)

    @staticmethod
    def _prepend_prefix(sql: str, prefix: str) -> str:
        def repl(m):
            kw, name = m.group(1), m.group(2)
            if "." in name or name.startswith("("):
                return m.group(0)
            return f"{kw} `{prefix}{name}`"  # backtick quoting for MySQL

        pattern = re.compile(
            r"\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE
        )
        return pattern.sub(repl, sql)
