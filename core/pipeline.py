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
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable

from sqlalchemy import create_engine, text
from sqlalchemy import text as _sqltext
from sqlalchemy.engine import Engine
import sqlalchemy as sa

import importlib
from core.agents import ClarifierAgent, PlannerAgent, ValidatorAgent
from core.model_loader import load_model, load_clarifier_model
from core.intent import IntentRouter
from core.settings import Settings
from core.research import build_researcher
from core.sql_exec import get_app_engine, run_select, as_csv
from core.sql_utils import extract_sql, looks_like_sql
from core.inquiries import create_or_update_inquiry
from core.emailer import Emailer

from types import SimpleNamespace


def _as_dicts(rows):
    return [dict(r) for r in rows]


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
        self.researcher = build_researcher(self.settings)
        self._researcher_class_path: str | None = None
        self._researcher_fingerprint = None

        # 1) Load cfg and build engines
        self.cfg = self._load_cfg(self.settings)
        self.mem_engine = self._make_engine(self.cfg.memory_db_url, pool_name="mem")

        # Attach mem engine & namespace so Settings can read mem_settings
        try:
            self.settings.attach_mem_engine(self.mem_engine)
            self.settings.set_namespace(namespace)
        except Exception:
            pass

        self.app_engine = get_app_engine(self.settings, namespace=namespace)

        # 2) Load the LLM
        self.llm = load_model(self.settings)
        self.clarifier_llm = load_clarifier_model(self.settings)
        self.intent_router = IntentRouter(self.clarifier_llm)

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
        ClarifierCls = getattr(mod, "ClarifierAgentFA", ClarifierAgent)
        ValidatorCls = getattr(mod, "ValidatorAgentFA", ValidatorAgent)
        self.clarifier = ClarifierCls(self.llm, self.settings)
        self.validator = ValidatorCls(self.app_engine, self.settings)

        # 5) Researcher: load after engines & settings are ready
        self._ensure_researcher_loaded()

        # ---------- researcher helpers (class methods, NOT nested) ----------

    def _ensure_researcher_loaded(self) -> None:
        """(Re)build the researcher if relevant settings changed."""
        try:
            fp = {
                "mode": bool(self.settings.get("RESEARCH_MODE", False)),
                "provider": (self.settings.get("RESEARCH_PROVIDER") or "").lower(),
                "class": self.settings.get("RESEARCHER_CLASS") or "",
            }
            desired_key = json.dumps(fp, sort_keys=True)
        except Exception:
            desired_key = None

        if desired_key == self._researcher_fingerprint:
            return

        # build via research factory (handles class/provider/mode)
        from core.research import build_researcher

        self.researcher = build_researcher(self.settings)
        self._researcher_fingerprint = desired_key

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

    def continue_inquiry(self, inquiry_id: int) -> dict:
        """
        Re-run planning/execution for an existing inquiry using accumulated
        admin_notes as hints. Updates mem_inquiries.status to either 'answered'
        or stays/returns 'needs_clarification'. Respects MAX_CLARIFICATION_ROUNDS
        (mem_settings).
        """
        mem = self.mem_engine
        with mem.begin() as c:
            row = c.execute(text("SELECT * FROM mem_inquiries WHERE id = :id"), {"id": inquiry_id}).fetchone()
        if not row:
            return {"ok": False, "error": "not_found", "inquiry_id": inquiry_id}

        ns = row.namespace
        question = row.question
        prefixes = row.prefixes or []
        auth_email = getattr(row, "auth_email", None)
        rounds = int(getattr(row, "clarification_rounds", 0) or 0)
        notes = list(getattr(row, "admin_notes", []) or [])

        max_rounds = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", "3") or 3)
        if rounds >= max_rounds:
            with mem.begin() as c:
                c.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status = 'failed', updated_at = NOW()
                     WHERE id = :id
                    """
                    ),
                    {"id": inquiry_id},
                )
            return {
                "ok": True,
                "status": "failed",
                "inquiry_id": inquiry_id,
                "message": f"Max clarification rounds reached ({max_rounds}).",
            }

        admin_hint = ""
        if notes:
            admin_hint = " | ".join(n.get("note", "") for n in notes if isinstance(n, dict))[-2000:]

        try:
            from apps.fa.hints import make_fa_hints

            app_hints = make_fa_hints(self.mem_engine, prefixes, question)
        except Exception:
            app_hints = {}

        app_hints = dict(app_hints or {})
        if admin_hint:
            app_hints["admin_hint"] = admin_hint

        try:
            context = self._build_context(ns, prefixes)
            canonical_sql, rationale = self.planner.plan(question, context, hints=app_hints)
            run_info = self._validate_and_execute(ns, canonical_sql, prefixes, question, auth_email=auth_email)
            run_id = run_info.get("run_id")

            with mem.begin() as c:
                c.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status = 'answered',
                           run_id = :rid,
                           answered_by = COALESCE(answered_by, :by),
                           answered_at = COALESCE(answered_at, NOW()),
                           updated_at = NOW()
                     WHERE id = :id
                    """
                    ),
                    {"id": inquiry_id, "rid": run_id, "by": auth_email or "system"},
                )
            return {"ok": True, "status": "answered", "inquiry_id": inquiry_id, "run_id": run_id}

        except Exception as e:
            followup_q = "I couldn't derive a clean SQL. Can you clarify the tables or metrics?"
            try:
                followup_q = self.planner.followup_question or followup_q
            except Exception:
                pass

            with mem.begin() as c:
                c.execute(
                    text(
                        """
                    UPDATE mem_inquiries
                       SET status = 'needs_clarification',
                           updated_at = NOW()
                     WHERE id = :id
                    """
                    ),
                    {"id": inquiry_id},
                )
            return {
                "ok": True,
                "status": "needs_clarification",
                "inquiry_id": inquiry_id,
                "questions": [followup_q],
            }

    def resume_inquiry(self, inquiry_id: int) -> dict:
        with self.mem_engine.begin() as c:
            row = c.execute(
                text("SELECT id, namespace, prefixes, question, auth_email, admin_notes, clarification_rounds, status FROM mem_inquiries WHERE id=:id"),
                {"id": inquiry_id},
            ).mappings().first()
        if not row:
            return {"ok": False, "error": "inquiry_not_found", "inquiry_id": inquiry_id}

        ns = row["namespace"]
        question = row["question"]
        auth_email = row["auth_email"]
        prefixes = row["prefixes"] or []
        if isinstance(prefixes, str):
            try:
                prefixes = json.loads(prefixes) or []
            except Exception:
                prefixes = []

        from apps.fa.hints import make_fa_hints
        hints = make_fa_hints(self.mem_engine, prefixes, question)
        admin_text = self._notes_to_text(row["admin_notes"] or [])
        if admin_text:
            hints["admin_notes"] = admin_text

        result = self.answer(
            question=question,
            context={"prefixes": prefixes, "auth_email": auth_email},
            hints=hints,
            inquiry_id=inquiry_id,
            allow_new_inquiry=False,
        )
        return result


    def apply_admin_notes(self, inquiry_id: int, max_rounds: int = 3) -> dict:
        """
        Try to re-derive SQL using the accumulated admin_notes.
        Returns a small dict and is safe to call repeatedly.
        """
        with self.mem_engine.begin() as c:
            row = c.execute(
                sa.text(
                    "SELECT id, namespace, prefixes, question, auth_email, admin_notes "
                    "FROM mem_inquiries WHERE id = :id"
                ),
                {"id": inquiry_id},
            ).mappings().first()
        if not row:
            raise ValueError(f"inquiry {inquiry_id} not found")

        ns = row["namespace"]
        prefixes = row["prefixes"] if isinstance(row["prefixes"], list) else []
        question = row.get("question") or ""
        auth_email = row.get("auth_email")
        notes = row.get("admin_notes") or []

        lines = [n.get("text") for n in notes if isinstance(n, dict) and n.get("text")]
        admin_context = ""
        if lines:
            recent = lines[-max_rounds:]
            admin_context = "Admin notes:\n" + "\n".join(f"- {t}" for t in recent)

        hints = {"admin_notes": admin_context} if admin_context else None
        result = self.answer(
            question=question,
            context={"prefixes": prefixes, "auth_email": auth_email, "namespace": ns},
            hints=hints,
            inquiry_id=inquiry_id,
            allow_new_inquiry=False,
        )
        return result


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
            if admin_reply:
                enriched_q += f"\n\nClarifications: {admin_reply}"

            ctx = self.build_context_pack("fa", prefixes, enriched_q)
            if context:
                ctx.update({k: v for k, v in context.items() if v is not None})

            from core.hints import make_hints as _gen_hints
            gh = _gen_hints(enriched_q) or {}
            if hints:
                gh.update(hints)

            canonical_sql, rationale = self.planner.plan(enriched_q, ctx, hints=gh)
            canonical_sql = extract_sql(canonical_sql) or self._force_sql_only(canonical_sql, question)
            if not canonical_sql:
                if allow_new_inquiry:
                    inquiry_id = self._log_inquiry(
                        ns, prefixes, question, auth_email, status="needs_clarification"
                    )
                # otherwise keep existing inquiry_id
                return {
                    "status": "needs_clarification",
                    "inquiry_id": inquiry_id,
                    "questions": [
                        "I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."
                    ],
                }
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

        # -- 0) intent classification
        it = self.intent_router.classify(question or "")
        if it.kind in {"smalltalk", "help"}:
            return {
                "status": "ok",
                "intent": it.kind,
                "message": (
                    self._render_help(context)
                    if it.kind == "help"
                    else "👋 Hi! Ask me about your data (e.g. “top 10 customers by sales last month”)."
                ),
                "is_sql": False,
            }

        # -- 1) context
        ctx = self.build_context_pack("fa", prefixes, question)
        if context:
            ctx.update({k: v for k, v in context.items() if v is not None})

        # -- 2) clarify
        needs_clarification, clarification_questions = self.clarifier.maybe_ask(
            question, ctx
        )

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
                    ns, prefixes, question, auth_email, status="needs_clarification"
                )
            # otherwise keep existing inquiry_id
            return {
                "status": "needs_clarification",
                "inquiry_id": inquiry_id,
                "questions": [
                    "I couldn't derive a clean SQL. Can you clarify the tables or metrics?"
                ],
                "context": ctx,
                "intent": it.kind,
                "is_sql": True,
            }

        if needs_clarification and it.kind in {"sql", "ambiguous"}:
            inline_ok = bool(context.get("inline_clarify"))
            if inline_ok:
                return {
                    "status": "needs_clarification",
                    "questions": clarification_questions,
                    "context": ctx,
                    "intent": it.kind,
                    "is_sql": True,
                }
            else:
                return {
                    "status": "awaiting_admin",
                    "questions": clarification_questions,
                    "context": ctx,
                    "intent": it.kind,
                    "is_sql": True,
                }

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
                "intent": it.kind,
                "is_sql": True,
            }

        return {
            "status": "ok",
            "sql": sql,
            "rationale": rationale,
            "context": ctx,
            "intent": it.kind,
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
            max_rounds = int(self.settings.get("MAX_CLARIFICATION_ROUNDS", "3") or 3)
            status = "needs_clarification"
            msg = (
                "I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."
            )
            if row["rounds"] + 1 >= max_rounds:
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
        result = self.validate_and_execute(
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
            result = self.validate_and_execute(sql_exec, list(prefixes), auth_email=None, inquiry_id=inquiry_id)
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
        try:
            if self.clarifier:
                need, qs = self.clarifier.maybe_ask(question, context)
                if qs:
                    return qs[:1]
        except Exception:
            pass
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
        need, clar_qs = self.clarifier.maybe_ask(question, context)
        next_qs = clar_qs or ["I couldn't derive a clean SQL from the admin notes. Add one more hint or confirm the tables."]

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

    def validate_and_execute(
        self,
        sql: str,
        prefixes: list[str],
        auth_email: str | None,
        inquiry_id: int,
        notes: dict | None = None,
    ) -> dict:
        """
        Validate the SQL via validator, execute it, optionally email preview.
        Returns dict with keys {sql_final, rows, preview}.
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
