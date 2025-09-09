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
from sqlalchemy.engine import Engine

import importlib
from core.agents import ClarifierAgent, PlannerAgent, ValidatorAgent
from core.model_loader import load_model, load_clarifier_model
from core.intent import IntentRouter
from core.settings import Settings
from core.research import build_researcher
from core.sql_exec import get_app_engine, run_select

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
        ns = context.get("namespace") or self.namespace
        prefixes = context.get("prefixes") or []
        auth_email = context.get("auth_email")
        inquiry_id = context.get("inquiry_id")
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
                sql = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)
                ok, info = self.validator.quick_validate(sql)

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
