"""
Core pipeline orchestration
- Loads settings & model
- Builds DB engines (Postgres memory, MySQL FA)
- Runs FA schema ingestion (prefix-aware)
- Provides a light ContextPack builder
- Scaffolds Clarifier / Planner / Validator agents (FA-specific if available)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.model_loader import load_model
from core.settings import Settings
from core.agents import ClarifierAgent, PlannerAgent, ValidatorAgent


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
        # 0) Basics used by other steps
        self._cache: Dict[str, Dict[str, Any]] = {}  # <-- ensure exists early
        self.settings = settings if isinstance(settings, Settings) else Settings(namespace=namespace)

        # 1) Load cfg and build engines
        self.cfg = self._load_cfg(self.settings)
        self.mem_engine = self._make_engine(self.cfg.memory_db_url, pool_name="mem")

        # Attach mem engine & namespace so Settings can read mem_settings
        try:
            self.settings.attach_mem_engine(self.mem_engine)
            self.settings.set_namespace(namespace)
        except Exception:
            pass

        self.fa_engine: Optional[Engine] = (
            self._make_engine(self.cfg.fa_db_url, pool_name="fa")
            if self.cfg.fa_db_url else None
        )

        # 2) Load the LLM
        self.llm = load_model(self.settings)

        # 3) Compile prefix regex
        import re as _re
        self._prefix_re = _re.compile(self.cfg.prefix_regex)

        # 4) Lazy-import FA agents (project-specific) with fallback to core agents
        try:
            from apps.fa.agents import ClarifierAgentFA, PlannerAgentFA, ValidatorAgentFA
            self.clarifier = ClarifierAgentFA(self.llm)
            self.planner = PlannerAgentFA(self.llm)
            self.validator = ValidatorAgentFA(self.fa_engine)
        except Exception:
            # generic core agents
            from core.agents import ClarifierAgent, PlannerAgent, ValidatorAgent
            self.clarifier = ClarifierAgent(self.llm)
            self.planner = PlannerAgent(self.llm)
            self.validator = ValidatorAgent(self.fa_engine)

    # ------------------ public API ------------------
    def ensure_ingested(self, source: str, prefixes: Iterable[str], fa_version: Optional[str] = None) -> Dict[str, int]:
        """Ensure metadata for given prefixes exists/updated. Returns {prefix: snapshot_id}.
        For source=="fa", uses apps.fa.ingestor.FASchemaIngestor.
        """
        if source != "fa":
            raise ValueError("Unknown source; only 'fa' is supported right now")
        if not self.fa_engine:
            raise RuntimeError("FA DB URL not configured")

        # local import keeps core reusable
        from apps.fa.ingestor import FASchemaIngestor

        ing = FASchemaIngestor(
            fa_engine=self.fa_engine,
            mem_engine=self.mem_engine,
            prefix_regex=self.cfg.prefix_regex,
            sample_rows_per_table=self.cfg.sample_rows_per_table,
            profile_stats=self.cfg.profile_stats,
            namespace_prefix="fa::",
        )
        out: Dict[str, int] = {}
        for p in prefixes:
            if not self._prefix_re.match(p):
                raise ValueError(f"Invalid prefix '{p}' per regex {self.cfg.prefix_regex}")
            out[p] = ing.ingest_prefix(p, fa_version=fa_version)
            # drop cache for this namespace so next context build reloads
            self._cache.pop(f"fa::{p}", None)
        return out

    def build_context_pack(self, source: str, prefixes: Iterable[str], query: str, extra: dict | None = None) -> Dict[
        str, Any]:
        if source != "fa":
            raise ValueError("Unknown source; only 'fa' is supported right now")
        namespaces = [f"fa::{p}" for p in prefixes]
        ctx = ContextBuilder(self.mem_engine, namespaces,
                             topk_tables=self.cfg.context_topk_tables,
                             topk_columns=self.cfg.context_topk_columns,
                             cache=self._cache).build(query)
        if extra:
            ctx.update(extra)
        return ctx

    def answer(self, source: str, prefixes: Iterable[str], question: str,
               context_override: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """
        End-to-end ask flow:
          1) Build/accept context
          2) Clarification (policy-aware, FA-savvy)
          3) Planning (metric/date aware)
          4) Prefix rewrite
          5) Validate (EXPLAIN or probe)
        """
        # 1) Build/accept context (tables/columns/metrics/terms)
        context = context_override or self.build_context_pack(source, prefixes, question)

        # 2) Clarify based on ASK_MODE policy
        need, clar_qs = self.clarifier.maybe_ask(question, context)
        if need:
            return {"status": "needs_clarification", "questions": clar_qs, "context": context}

        # 3) Plan canonical SQL
        canonical_sql, rationale = self.planner.plan(question, context)

        # 4) Prefix-rewrite canonical → tenant tables
        sql = SQLRewriter.rewrite_for_prefixes(canonical_sql, prefixes)

        # 5) Validate
        ok, info = self.validator.quick_validate(sql)
        if not ok:
            return {"status": "needs_fix", "sql": sql, "rationale": rationale, "validation": info, "context": context}

        return {"status": "ok", "sql": sql, "rationale": rationale, "context": context}


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
            memory_db_url=g("MEMORY_DB_URL") or "postgresql+psycopg2://copilot:pass@localhost/copilot_mem_dev",
            fa_db_url=g("FA_DB_URL"),
            environment=(g("ENVIRONMENT", "local") or "local").lower(),
            prefix_regex=g("FA_PREFIX_REGEX", r"^[0-9]+_$") or r"^[0-9]+_$",
            sample_rows_per_table=int(g("FA_SAMPLE_ROWS_PER_TABLE", "5") or 5),
            profile_stats=(g("FA_PROFILE_STATS", "false") or "false").lower() in {"1","true","t","yes","y"},
            context_topk_tables=int(g("CONTEXT_TOPK_TABLES", "5") or 5),
            context_topk_columns=int(g("CONTEXT_TOPK_COLUMNS", "20") or 20),
        )

    def _make_engine(self, url: Optional[str], pool_name: str) -> Engine:
        if not url:
            raise RuntimeError(f"Missing DB URL for {pool_name}")
        return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


# ------------------ context builder ------------------
class ContextBuilder:
    def __init__(self, mem_engine: Engine, namespaces: List[str], topk_tables: int,
                 topk_columns: int, cache: Dict[str, Dict[str, Any]],
                 keyword_expander: Optional[callable] = None):  # NEW
        self.db = mem_engine
        self.namespaces = namespaces
        self.topk_tables = topk_tables
        self.topk_columns = topk_columns
        self.cache = cache
        self.keyword_expander = keyword_expander  # NEW

    def build(self, user_text: str) -> Dict[str, Any]:
        base = self._keywords(user_text)  # generic split only
        keywords = list(set(self.keyword_expander(base))) if self.keyword_expander else base  # NEW
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
            columns += self._match_columns(ns_cache["columns"], keywords, self.topk_columns)
            glossary += ns_cache.get("glossary_top", [])
            rules += ns_cache.get("rules_top", [])

        return {
            "namespaces": self.namespaces,
            "keywords": keywords,
            "tables": tables[: self.topk_tables],
            "columns": columns[: self.topk_columns],
            "glossary": glossary,
            "rules": rules,
        }

    def _load_ns(self, namespace: str) -> Dict[str, Any]:
        def _dicts(rows):
            # rows are RowMapping (SQLAlchemy 2.0); make them JSON-serializable
            return [dict(r) for r in rows]

        with self.db.connect() as c:
            tables_rows = c.execute(
                text("""
                    SELECT id, table_name, schema_name, table_comment, primary_key, date_columns
                    FROM mem_tables
                    WHERE namespace = :ns
                """),
                {"ns": namespace},
            ).mappings().all()
            tables = _dicts(tables_rows)

            cols_rows = c.execute(
                text("""
                    SELECT c.table_id, c.column_name, c.data_type, c.is_nullable
                    FROM mem_columns c
                    JOIN mem_tables t ON t.id = c.table_id AND t.namespace = :ns
                """),
                {"ns": namespace},
            ).mappings().all()

        # group columns by table_id
        cols_by_table: Dict[int, List[Dict[str, Any]]] = {}
        for r in cols_rows:
            tr = dict(r)  # make plain dict
            cols_by_table.setdefault(tr["table_id"], []).append(tr)

        # flatten for keyword matching
        flat_cols: List[Dict[str, Any]] = []
        for t in tables:
            for c in cols_by_table.get(t["id"], []):
                flat_cols.append({
                    "table_name": t["table_name"],
                    "schema_name": t["schema_name"],
                    "column_name": c["column_name"],
                    "data_type": c["data_type"],
                })

        # glossary / rules
        with self.db.connect() as c:
            glossary = _dicts(c.execute(
                text("SELECT term, definition FROM mem_glossary WHERE namespace = :ns LIMIT 20"),
                {"ns": namespace},
            ).mappings().all())
            rules = _dicts(c.execute(
                text("SELECT rule_name, rule_type, scope FROM mem_rules WHERE namespace = :ns LIMIT 20"),
                {"ns": namespace},
            ).mappings().all())

        return {"tables": tables, "columns": flat_cols, "glossary_top": glossary, "rules_top": rules}

    def _keywords(self, s: str) -> List[str]:
        return [w.lower() for w in re.findall(r"[A-Za-z0-9_]+", s)]

    def _match_tables(self, tables: List[Dict[str, Any]], keywords: List[str], k: int) -> List[Dict[str, Any]]:
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for t in tables:
            raw = (t["table_name"] or "").lower()
            # handle stored names with prefixes like "579_debtors_master"
            name = re.sub(r"^\d+_", "", raw)
            comment = (t.get("table_comment") or "").lower()
            score = 0
            for kw in keywords:
                if kw in raw or kw in name or kw in comment:
                    score += 1
            if score:
                scored.append((score, t))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in scored[:k]]

    def _match_columns(self, columns: List[Dict[str, Any]], keywords: List[str], k: int) -> List[Dict[str, Any]]:
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for c in columns:
            tname_raw = (c['table_name'] or "").lower()
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
        pattern = re.compile(r"\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE)
        return pattern.sub(repl, sql)
