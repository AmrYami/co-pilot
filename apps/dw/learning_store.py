from __future__ import annotations
"""
Lightweight learning store for DW: runs, examples, rules and patches.
Uses MEMORY_DB_URL (Postgres recommended). Falls back to SQLite if needed.
All comments in English by convention.
"""
import datetime as dt
import os
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    JSON,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text as _sql_text
import hashlib as _hashlib
import json as _json
import re as _re
import hashlib as _hashlib
import json as _json
import re as _re

MEM_URL = os.environ.get("MEMORY_DB_URL") or os.getenv("MEMORY_DB_URL")
if not MEM_URL:
    # Safe fallback to file SQLite (dev only)
    MEM_URL = "sqlite:///copilot_mem_dev.sqlite3"

engine = create_engine(MEM_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


class DWRun(Base):
    __tablename__ = "dw_runs"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, index=True)
    namespace = Column(String(128), default="dw::common", index=True)
    user_email = Column(String(255), index=True)
    question = Column(Text)
    question_norm = Column(Text)
    sql = Column(Text)
    ok = Column(Boolean, default=True)
    duration_ms = Column(Integer)
    rows = Column(Integer)
    strategy = Column(String(64))
    explain = Column(Text)
    meta = Column(JSON)


class DWExample(Base):
    __tablename__ = "dw_examples"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow, index=True)
    namespace = Column(String(128), default="dw::common", index=True)
    user_email = Column(String(255), index=True)
    # Normalized Q helps lexical retrieval
    question_norm = Column(Text, index=True)
    raw_question = Column(Text)
    sql = Column(Text)
    tags = Column(JSON)  # optionally: ["fts","eq:ENTITY","order:REQUEST_DATE"]
    success_count = Column(Integer, default=1)
    rating_last = Column(Integer, default=5)


class DWRule(Base):
    __tablename__ = "dw_rules"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow)
    namespace = Column(String(128), default="dw::common", index=True)
    # Types: "patch","constraint","example-pin"
    rule_type = Column(String(64), default="patch", index=True)
    version = Column(Integer, default=1)
    status = Column(String(32), default="shadow")  # shadow|canary|active|disabled
    name = Column(String(255), index=True)
    payload = Column(JSON)  # structured rule content
    canary_percent = Column(Integer, default=10)
    approved_by = Column(String(255))
    approved_at = Column(DateTime)


class DWPatch(Base):
    __tablename__ = "dw_patches"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow)
    namespace = Column(String(128), default="dw::common", index=True)
    user_email = Column(String(255), index=True)
    inquiry_id = Column(Integer, index=True)
    rating = Column(Integer)
    comment = Column(Text)
    # Extracted intent from comment (safe to apply)
    patch_intent = Column(JSON)
    status = Column(String(32), default="shadow")  # shadow|canary|active|disabled
    applied_now = Column(Boolean, default=False)  # whether we already re-planned with it


def init_db() -> None:
    Base.metadata.create_all(engine)


def _normalize_q(q: str) -> str:
    return (q or "").strip().lower()


def record_run(
    namespace: str,
    user_email: Optional[str],
    question: Optional[str],
    sql: str,
    ok: bool,
    duration_ms: int,
    rows: int,
    strategy: str,
    explain: str,
    meta: Dict[str, Any],
) -> None:
    with SessionLocal() as session:
        session.add(
            DWRun(
                namespace=namespace,
                user_email=user_email,
                question=question,
                question_norm=_normalize_q(question or ""),
                sql=sql,
                ok=ok,
                duration_ms=duration_ms,
                rows=rows,
                strategy=strategy,
                explain=explain,
                meta=meta or {},
            )
        )
        session.commit()


def record_example(
    namespace: str,
    user_email: Optional[str],
    question: str,
    sql: str,
    *,
    tags: Optional[List[str]] = None,
    rating: int = 5,
) -> int:
    qn = _normalize_q(question)
    with SessionLocal() as session:
        existing = (
            session.query(DWExample)
            .filter_by(namespace=namespace, question_norm=qn, sql=sql)
            .first()
        )
        if existing:
            existing.success_count = (existing.success_count or 0) + 1
            existing.rating_last = rating
            session.commit()
            return existing.id
        example = DWExample(
            namespace=namespace,
            user_email=user_email,
            question_norm=qn,
            raw_question=question,
            sql=sql,
            tags=tags or [],
            rating_last=rating,
        )
        session.add(example)
        session.commit()
        return example.id


def record_patch(
    namespace: str,
    user_email: Optional[str],
    inquiry_id: int,
    rating: int,
    comment: str,
    patch_intent: Dict[str, Any],
    *,
    status: str = "shadow",
    applied_now: bool = False,
) -> int:
    with SessionLocal() as session:
        patch = DWPatch(
            namespace=namespace,
            user_email=user_email,
            inquiry_id=inquiry_id,
            rating=rating,
            comment=comment,
            patch_intent=patch_intent,
            status=status,
            applied_now=applied_now,
        )
        session.add(patch)
        session.commit()
        return patch.id


def get_similar_examples(namespace: str, question: str, limit: int = 5) -> List[DWExample]:
    qn = _normalize_q(question)
    with SessionLocal() as session:
        rows = session.query(DWExample).filter_by(namespace=namespace).all()
        scored: List[tuple[int, DWExample]] = []
        for row in rows:
            score = 0
            if row.question_norm and qn:
                if row.question_norm in qn or qn in row.question_norm:
                    score += 3
                tokens_a = set(qn.split())
                tokens_b = set((row.question_norm or "").split())
                score += min(len(tokens_a & tokens_b), 3)
            score += min(row.success_count or 0, 3)
            scored.append((score, row))
        scored.sort(key=lambda item: (-item[0], -item[1].id))
        return [row for _, row in scored[:limit]]


def list_metrics_summary(hours: int = 24) -> Dict[str, Any]:
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=hours)
    with SessionLocal() as session:
        runs = session.query(DWRun).filter(DWRun.created_at >= cutoff).all()
        total = len(runs)
        ok = sum(1 for run in runs if run.ok)
        latencies = sorted([run.duration_ms or 0 for run in runs])

    def _p95(values: List[int]) -> int:
        if not values:
            return 0
        idx = int(round(0.95 * (len(values) - 1)))
        return values[idx]

    return {
        "total": total,
        "ok": ok,
        "ok_rate": (ok / total if total else 0.0),
        "p95_ms": _p95(latencies),
    }


# --- Signature-first rules loader (fallback implementation) ------------------

_EMAIL_RE = _re.compile(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$")
_NUMBER_RE = _re.compile(r"^-?\d+(\.\d+)?$")


def _val_type(v: str) -> str:
    s = (v or "").strip()
    if _EMAIL_RE.match(s):
        return "EMAIL"
    if _NUMBER_RE.match(s):
        return "NUMBER"
    return "TEXT"


def _intent_shape_only(intent: Dict[str, Any]) -> Dict[str, Any]:
    eq_shape = dict(intent.get("eq") or {})
    if not eq_shape and intent.get("eq_filters"):
        tmp: Dict[str, Any] = {}
        for col, vals in intent["eq_filters"]:
            types = sorted({_val_type(v) for v in (vals or [])})
            tmp[str(col).upper()] = {"op": ("in" if len(vals or []) > 1 else "eq"), "types": types}
        eq_shape = tmp
    order = None
    if intent.get("order", {}).get("col"):
        order = {
            "col": str(intent["order"]["col"]).upper(),
            "desc": bool(intent["order"].get("desc", True)),
        }
    return {"eq": eq_shape, "fts": [], "group_by": [], "order": order}


def _canon_signature_from_intent(intent: Dict[str, Any]) -> tuple[str, str, str]:
    sig_json = _json.dumps(_intent_shape_only(intent), separators=(",", ":"), sort_keys=True)
    sha256 = _hashlib.sha256(sig_json.encode("utf-8")).hexdigest()
    sha1 = _hashlib.sha1(sig_json.encode("utf-8")).hexdigest()
    return sha256, sha1, sig_json


def _merge_eq_filters_prefer_question(current_eq, rule_eq):
    """Merge rule eq filters with question eq filters, preferring current question values for same columns.

    Accepts either list-of-lists form [["COL", ["V1","V2"]], ...] or canonical dict items
    {"col":"COL","val":"V"}. Returns list-of-lists.
    """
    qmap = {str(c).upper(): vs for c, vs in (current_eq or [])}
    out = []
    for item in (rule_eq or []):
        if isinstance(item, (list, tuple)) and len(item) == 2:
            col, rvals = item[0], item[1]
        elif isinstance(item, dict):
            col = item.get("col") or item.get("field")
            rvals = item.get("val") or item.get("values") or []
            if rvals is not None and not isinstance(rvals, (list, tuple, set)):
                rvals = [rvals]
        else:
            continue
        col = str(col).upper()
        out.append([col, qmap.get(col, rvals or [])])
    for col, vals in (current_eq or []):
        c = str(col).upper()
        if not any(c == x[0] for x in out):
            out.append([c, vals])
    return out


def load_rules_for_question(engine, qnorm: str, intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns merged hints from dw_rules using precedence:
      intent_sha -> rule_signature -> question_norm (exact/global)
    Merges EQ such that question-provided values win for the same columns.
    """
    sha256, sha1, sig_text = _canon_signature_from_intent(intent or {})
    merged: Dict[str, Any] = {}
    with engine.connect() as cx:  # type: ignore[union-attr]
        def _fetch(where_sql: str, binds: Dict[str, Any]):
            sql = _sql_text(
                f"""
                SELECT rule_kind, rule_payload
                  FROM dw_rules
                 WHERE enabled = TRUE AND {where_sql}
                 ORDER BY id DESC
                """
            )
            return cx.execute(sql, binds).all()

        rows = _fetch("intent_sha IN (:sha1, :sha256)", {"sha1": sha1, "sha256": sha256})
        if not rows:
            rows = _fetch("rule_signature = :sig", {"sig": sig_text})
        if not rows:
            rows = _fetch("(question_norm = :q OR COALESCE(question_norm,'') = '')", {"q": qnorm})

    eq_from_rules = []
    for kind, payload in rows:
        try:
            data = payload if isinstance(payload, dict) else _json.loads(payload)
        except Exception:
            continue
        k = (kind or "").strip().lower()
        if k == "eq":
            if isinstance(data.get("eq_filters"), list):
                eq_from_rules.extend(data["eq_filters"])  # type: ignore[index]
        elif k == "order_by":
            merged["order"] = {
                "col": (data.get("sort_by") or "REQUEST_DATE"),
                "desc": bool(data.get("sort_desc", True)),
            }
        elif k == "fts":
            if data.get("tokens"):
                merged["fts_tokens"] = data.get("tokens")

    if eq_from_rules or intent.get("eq_filters"):
        merged["eq_filters"] = _merge_eq_filters_prefer_question(
            intent.get("eq_filters"), eq_from_rules
        )
    return merged
