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

