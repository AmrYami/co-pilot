from __future__ import annotations
"""
Lightweight learning store for DW: runs, examples, rules and patches.
Uses MEMORY_DB_URL (Postgres recommended). Falls back to SQLite if needed.
All comments in English by convention.
"""
import datetime as dt
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, NamedTuple

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

from core.settings import Settings


class SignatureKnobs(NamedTuple):
    fts_shape: str = "groups_sizes"
    eq_list_mode: str = "exact_len"
    eq_list_min_coverage: float = 0.0


DEFAULT_SIGNATURE_KNOBS = SignatureKnobs()
_SIG_KNOBS_CACHE: Optional[SignatureKnobs] = None
log = logging.getLogger("dw")

_CANON_FAMILY_MAP_DEFAULT = {
    "STAKEHOLDERS": "STAKEHOLDER",
    "DEPARTMENTS": "DEPARTMENT",
    "DEPT": "DEPARTMENT",
}

_SETTINGS_OBJ: Optional[Settings] = None


def _settings_obj() -> Optional[Settings]:
    global _SETTINGS_OBJ
    if _SETTINGS_OBJ is None:
        try:
            _SETTINGS_OBJ = Settings(namespace="dw::common")
        except Exception:
            _SETTINGS_OBJ = None
    return _SETTINGS_OBJ


def reset_settings_cache() -> None:
    global _SETTINGS_OBJ
    _SETTINGS_OBJ = None


def _settings_get(key: str) -> Any:
    settings = _settings_obj()
    if settings is None:
        return None
    for scope, namespace in (("namespace", "dw::common"), ("global", "global")):
        try:
            record = settings._fetch(key, scope=scope, namespace=namespace)  # type: ignore[attr-defined]
        except Exception:
            record = None
        if record:
            try:
                return settings._coerce(record.get("value"), record.get("value_type"))  # type: ignore[attr-defined]
            except Exception:
                return record.get("value")
    return None


def _read_flag(name: str, default: bool = False) -> bool:
    raw = _settings_get(name)
    if raw is None:
        raw = os.getenv(name)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        lowered = raw.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "f", "no", "n", "off"}:
            return False
    return default


def _read_setting_str(name: str) -> Optional[str]:
    raw = _settings_get(name)
    if raw is None:
        raw = os.getenv(name)
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw
    return str(raw)


def _read_setting_json(name: str) -> Any:
    raw = _settings_get(name)
    if raw is None:
        raw = os.getenv(name)
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return _json.loads(raw)
        except Exception:
            return None
    return raw


def _read_signature_knobs() -> SignatureKnobs:
    fts_shape_raw = _read_setting_str("SIG_FTS_SHAPE") or DEFAULT_SIGNATURE_KNOBS.fts_shape
    fts_shape = fts_shape_raw.strip().lower()
    if fts_shape not in {"groups_only", "groups_sizes"}:
        fts_shape = DEFAULT_SIGNATURE_KNOBS.fts_shape

    eq_list_mode_raw = _read_setting_str("SIG_EQ_LIST_MODE") or DEFAULT_SIGNATURE_KNOBS.eq_list_mode
    eq_list_mode = eq_list_mode_raw.strip().lower()
    if eq_list_mode not in {"exact_len", "any_len", "bins"}:
        eq_list_mode = DEFAULT_SIGNATURE_KNOBS.eq_list_mode

    try:
        coverage_source = _settings_get("SIG_EQ_LIST_MIN_COVERAGE")
        if coverage_source is None:
            coverage_source = os.getenv("SIG_EQ_LIST_MIN_COVERAGE")
        if coverage_source is None:
            coverage = DEFAULT_SIGNATURE_KNOBS.eq_list_min_coverage
        elif isinstance(coverage_source, (int, float)):
            coverage = float(coverage_source)
        else:
            coverage = float(str(coverage_source))
    except (TypeError, ValueError):
        coverage = DEFAULT_SIGNATURE_KNOBS.eq_list_min_coverage
    coverage = max(0.0, min(1.0, coverage))

    return SignatureKnobs(fts_shape=fts_shape, eq_list_mode=eq_list_mode, eq_list_min_coverage=coverage)


def signature_knobs() -> SignatureKnobs:
    global _SIG_KNOBS_CACHE
    if _SIG_KNOBS_CACHE is None:
        _SIG_KNOBS_CACHE = _read_signature_knobs()
    return _SIG_KNOBS_CACHE


def reset_signature_knobs_cache() -> None:
    global _SIG_KNOBS_CACHE
    _SIG_KNOBS_CACHE = None
    reset_settings_cache()


def _load_family_map() -> Dict[str, str]:
    raw = _read_setting_json("DW_INTENT_FAMILY_CANON")
    if raw is None:
        raw = _CANON_FAMILY_MAP_DEFAULT
    mapped: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            key = str(k or "").strip().upper()
            val = str(v or "").strip().upper()
            if key and val:
                mapped[key] = val
    elif isinstance(raw, str):
        try:
            data = _json.loads(raw)
        except Exception:
            data = None
        if isinstance(data, dict):
            for k, v in data.items():
                key = str(k or "").strip().upper()
                val = str(v or "").strip().upper()
                if key and val:
                    mapped[key] = val
    if mapped:
        return mapped
    return dict(_CANON_FAMILY_MAP_DEFAULT)


_CANON_FAMILY_MAP = _load_family_map()


def _log_intent_match_enabled() -> bool:
    return _read_flag("LOG_INTENT_MATCH", default=False)


def _canonical_family(name: str, canonicalize: bool = True) -> str:
    key = str(name or "").strip().upper()
    if not canonicalize:
        return key
    return _CANON_FAMILY_MAP.get(key, key)


def reset_intent_match_log_cache() -> None:
    return None


def reset_family_map_cache() -> None:
    global _CANON_FAMILY_MAP
    reset_settings_cache()
    _CANON_FAMILY_MAP = _load_family_map()

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


def _val_type(v: Any) -> str:
    """Heuristic type detector for signature hashing."""
    if v is None:
        return "TEXT"
    if isinstance(v, (int, float)):
        s = str(v)
    else:
        s = str(v or "")
    s = s.strip()
    if not s:
        return "TEXT"
    if _EMAIL_RE.match(s):
        return "EMAIL"
    if _NUMBER_RE.match(s):
        return "NUMBER"
    return "TEXT"


def _normalize_value_list(values: Any) -> List[Any]:
    out: List[Any] = []
    seen: set[str] = set()
    for v in values or []:
        if v is None:
            continue
        if isinstance(v, str):
            text = v.strip()
            if not text:
                continue
            key = text.upper()
            value = text
        else:
            value = v
            key = str(v)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _normalize_eq_filters_list(raw: Any) -> List[List[Any]]:
    if not isinstance(raw, list):
        return []
    normalized: List[List[Any]] = []
    for item in raw:
        col = ""
        vals: List[Any] = []
        if isinstance(item, (list, tuple)) and len(item) == 2:
            col = str(item[0] or "").strip().upper()
            candidate = item[1]
        elif isinstance(item, dict):
            col = str((item.get("col") or item.get("field") or "")).strip().upper()
            candidate = (
                item.get("val")
                if item.get("val") is not None
                else item.get("values")
            )
        else:
            continue
        if not col:
            continue
        if isinstance(candidate, (list, tuple, set)):
            vals = list(candidate)
        elif candidate is not None:
            vals = [candidate]
        else:
            vals = []
        clean_vals = _normalize_value_list(vals)
        if not clean_vals:
            continue
        normalized.append([col, clean_vals])
    return normalized


def _normalize_token_str(token: Any) -> Optional[str]:
    if token is None:
        return None
    text = str(token).strip().lower()
    if not text:
        return None
    base = text
    if base.endswith("'s") and len(base) > 2:
        base = base[:-2]
    elif base.endswith("ies") and len(base) > 3:
        base = base[:-3] + "y"
    elif base.endswith(("xes", "zes", "ches", "shes", "sses", "ees")) and len(base) > 3:
        base = base[:-2]
    elif base.endswith("s") and len(base) > 3 and not base.endswith("ss"):
        base = base[:-1]
    return base


def _normalize_token_list(tokens: Any) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()

    def _add(value: Any) -> None:
        norm = _normalize_token_str(value)
        if not norm:
            return
        if norm in seen:
            return
        seen.add(norm)
        result.append(norm)

    if isinstance(tokens, (list, tuple, set)):
        for item in tokens:
            if isinstance(item, (list, tuple, set)):
                for sub in item:
                    _add(sub)
            else:
                _add(item)
    elif tokens is not None:
        _add(tokens)
    return result


def _normalize_aggregations_list(items: Any) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    normalized: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, bool, str]] = set()
    for entry in items:
        if not isinstance(entry, dict):
            continue
        func = str(entry.get("func") or "").strip().upper()
        if not func:
            continue
        column_raw = entry.get("column")
        if column_raw == "*" or str(column_raw or "").strip() == "*":
            column = "*"
        else:
            column = str(column_raw or "").strip().upper()
        distinct = bool(entry.get("distinct"))
        alias_raw = entry.get("alias")
        alias = str(alias_raw or "").strip().upper() if alias_raw else None
        key = (func, column, distinct, alias or "")
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "func": func,
                "column": column if column else "*",
                "distinct": distinct,
                "alias": alias,
            }
        )
    normalized.sort(key=lambda item: (item["func"], item["column"], item["distinct"], item.get("alias") or ""))
    return normalized


def _eq_len_bin(length: int) -> str:
    if length <= 1:
        return "1"
    if length <= 3:
        return "2-3"
    if length <= 8:
        return "4-8"
    return "9+"


def _intent_shape_only(
    intent: Dict[str, Any],
    knobs: Optional[SignatureKnobs] = None,
    *,
    canonicalize: bool = True,
) -> Dict[str, Any]:
    knobs = knobs or signature_knobs()
    eq_info: Dict[str, Dict[str, Any]] = {}

    def _update_eq_info(column: str, length: int, types_iter: List[str]) -> None:
        family = _canonical_family(column, canonicalize=canonicalize)
        if not family:
            return
        info = eq_info.setdefault(family, {"len": 0, "types": set()})
        if length > info["len"]:
            info["len"] = length
        info["types"].update({str(t or "").strip().upper() for t in types_iter if str(t or "").strip()})

    for col, vals in _normalize_eq_filters_list(intent.get("eq_filters") or []):
        values = vals or []
        _update_eq_info(col, len(values), [_val_type(v) for v in values])

    if intent.get("eq"):
        for key, meta in (intent.get("eq") or {}).items():
            col = str(key or "").strip().upper()
            if not col:
                continue
            op = str((meta or {}).get("op") or "").strip().lower()
            inferred_len = 2 if op == "in" else 1
            meta_types = meta.get("types") or []
            _update_eq_info(col, inferred_len, meta_types)

    eq_shape: Dict[str, Dict[str, Any]] = {}
    for family, data in eq_info.items():
        family_norm = str(family or "").strip().upper()
        if not family_norm:
            continue
        length = int(data.get("len") or 0)
        types_set = data.get("types") or set()
        types = sorted(types_set)
        entry: Dict[str, Any] = {}
        if types:
            entry["types"] = types
        mode = knobs.eq_list_mode
        if length <= 1:
            if mode == "any_len":
                entry["op"] = "eq"
                entry["logic"] = "OR"
            else:
                entry["op"] = "eq"
        else:
            if mode == "any_len":
                entry["op"] = "eq"
                entry["logic"] = "OR"
            elif mode == "bins":
                entry["op"] = "eq"
                entry["logic"] = "OR"
                entry["len_bin"] = _eq_len_bin(length)
            else:
                entry["op"] = "in"
                entry["logic"] = "OR"
        eq_shape[family_norm] = entry

    if not eq_shape:
        original_eq = intent.get("eq") or {}
        if canonicalize and isinstance(original_eq, dict):
            for key, meta in original_eq.items():
                family_norm = _canonical_family(key, canonicalize=True)
                if not family_norm:
                    continue
                eq_shape[family_norm] = dict(meta or {})
        elif isinstance(original_eq, dict):
            eq_shape = dict(original_eq)

    fts_shape: Optional[Dict[str, Any]] = None
    fts_operator_raw = intent.get("fts_operator") or intent.get("fts_op") or "OR"
    fts_operator = str(fts_operator_raw).strip().upper() if fts_operator_raw else "OR"
    if fts_operator not in {"AND", "OR"}:
        fts_operator = "OR"

    groups = intent.get("fts_groups")
    if isinstance(groups, list) and groups:
        group_sizes: List[int] = []
        for group in groups:
            if isinstance(group, (list, tuple, set)):
                count = sum(1 for token in group if isinstance(token, str) and token.strip())
                if count:
                    group_sizes.append(count)
            elif isinstance(group, str) and group.strip():
                group_sizes.append(1)
        if group_sizes:
            if knobs.fts_shape == "groups_only":
                fts_shape = {
                    "groups": len(group_sizes),
                    "operator": fts_operator,
                }
            else:
                fts_shape = {
                    "groups": len(group_sizes),
                    "group_sizes": sorted(group_sizes),
                    "operator": fts_operator,
                }
    if fts_shape is None:
        fts_tokens = _normalize_token_list(intent.get("fts_tokens"))
        if fts_tokens:
            if knobs.fts_shape == "groups_only":
                fts_shape = {
                    "groups": len(fts_tokens),
                    "operator": fts_operator,
                }
            else:
                fts_shape = {
                    "groups": len(fts_tokens),
                    "group_sizes": [1] * len(fts_tokens),
                    "operator": fts_operator,
                }

    order = None
    if intent.get("order", {}).get("col"):
        order = {
            "col": str(intent["order"]["col"]).upper(),
            "desc": bool(intent["order"].get("desc", True)),
        }
    group_by = []
    if isinstance(intent.get("group_by"), list):
        group_by = sorted(
            {
                str(item or "").strip().upper()
                for item in intent.get("group_by")
                if isinstance(item, str) and str(item or "").strip()
            }
        )
    elif isinstance(intent.get("group_by"), str) and intent.get("group_by").strip():
        group_by = [str(intent.get("group_by")).strip().upper()]

    agg_shape = _normalize_aggregations_list(intent.get("aggregations"))

    return {"eq": eq_shape, "fts": fts_shape, "group_by": group_by, "order": order, "agg": agg_shape}


def _canon_signature_from_intent(intent: Dict[str, Any]) -> tuple[str, str, str]:
    shape = _intent_shape_only(intent, canonicalize=True)
    sig_json = _json.dumps(shape, separators=(",", ":"), sort_keys=True)
    sha256 = _hashlib.sha256(sig_json.encode("utf-8")).hexdigest()
    sha1 = _hashlib.sha1(sig_json.encode("utf-8")).hexdigest()
    return sha256, sha1, sig_json


def _signature_variants(intent: Dict[str, Any]) -> List[tuple[str, str, str]]:
    variants: List[tuple[str, str, str]] = []
    seen: set[str] = set()
    knobs = signature_knobs()

    def _append_variant(shape: Dict[str, Any]) -> None:
        sig_json = _json.dumps(shape, separators=(",", ":"), sort_keys=True)
        if sig_json in seen:
            return
        sha256 = _hashlib.sha256(sig_json.encode("utf-8")).hexdigest()
        sha1 = _hashlib.sha1(sig_json.encode("utf-8")).hexdigest()
        variants.append((sha256, sha1, sig_json))
        seen.add(sig_json)

    # Canonical signatures with current knobs
    canonical_current = _intent_shape_only(intent, knobs, canonicalize=True)
    _append_variant(canonical_current)

    # Canonical signature using default knobs (legacy knob behavior fallback)
    if knobs != DEFAULT_SIGNATURE_KNOBS:
        canonical_default = _intent_shape_only(intent, DEFAULT_SIGNATURE_KNOBS, canonicalize=True)
        _append_variant(canonical_default)

    # Legacy family naming (no canonical family collapse) with current knobs
    legacy_current = _intent_shape_only(intent, knobs, canonicalize=False)
    _append_variant(legacy_current)

    # Legacy family naming with default knobs
    if knobs != DEFAULT_SIGNATURE_KNOBS:
        legacy_default = _intent_shape_only(intent, DEFAULT_SIGNATURE_KNOBS, canonicalize=False)
        _append_variant(legacy_default)

    if _log_intent_match_enabled():
        try:
            fams = []
            for _sha256, _sha1, sig_json in variants:
                try:
                    sig_obj = _json.loads(sig_json)
                    fams.append(sorted((sig_obj.get("eq") or {}).keys()))
                except Exception:
                    fams.append([])
            log.info(
                {
                    "event": "rules.intent.variants",
                    "count": len(variants),
                    "families": fams,
                }
            )
        except Exception:
            pass

    return variants


def signature_variants(intent: Dict[str, Any]) -> List[tuple[str, str, str]]:
    return _signature_variants(intent)


def intent_shape(intent: Dict[str, Any], knobs: Optional[SignatureKnobs] = None) -> Dict[str, Any]:
    return _intent_shape_only(intent, knobs)


def _eq_coverage(question_eq: Any, rule_eq: Any) -> Optional[float]:
    question_norm = _normalize_eq_filters_list(question_eq or [])
    rule_norm = _normalize_eq_filters_list(rule_eq or [])
    if not rule_norm:
        return None

    q_map: Dict[str, set] = {}
    for col, values in question_norm:
        col_key = str(col or "").strip().upper()
        if not col_key:
            continue
        q_map[col_key] = {
            str(v or "").strip().upper()
            for v in values or []
            if str(v or "").strip()
        }

    total = 0
    matches = 0
    for col, values in rule_norm:
        col_key = str(col or "").strip().upper()
        if not col_key:
            continue
        total += 1
        if col_key not in q_map:
            continue
        if not values:
            matches += 1
            continue
        r_vals = {
            str(v or "").strip().upper()
            for v in values or []
            if str(v or "").strip()
        }
        if q_map[col_key] & r_vals:
            matches += 1
    if not total:
        return None
    return matches / total


def eq_coverage(question_eq: Any, rule_eq: Any) -> Optional[float]:
    return _eq_coverage(question_eq, rule_eq)


def _merge_eq_filters_prefer_question(current_eq, rule_eq):
    """Merge rule eq filters with question eq filters, preferring current question values."""

    try:
        alias_map = eq_alias_columns()
    except Exception:
        alias_map = {}

    alias_targets_index: Dict[str, Tuple[str, ...]] = {}
    canonical_for_targets: Dict[Tuple[str, ...], str] = {}

    def _score_alias(name: str) -> Tuple[int, int, str]:
        text = name or ""
        return (
            1 if text.endswith("S") and not text.endswith("SS") else 0,
            len(text),
            text,
        )

    for alias, cols in (alias_map or {}).items():
        alias_key = str(alias or "").strip().upper()
        cols_tuple = tuple(sorted(str(c or "").strip().upper() for c in (cols or []) if str(c or "").strip()))
        alias_targets_index[alias_key] = cols_tuple
        if not cols_tuple:
            continue
        current = canonical_for_targets.get(cols_tuple)
        if current is None or _score_alias(alias_key) > _score_alias(current):
            canonical_for_targets[cols_tuple] = alias_key

    def _canonical_alias(name: str) -> str:
        alias_key = str(name or "").strip().upper()
        cols_tuple = alias_targets_index.get(alias_key)
        if not cols_tuple:
            return alias_key
        return canonical_for_targets.get(cols_tuple, alias_key)

    def _canonical_keys(value: Any) -> set[str]:
        if value is None:
            return set()
        text = str(value).strip()
        if not text:
            return set()
        import re as _re
        upper = _re.sub(r"\s+", " ", text.upper())
        keys = {upper}
        if len(upper) > 3:
            if upper.endswith("IES"):
                keys.add(upper[:-3] + "Y")
            if upper.endswith("ES"):
                keys.add(upper[:-2])
            if upper.endswith("S"):
                keys.add(upper[:-1])
        return keys

    def _extract_values(item: Any) -> List[Any]:
        if isinstance(item, dict):
            if isinstance(item.get("values"), list):
                return list(item["values"])
            if item.get("val") is not None:
                return [item.get("val")]
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            raw_vals = item[1]
            if isinstance(raw_vals, (list, tuple, set)):
                return list(raw_vals)
            if raw_vals is not None:
                return [raw_vals]
        return []

    def _set_values(item: Any, values: List[Any]) -> Any:
        if isinstance(item, dict):
            if "values" in item and isinstance(item["values"], list):
                item["values"] = list(values)
            elif "val" in item:
                item["val"] = values[0] if values else item.get("val")
            else:
                item["values"] = list(values)
        elif isinstance(item, list) and len(item) >= 2:
            item[1] = list(values)
        elif isinstance(item, tuple) and len(item) >= 2:
            item = (item[0], list(values))
        return item

    def _align_question_with_rule(question_item: Any, rule_item: Any) -> Any:
        q_vals = _extract_values(question_item)
        r_vals = _extract_values(rule_item)
        if not q_vals or not r_vals:
            return question_item
        replaced: List[Any] = []
        used_rule: set[int] = set()
        changed = False
        import re as _re

        def _norm_alpha(value: Any) -> str:
            if value is None:
                return ""
            return _re.sub(r"[^A-Z0-9]+", "", str(value).upper())

        for q_val in q_vals:
            q_keys = _canonical_keys(q_val)
            match_val = None
            for idx, r_val in enumerate(r_vals):
                if idx in used_rule:
                    continue
                r_keys = _canonical_keys(r_val)
                if q_keys & r_keys:
                    match_val = r_val
                    used_rule.add(idx)
                    break
                if isinstance(q_val, str) and isinstance(r_val, str):
                    q_norm = _norm_alpha(q_val)
                    r_norm = _norm_alpha(r_val)
                    if q_norm and r_norm and (q_norm in r_norm or r_norm in q_norm):
                        match_val = r_val
                        used_rule.add(idx)
                        break
            if match_val is not None:
                replaced.append(match_val)
                if match_val != q_val:
                    changed = True
            else:
                replaced.append(q_val)
        if changed:
            question_item = _set_values(question_item, replaced)
        return question_item

    def _canonicalize(item: Any) -> Optional[Tuple[str, Any]]:
        if isinstance(item, dict):
            col = item.get("col") or item.get("field")
            if not isinstance(col, str):
                return None
            col_norm = _canonical_alias(col.strip().upper())
            if not col_norm:
                return None
            canon = dict(item)
            canon["col"] = col_norm
            return col_norm, canon
        if isinstance(item, (list, tuple)) and len(item) == 2:
            col = item[0]
            if not isinstance(col, (str, bytes)):
                return None
            col_norm = _canonical_alias(str(col).strip().upper())
            if not col_norm:
                return None
            vals = item[1]
            if isinstance(vals, (list, tuple, set)):
                values = list(vals)
            elif vals is None:
                values = []
            else:
                values = [vals]
            return col_norm, [col_norm, values]
        return None

    question_map: Dict[str, Any] = {}
    question_order: List[str] = []
    for item in current_eq or []:
        canonical = _canonicalize(item)
        if not canonical:
            continue
        col_norm, canon_item = canonical
        question_map[col_norm] = canon_item
        question_order.append(col_norm)

    out: List[Any] = []
    used: set[str] = set()

    for item in rule_eq or []:
        canonical = _canonicalize(item)
        if not canonical:
            continue
        col_norm, canon_item = canonical
        if col_norm in question_map:
            question_item = question_map[col_norm]
            question_item = _align_question_with_rule(question_item, canon_item)
            question_map[col_norm] = question_item
            if col_norm not in used:
                out.append(question_item)
                used.add(col_norm)
        else:
            out.append(canon_item)

    for col_norm in question_order:
        if col_norm in question_map and col_norm not in used:
            out.append(question_map[col_norm])
            used.add(col_norm)

    return out


def load_rules_for_question(engine, qnorm: str, intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns merged hints from dw_rules using precedence:
      intent_sha -> rule_signature -> question_norm (exact/global)
    Merges EQ such that question-provided values win for the same columns.
    """
    knobs = signature_knobs()
    variants = _signature_variants(intent or {})
    ask_shape = _intent_shape_only(intent or {}, knobs)
    matched_stage: Optional[str] = None
    matched_variant: Optional[int] = None
    rows: List[Any] = []
    merged: Dict[str, Any] = {}
    mismatch_reason: Optional[str] = None
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

        for idx, (sha256, sha1, sig_json) in enumerate(variants):
            rows = _fetch("intent_sha IN (:sha1, :sha256)", {"sha1": sha1, "sha256": sha256})
            if rows:
                matched_stage = "intent_sha"
                matched_variant = idx
                break
            rows = _fetch("rule_signature = :sig", {"sig": sig_json})
            if rows:
                matched_stage = "rule_signature"
                matched_variant = idx
                break
        if not rows:
            rows = _fetch("(question_norm = :q OR COALESCE(question_norm,'') = '')", {"q": qnorm})
            if rows:
                matched_stage = "question_norm"
            else:
                mismatch_reason = "no_rule"

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
        elif k == "group_by":
            if data.get("group_by"):
                merged["group_by"] = data.get("group_by")
            if data.get("gross") is not None:
                merged["gross"] = bool(data.get("gross"))
        elif k == "agg":
            normalized_aggs = _normalize_aggregations_list(data.get("aggregations"))
            if normalized_aggs:
                merged["aggregations"] = normalized_aggs
        elif k == "order_by":
            merged["order"] = {
                "col": (data.get("sort_by") or "REQUEST_DATE"),
                "desc": bool(data.get("sort_desc", True)),
            }
        elif k == "fts":
            if data.get("tokens"):
                merged["fts_tokens"] = data.get("tokens")

    eq_coverage_value = _eq_coverage(intent.get("eq_filters"), eq_from_rules)
    eq_coverage_enforced = False
    if (
        eq_from_rules
        and eq_coverage_value is not None
        and knobs.eq_list_min_coverage > 0
        and eq_coverage_value < knobs.eq_list_min_coverage
    ):
        mismatch_reason = mismatch_reason or "eq_coverage"
        eq_from_rules = []
        eq_coverage_enforced = True

    if eq_from_rules or intent.get("eq_filters"):
        merged["eq_filters"] = _merge_eq_filters_prefer_question(
            intent.get("eq_filters"), eq_from_rules
        )
    if not merged.get("aggregations"):
        question_aggs = _normalize_aggregations_list(intent.get("aggregations"))
        if question_aggs:
            merged["aggregations"] = question_aggs
    if not merged.get("group_by") and intent.get("group_by"):
        merged["group_by"] = intent.get("group_by")

    if _log_intent_match_enabled():
        try:
            log.info(
                {
                    "event": "rules.intent.match",
                    "source": "learning_store",
                    "question_norm": qnorm,
                    "match_stage": matched_stage,
                    "matched_variant": matched_variant,
                    "variants": len(variants),
                    "rows": len(rows or []),
                    "signature": ask_shape,
                    "knobs": knobs._asdict(),
                    "fallback_used": bool(matched_variant and matched_variant > 0),
                    "mismatch_reason": mismatch_reason,
                    "eq_coverage": eq_coverage_value,
                    "eq_coverage_enforced": eq_coverage_enforced,
                }
            )
        except Exception:
            pass
    return merged
