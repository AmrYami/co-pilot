# apps/dw/tests/golden_runner.py
from __future__ import annotations
import datetime as _dt
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# NOTE: Test runner helpers should be resilient to harmless SQL shape differences
# (aliases, bind names, spacing). We keep comments here in English by request.

MEASURE_ALIASES = ("MEASURE", "TOTAL_GROSS", "GROSS_VALUE", "NET_VALUE", "CNT", "TOTAL", "VALUE")
DATE_START_SYNS = (":date_start", ":ds")
DATE_END_SYNS = (":date_end", ":de")


def _normalize_sql(s: str) -> str:
    """Uppercase and normalize whitespace/punctuation for tolerant comparisons."""
    s = (s or "").replace("`", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r",\s+", ",", s)
    s = s.strip()
    return s.upper()


def _contains_any(sql: str, patterns: Iterable[str]) -> bool:
    return any(re.search(p, sql, flags=re.I) for p in patterns)

import yaml
from flask import Flask

from .yaml_tags import GoldenLoader, register_yaml_tags

# Stable, package-relative path to the golden YAML file
GOLDEN_PATH = Path(__file__).with_name("golden_dw_contracts.yaml")
DEFAULT_NS = "dw::common"

# NOTE: keep comments in English inside code.

GROSS_EXPR = (
    "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
    "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) ELSE NVL(VAT,0) END"
)
NET_EXPR = "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


@dataclass
class GoldenCase:
    question: str
    namespace: str = DEFAULT_NS
    prefixes: List[str] = field(default_factory=list)
    auth_email: str = "golden@local"
    full_text_search: bool = False
    binds: Dict[str, Any] = field(default_factory=dict)
    # Expectations (all optional)
    expect_sql_contains: List[str] = field(default_factory=list)
    expect_sql_not_contains: List[str] = field(default_factory=list)
    expect_group_by: List[str] = field(default_factory=list)
    expect_order_by: Optional[str] = None
    expect_date_col: Optional[str] = None  # e.g. "REQUEST_DATE", "OVERLAP", "END_DATE"
    expect_agg: Optional[str] = None       # e.g. "count", "sum"
    expect_top_n: Optional[int] = None
    assertions: Dict[str, Any] = field(default_factory=dict)

def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            # Register custom tags then load with our loader:
            register_yaml_tags()
            data = yaml.load(f, Loader=GoldenLoader) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Golden YAML root must be a mapping, got: {type(data).__name__}")
        if "cases" not in data or not isinstance(data["cases"], list):
            raise ValueError("Golden YAML missing a 'cases' list.")
        data.setdefault("namespace", DEFAULT_NS)
        return data
    except FileNotFoundError:
        logging.error("Golden YAML not found at %s", path)
        return {}
    except Exception as e:
        logging.exception("Failed to load golden YAML: %s", e)
        return {}


def _dense(s: str) -> str:
    """Lowercase + strip all whitespace for flexible substring checks."""
    return re.sub(r"\s+", "", (s or "")).lower()


def _flatten(s: str) -> str:
    """Normalize SQL to be whitespace- and punctuation-agnostic for substring checks."""
    return _dense(s).replace("`", "")


def _must_contain(sql: str, frag: str, reasons: List[str]) -> None:
    if not frag:
        return
    if _flatten(frag) not in _flatten(sql):
        reasons.append(f"SQL does not contain expected fragment: {frag}")


def assert_overlap_present(case: GoldenCase | None, sql: str, reasons: List[str]) -> None:
    _must_contain(sql, "START_DATE <= :date_end", reasons)
    _must_contain(sql, "END_DATE >= :date_start", reasons)


def assert_order_direction(sql: str, expect_desc: bool, reasons: List[str]) -> None:
    dir_kw = "DESC" if expect_desc else "ASC"
    if "ORDER BY" in sql.upper() and dir_kw not in sql.upper():
        reasons.append(f"ORDER BY direction not {dir_kw}")


def _contains_all(sql: str, fragments: List[str]) -> List[str]:
    """Return list of missing fragments after normalization; empty list means all found."""
    sql_n = _normalize_sql(sql)
    missing: List[str] = []
    for frag in fragments:
        frag_n = _normalize_sql(frag)
        if not frag_n:
            continue
        if frag_n == "ORDER BY MEASURE DESC":
            pattern = r"ORDER\s+BY\s+(%s)\s+DESC" % "|".join(MEASURE_ALIASES)
            if not re.search(pattern, sql_n, flags=re.I):
                missing.append(frag)
            continue
        match = re.match(r"FETCH\s+FIRST\s+(\d+)\s+ROWS\s+ONLY", frag_n, flags=re.I)
        if match:
            n = match.group(1)
            candidates = [
                rf"FETCH\s+FIRST\s+{n}\s+ROWS\s+ONLY",
                r"FETCH\s+FIRST\s+:TOP_N\s+ROWS\s+ONLY",
            ]
            if not _contains_any(sql_n, candidates):
                missing.append(f"FETCH FIRST {n} ROWS ONLY")
            continue
        if "REQUEST_DATE BETWEEN :DATE_START AND :DATE_END" in frag_n:
            patterns = [
                rf"REQUEST_DATE\s+BETWEEN\s+{ds}\s+AND\s+{de}" for ds in DATE_START_SYNS for de in DATE_END_SYNS
            ]
            if not _contains_any(sql_n, patterns):
                missing.append(frag)
            continue
        if "START_DATE <= :DATE_END" in frag_n:
            patterns = [rf"START_DATE\s*<=\s*{de}" for de in DATE_END_SYNS]
            if not _contains_any(sql_n, patterns):
                missing.append(frag)
            continue
        if "END_DATE >= :DATE_START" in frag_n:
            patterns = [rf"END_DATE\s*>=\s*{ds}" for ds in DATE_START_SYNS]
            if not _contains_any(sql_n, patterns):
                missing.append(frag)
            continue
        if frag_n not in sql_n:
            missing.append(frag)
    return missing


def _assert_overlap(sql: str) -> List[str]:
    """Expect overlap window predicates to be present."""
    reasons: List[str] = []
    assert_overlap_present(None, sql, reasons)
    return reasons


def _assert_end_only(sql: str) -> List[str]:
    """Expect END_DATE window predicates to be present."""
    reasons: List[str] = []
    _must_contain(sql, "END_DATE BETWEEN :date_start AND :date_end", reasons)
    return reasons


def _assert_request_window(sql: str) -> List[str]:
    reasons: List[str] = []
    _must_contain(sql, "REQUEST_DATE BETWEEN :date_start AND :date_end", reasons)
    return reasons


def _assert_order(sql: str, metric: str, direction: str) -> List[str]:
    """metric in {'gross','net','measure'}; direction in {'asc','desc'}."""
    reasons: List[str] = []
    metric_key = (metric or "measure").lower()
    dir_key = (direction or "desc").lower()
    if metric_key == "gross":
        _must_contain(sql, f"ORDER BY {GROSS_EXPR} {dir_key.upper()}", reasons)
    elif metric_key == "net":
        _must_contain(sql, f"ORDER BY {NET_EXPR} {dir_key.upper()}", reasons)
    elif metric_key == "measure":
        _must_contain(sql, f"ORDER BY MEASURE {dir_key.upper()}", reasons)
    else:
        _must_contain(sql, "ORDER BY", reasons)
        assert_order_direction(sql, dir_key == "desc", reasons)
    return reasons


def _assert_group_by(sql: str, cols: List[str]) -> List[str]:
    columns = [c.strip() for c in cols if c and c.strip()]
    if not columns:
        return []
    reasons: List[str] = []
    _must_contain(sql, "GROUP BY " + ", ".join(columns), reasons)
    return reasons


def _assert_owner_vs_oul_mismatch(sql: str) -> List[str]:
    reasons: List[str] = []
    _must_contain(sql, "DEPARTMENT_OUL IS NOT NULL", reasons)
    _must_contain(sql, "NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')", reasons)
    _must_contain(sql, "ORDER BY CNT DESC", reasons)
    return reasons


def _ensure_date(v: Any) -> Any:
    if isinstance(v, _dt.date):
        return v
    if isinstance(v, str):
        try:
            return _dt.date.fromisoformat(v)
        except ValueError:
            return v
    return v


def _normalize_binds(raw_binds: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not raw_binds:
        return {}
    return {k: _ensure_date(v) for k, v in raw_binds.items()}


def _serialize_binds_for_json(binds: Dict[str, Any]) -> Dict[str, Any]:
    serialised: Dict[str, Any] = {}
    for key, value in binds.items():
        if isinstance(value, _dt.date):
            serialised[key] = value.isoformat()
        else:
            serialised[key] = value
    return serialised


def _ensure_str_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value]


def _hydrate_case(raw: Dict[str, Any]) -> GoldenCase:
    expect = raw.get("expect") or {}
    return GoldenCase(
        question = raw.get("question", "").strip(),
        namespace = (raw.get("namespace") or DEFAULT_NS).strip(),
        prefixes = raw.get("prefixes", []) or [],
        auth_email = raw.get("auth_email", "golden@local"),
        full_text_search = bool(raw.get("full_text_search", False)),
        binds = _normalize_binds(raw.get("binds")),
        expect_sql_contains = _ensure_str_list(raw.get("expect_sql_contains") or expect.get("sql_like")),
        expect_sql_not_contains = _ensure_str_list(expect.get("must_not")),
        expect_group_by = _ensure_str_list(raw.get("expect_group_by")),
        expect_order_by = raw.get("expect_order_by"),
        expect_date_col = raw.get("expect_date_col"),
        expect_agg = raw.get("expect_agg"),
        expect_top_n = raw.get("expect_top_n"),
        assertions = raw.get("assertions") or {},
    )

def _check_expectations(case: GoldenCase, resp: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True

    sql = (resp or {}).get("sql") or ""
    meta = (resp or {}).get("meta") or {}
    intent = ((resp or {}).get("debug") or {}).get("intent") or meta.get("clarifier_intent") or {}

    # 1) sql contains
    missing_fragments = _contains_all(sql, case.expect_sql_contains)
    if missing_fragments:
        ok = False
        reasons.extend([f"SQL does not contain expected fragment: {frag}" for frag in missing_fragments])

    sql_dense = _dense(sql)
    for frag in case.expect_sql_not_contains:
        if _dense(frag) in sql_dense:
            ok = False
            reasons.append(f"SQL unexpectedly contained fragment: {frag}")

    # 2) group by
    if case.expect_group_by:
        up = sql.upper()
        if "GROUP BY" not in up:
            ok = False
            reasons.append("Expected GROUP BY, but not found.")
        else:
            for col in case.expect_group_by:
                if col.upper() not in up:
                    ok = False
                    reasons.append(f"Expected GROUP BY column missing: {col}")

    # 3) order by
    if case.expect_order_by:
        if case.expect_order_by.upper() not in sql.upper():
            ok = False
            reasons.append(f"Expected ORDER BY on: {case.expect_order_by}")

    # 3b) structured assertions
    structured_reasons: List[str] = []
    assertions = case.assertions or {}
    if assertions.get("overlap"):
        structured_reasons.extend(_assert_overlap(sql))
    if assertions.get("end_only"):
        structured_reasons.extend(_assert_end_only(sql))
    if assertions.get("request_window"):
        structured_reasons.extend(_assert_request_window(sql))
    if "order" in assertions:
        order_cfg = assertions.get("order") or {}
        structured_reasons.extend(
            _assert_order(sql, order_cfg.get("metric", "measure"), order_cfg.get("dir", "desc"))
        )
    if "group_by" in assertions:
        structured_reasons.extend(_assert_group_by(sql, assertions.get("group_by") or []))
    if assertions.get("owner_vs_oul_mismatch"):
        structured_reasons.extend(_assert_owner_vs_oul_mismatch(sql))
    if structured_reasons:
        ok = False
        reasons.extend(structured_reasons)

    # 4) date column intent
    if case.expect_date_col:
        # tolerate either meta.suggested_date_column or intent.date_column
        date_col = meta.get("suggested_date_column") or intent.get("date_column")
        if (date_col or "").upper() != case.expect_date_col.upper():
            ok = False
            reasons.append(f"Expected date column '{case.expect_date_col}' but got '{date_col}'")

    # 5) aggregation intent
    if case.expect_agg:
        agg = (intent.get("agg") or meta.get("agg") or "").lower()
        if agg != case.expect_agg.lower():
            ok = False
            reasons.append(f"Expected agg '{case.expect_agg}', got '{agg}'")

    # 6) top N
    if case.expect_top_n is not None:
        top_n = meta.get("binds", {}).get("top_n")
        if top_n != case.expect_top_n:
            ok = False
            reasons.append(f"Expected top_n={case.expect_top_n}, got {top_n}")

    return ok, reasons

def run_golden_tests(
    flask_app: Optional[Flask] = None,
    namespace: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    ns_clean = namespace.strip() if isinstance(namespace, str) else None

    data = _load_yaml(GOLDEN_PATH)
    if not data:
        return {
            "ok": False,
            "total": 0,
            "passed": 0,
            "results": [],
            "error": "Golden YAML failed to load.",
            "namespace": ns_clean or DEFAULT_NS,
        }

    raw_cases: List[Dict[str, Any]] = data.get("cases", [])
    default_namespace = data.get("namespace", DEFAULT_NS)

    # Namespace filter with fallback to all cases if none matched
    if ns_clean:
        cases = [c for c in raw_cases if (c.get("namespace") or default_namespace) == ns_clean]
        if not cases:
            logging.info(
                "No golden cases matched namespace '%s'. Running all cases (%d).",
                ns_clean,
                len(raw_cases),
            )
            cases = raw_cases
    else:
        cases = raw_cases

    # Limit if requested
    if isinstance(limit, int) and limit > 0:
        cases = cases[:limit]

    # If we do not have an app to call, just report the count
    if not flask_app:
        return {
            "ok": True,
            "total": len(cases),
            "passed": 0,
            "results": [],
            "namespace": ns_clean or default_namespace,
        }

    results: List[Dict[str, Any]] = []
    passed_count = 0

    with flask_app.test_client() as client:
        for idx, raw in enumerate(cases, start=1):
            case = _hydrate_case(raw)
            payload = {
                "prefixes": case.prefixes,
                "question": case.question,
                "auth_email": case.auth_email,
                "full_text_search": case.full_text_search,
            }
            if case.namespace:
                payload["namespace"] = case.namespace
            if case.binds:
                payload["binds"] = _serialize_binds_for_json(case.binds)
            try:
                rv = client.post("/dw/answer", json=payload)
                data = rv.get_json(silent=True) or {}
            except Exception as e:
                data = {"ok": False, "error": f"exception: {e}"}

            ok, reasons = _check_expectations(case, data)
            if ok:
                passed_count += 1

            results.append({
                "idx": idx,
                "namespace": case.namespace,
                "question": case.question,
                "passed": ok,
                "reasons": reasons,
                "binds": _serialize_binds_for_json(case.binds),
                "sql": data.get("sql"),
                "meta": data.get("meta"),
            })

    return {
        "ok": True,
        "total": len(results),
        "passed": passed_count,
        "results": results,
        "namespace": ns_clean or default_namespace,
    }
