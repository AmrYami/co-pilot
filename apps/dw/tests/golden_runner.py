# apps/dw/tests/golden_runner.py
from __future__ import annotations
import logging
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from flask import Flask

from .yaml_tags import GoldenLoader, register_yaml_tags

# Stable, package-relative path to the golden YAML file
GOLDEN_PATH = Path(__file__).with_name("golden_dw_contracts.yaml")
DEFAULT_NS = "dw::common"


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


def _canon_sql(value: str) -> str:
    """Normalise SQL by removing all whitespace and lowercasing."""
    return re.sub(r"\s+", "", (value or "")).lower()


def _ensure_date(v: Any) -> Any:
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        try:
            return date.fromisoformat(v)
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
        if isinstance(value, date):
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
    )

def _check_expectations(case: GoldenCase, resp: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True

    sql = (resp or {}).get("sql") or ""
    sql_canon = _canon_sql(sql)
    meta = (resp or {}).get("meta") or {}
    intent = ((resp or {}).get("debug") or {}).get("intent") or meta.get("clarifier_intent") or {}

    # 1) sql contains
    for frag in case.expect_sql_contains:
        if _canon_sql(frag) not in sql_canon:
            ok = False
            reasons.append(f"SQL does not contain expected fragment: {frag}")

    for frag in case.expect_sql_not_contains:
        if frag in sql:
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
