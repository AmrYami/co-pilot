# apps/dw/tests/golden_runner.py
from __future__ import annotations
import datetime as _dt
import logging
import os
import re
from calendar import monthrange
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from dateutil.relativedelta import relativedelta
from flask import Flask


class GoldenLoader(yaml.SafeLoader):
    """Custom YAML loader supporting temporal tags for golden tests."""


def _today() -> _dt.date:
    env_v = os.getenv("GOLDEN_TODAY")
    if env_v:
        try:
            return _dt.date.fromisoformat(env_v)
        except ValueError:
            pass
    return _dt.date.today()


def _iso(d: _dt.date) -> str:
    return d.isoformat()


def _scalar(loader: yaml.Loader, node: yaml.Node) -> Any:
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    return loader.construct_object(node)


def _maybe_date(value: Any) -> Optional[_dt.date]:
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return _dt.date.fromisoformat(s)
        except ValueError:
            return None
    return None


def _parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        try:
            return int(s)
        except ValueError:
            return default
    try:
        return int(value)
    except Exception:
        return default


def _start_of_month(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    base = _maybe_date(raw)
    if base:
        target = base.replace(day=1)
    else:
        offset = _parse_int(raw, 0)
        target = _today().replace(day=1) + relativedelta(months=offset)
    return _iso(target)


def _end_of_month(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    base = _maybe_date(raw)
    if base:
        target = base.replace(day=1)
    else:
        offset = _parse_int(raw, 0)
        target = _today().replace(day=1) + relativedelta(months=offset)
    last = monthrange(target.year, target.month)[1]
    return _iso(target.replace(day=last))


def _start_of_last_month(loader: Any, node: yaml.Node) -> str:
    _scalar(loader, node)
    target = _today().replace(day=1) - relativedelta(months=1)
    return _iso(target)


def _end_of_last_month(loader: Any, node: yaml.Node) -> str:
    _scalar(loader, node)
    base = _today().replace(day=1) - relativedelta(months=1)
    last = monthrange(base.year, base.month)[1]
    return _iso(base.replace(day=last))


def _start_of_prev_months(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 0)
    target = _today().replace(day=1) - relativedelta(months=n)
    return _iso(target)


def _end_of_prev_months(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 0)
    base = _today().replace(day=1) - relativedelta(months=n)
    last = monthrange(base.year, base.month)[1]
    return _iso(base.replace(day=last))


def _year_from_value(value: Any) -> int:
    base = _today()
    candidate = _maybe_date(value)
    if candidate:
        return candidate.year
    try:
        if value is None:
            return base.year
        if isinstance(value, int):
            num = value
        else:
            num = int(str(value).strip())
    except Exception:
        return base.year
    if num >= 1000:
        return num
    return (base + relativedelta(years=num)).year


def _start_of_year(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    year = _year_from_value(raw)
    return _iso(_dt.date(year, 1, 1))


def _end_of_year(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    year = _year_from_value(raw)
    return _iso(_dt.date(year, 12, 31))


def _start_of_prev_years(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 1)
    year = _today().year - n
    return _iso(_dt.date(year, 1, 1))


def _end_of_prev_years(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 1)
    year = _today().year - n
    return _iso(_dt.date(year, 12, 31))


def _quarter_start(d: _dt.date) -> _dt.date:
    quarter = (d.month - 1) // 3
    month = quarter * 3 + 1
    return _dt.date(d.year, month, 1)


def _start_of_quarter(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    base = _maybe_date(raw) or _today()
    offset = 0 if _maybe_date(raw) else _parse_int(raw, 0)
    start = _quarter_start(base)
    return _iso(start + relativedelta(months=3 * offset))


def _end_of_quarter(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    base = _maybe_date(raw) or _today()
    offset = 0 if _maybe_date(raw) else _parse_int(raw, 0)
    start = _quarter_start(base) + relativedelta(months=3 * offset)
    return _iso(start + relativedelta(months=3) - _dt.timedelta(days=1))


def _start_of_last_quarter(loader: Any, node: yaml.Node) -> str:
    _scalar(loader, node)
    start_current = _quarter_start(_today())
    prev = start_current - relativedelta(months=3)
    return _iso(prev)


def _end_of_last_quarter(loader: Any, node: yaml.Node) -> str:
    _scalar(loader, node)
    start_current = _quarter_start(_today())
    prev = start_current - relativedelta(months=3)
    return _iso(prev + relativedelta(months=3) - _dt.timedelta(days=1))


def _days_ago(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 0)
    return _iso(_today() - _dt.timedelta(days=n))


def _months_ago(loader: Any, node: yaml.Node) -> str:
    raw = _scalar(loader, node)
    n = _parse_int(raw, 0)
    return _iso(_today() - relativedelta(months=n))


def _construct_today(loader: Any, node: yaml.Node) -> str:
    _scalar(loader, node)
    return _iso(_today())


def _generic_start(loader: Any, suffix: str, node: yaml.Node) -> str:
    s = (suffix or "").strip().lower().replace('-', '_')
    if s in {"", "month", "months", "this_month"}:
        return _start_of_month(loader, node)
    if s in {"last_month", "previous_month"}:
        return _start_of_last_month(loader, node)
    if s.startswith("prev_month"):
        return _start_of_prev_months(loader, node)
    if s in {"year", "this_year"}:
        return _start_of_year(loader, node)
    if s in {"last_year", "previous_year"}:
        year = _today().year - 1
        return _iso(_dt.date(year, 1, 1))
    if s.startswith("prev_year"):
        return _start_of_prev_years(loader, node)
    if s in {"quarter", "this_quarter"}:
        return _start_of_quarter(loader, node)
    if s in {"last_quarter", "previous_quarter"}:
        return _start_of_last_quarter(loader, node)
    return _iso(_today())


def _generic_end(loader: Any, suffix: str, node: yaml.Node) -> str:
    s = (suffix or "").strip().lower().replace('-', '_')
    if s in {"", "month", "months", "this_month"}:
        return _end_of_month(loader, node)
    if s in {"last_month", "previous_month"}:
        return _end_of_last_month(loader, node)
    if s.startswith("prev_month"):
        return _end_of_prev_months(loader, node)
    if s in {"year", "this_year"}:
        return _end_of_year(loader, node)
    if s in {"last_year", "previous_year"}:
        year = _today().year - 1
        return _iso(_dt.date(year, 12, 31))
    if s.startswith("prev_year"):
        return _end_of_prev_years(loader, node)
    if s in {"quarter", "this_quarter"}:
        return _end_of_quarter(loader, node)
    if s in {"last_quarter", "previous_quarter"}:
        return _end_of_last_quarter(loader, node)
    return _iso(_today())


GoldenLoader.add_constructor("!today", _construct_today)
GoldenLoader.add_constructor("!start_of_month", _start_of_month)
GoldenLoader.add_constructor("!end_of_month", _end_of_month)
GoldenLoader.add_constructor("!start_of_last_month", _start_of_last_month)
GoldenLoader.add_constructor("!end_of_last_month", _end_of_last_month)
GoldenLoader.add_constructor("!days_ago", _days_ago)
GoldenLoader.add_constructor("!months_ago", _months_ago)
GoldenLoader.add_constructor("!start_of_prev_months", _start_of_prev_months)
GoldenLoader.add_constructor("!end_of_prev_months", _end_of_prev_months)
GoldenLoader.add_constructor("!start_of_year", _start_of_year)
GoldenLoader.add_constructor("!end_of_year", _end_of_year)
GoldenLoader.add_constructor("!start_of_prev_years", _start_of_prev_years)
GoldenLoader.add_constructor("!end_of_prev_years", _end_of_prev_years)
GoldenLoader.add_constructor("!start_of_quarter", _start_of_quarter)
GoldenLoader.add_constructor("!end_of_quarter", _end_of_quarter)
GoldenLoader.add_constructor("!start_of_last_quarter", _start_of_last_quarter)
GoldenLoader.add_constructor("!end_of_last_quarter", _end_of_last_quarter)
GoldenLoader.add_multi_constructor("!start_of_", _generic_start)
GoldenLoader.add_multi_constructor("!end_of_", _generic_end)

# NOTE: Test runner helpers should be resilient to harmless SQL shape differences
# (aliases, bind names, spacing). We keep comments here in English by request.

MEASURE_ALIASES = ("MEASURE", "TOTAL_GROSS", "GROSS_VALUE", "NET_VALUE", "CNT", "TOTAL", "VALUE")
DATE_START_SYNS = (":date_start", ":ds")
DATE_END_SYNS = (":date_end", ":de")


def _normalize_sql(s: str) -> str:
    """Lower-case and normalize whitespace for tolerant comparisons."""
    s = (s or "").replace("`", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r",\s+", ",", s)
    return s.strip().lower()


def _contains_any(sql: str, patterns: Iterable[str]) -> bool:
    return any(re.search(p, sql, flags=re.I) for p in patterns)

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
    sql_norm = _normalize_sql(sql)
    missing: List[str] = []
    for frag in fragments:
        frag_norm = _normalize_sql(frag)
        if not frag_norm:
            continue
        if frag_norm == "order by measure desc":
            alias_pattern = "|".join(alias.lower() for alias in MEASURE_ALIASES)
            pattern = rf"order\s+by\s+({alias_pattern})\s+desc"
            if not re.search(pattern, sql_norm, flags=re.I):
                missing.append(frag)
            continue
        match = re.match(r"fetch\s+first\s+(\d+)\s+rows\s+only", frag_norm, flags=re.I)
        if match:
            n = match.group(1)
            candidates = [
                rf"fetch\s+first\s+{n}\s+rows\s+only",
                r"fetch\s+first\s+:top_n\s+rows\s+only",
            ]
            if not _contains_any(sql, candidates):
                missing.append(f"FETCH FIRST {n} ROWS ONLY")
            continue
        if "request_date between :date_start and :date_end" in frag_norm:
            patterns = [
                rf"request_date\s+between\s+{ds}\s+and\s+{de}" for ds in DATE_START_SYNS for de in DATE_END_SYNS
            ]
            if not _contains_any(sql, patterns):
                missing.append(frag)
            continue
        if "start_date <= :date_end" in frag_norm:
            patterns = [rf"start_date\s*<=\s*{de}" for de in DATE_END_SYNS]
            if not _contains_any(sql, patterns):
                missing.append(frag)
            continue
        if "end_date >= :date_start" in frag_norm:
            patterns = [rf"end_date\s*>=\s*{ds}" for ds in DATE_START_SYNS]
            if not _contains_any(sql, patterns):
                missing.append(frag)
            continue
        if frag_norm not in sql_norm:
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
