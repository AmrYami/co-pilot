# apps/dw/tests/golden_runner.py
# Runs golden tests by asking the planner to derive SQL (no execution), then compares to expected SQL.

from __future__ import annotations
import re
import datetime as dt
from pathlib import Path
from typing import Dict, Any, List

try:
    # Prefer a dedicated derive function exposed by dw app.
    from apps.dw.app import derive_sql_for_test  # you’ll add this helper below
except Exception:
    derive_sql_for_test = None

GOLDEN_PATH = Path(__file__).with_name("golden_dw_contracts.yaml")

def _load_yaml(path: Path) -> Dict[str, Any]:
    import yaml
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _normalize_sql(s: str) -> str:
    # collapse whitespace and upper-case keywords for lenient matching
    s = re.sub(r"\s+", " ", s.strip())
    return s

def _compute_window(window_key: str) -> Dict[str, Any]:
    """Return a dict of binds needed for date windows, without tying to Oracle types here.
    The caller (/admin/run_golden) should convert to real date binds if it wishes to execute;
    for comparison we only need to ensure the planner *inserts* the correct WHERE and order/limit.
    """
    today = dt.date.today()
    first_of_this_month = today.replace(day=1)
    first_of_last_month = (first_of_this_month - dt.timedelta(days=1)).replace(day=1)
    last_of_last_month = first_of_this_month - dt.timedelta(days=1)

    def add_months(d: dt.date, months: int) -> dt.date:
        year = d.year + (d.month - 1 + months) // 12
        month = (d.month - 1 + months) % 12 + 1
        day = min(d.day, [31,29 if year%4==0 and (year%100!=0 or year%400==0) else 28,31,30,31,30,31,31,30,31,30,31][month-1])
        return dt.date(year, month, day)

    if window_key == "last_month_overlaps":
        return {"date_start": first_of_last_month.isoformat(), "date_end": last_of_last_month.isoformat()}
    if window_key == "last_8_months_overlaps":
        start = add_months(today, -8)
        return {"date_start": start.isoformat(), "date_end": today.isoformat()}
    if window_key == "last_quarter_overlaps":
        # previous calendar quarter
        m = ((today.month - 1) // 3) * 3 + 1
        q_start_this = dt.date(today.year, m, 1)
        start = add_months(q_start_this, -3)
        end = q_start_this - dt.timedelta(days=1)
        return {"date_start": start.isoformat(), "date_end": end.isoformat()}
    if window_key == "last_90_days_overlaps":
        return {"date_start": (today - dt.timedelta(days=90)).isoformat(), "date_end": today.isoformat()}
    if window_key == "last_6_months_overlaps":
        return {"date_start": add_months(today, -6).isoformat(), "date_end": today.isoformat()}
    if window_key == "last_12_months_requestdate":
        return {"date_start": add_months(today, -12).isoformat(), "date_end": today.isoformat()}
    if window_key == "last_180_days_overlaps":
        return {"date_start": (today - dt.timedelta(days=180)).isoformat(), "date_end": today.isoformat()}
    if window_key == "this_year_overlaps":
        start = dt.date(today.year, 1, 1)
        return {"date_start": start.isoformat(), "date_end": today.isoformat()}
    if window_key == "year_2023_requestdate":
        return {"date_start": "2023-01-01", "date_end": "2023-12-31"}
    if window_key == "year_2024_overlaps":
        return {"date_start": "2024-01-01", "date_end": "2024-12-31"}
    if window_key == "ytd_2024_overlaps":
        return {"date_start": "2024-01-01", "date_end": today.isoformat()}
    if window_key == "next_30_days_enddate":
        return {"date_start": today.isoformat(), "date_end": (today + dt.timedelta(days=30)).isoformat()}
    if window_key == "next_90_days_enddate":
        return {"date_start": today.isoformat(), "date_end": (today + dt.timedelta(days=90)).isoformat()}
    if window_key == "last_month_requestdate":
        return {"date_start": first_of_last_month.isoformat(), "date_end": last_of_last_month.isoformat()}
    if window_key == "last_365_days_overlaps":
        return {"date_start": (today - dt.timedelta(days=365)).isoformat(), "date_end": today.isoformat()}
    # Default: no binds
    return {}

def run_golden_tests(namespace: str = "dw::common") -> Dict[str, Any]:
    data = _load_yaml(GOLDEN_PATH)
    results = []
    passed = 0
    total = 0

    for t in data.get("tests", []):
        total += 1
        q = t["question"]
        expect_sql = _normalize_sql(t["expect_sql"])
        window_key = t.get("window")
        binds_hint = _compute_window(window_key) if window_key else {}
        # supply common aux binds when needed
        if ":top_n" in expect_sql:
            binds_hint.setdefault("top_n", 10)
        if ":entity_no" in expect_sql:
            binds_hint.setdefault("entity_no", "E-123")
        if ":requester" in expect_sql:
            binds_hint.setdefault("requester", "john@corp")
        if ":min_n" in expect_sql:
            binds_hint.setdefault("min_n", 5)
        if all(k in expect_sql for k in (":period_start", ":period_end")):
            # Default to this-year Q1 as example period
            binds_hint.setdefault("period_start", f"{dt.date.today().year}-01-01")
            binds_hint.setdefault("period_end", f"{dt.date.today().year}-03-31")

        if derive_sql_for_test is None:
            actual_sql = "<derive_sql_for_test not available>"
        else:
            actual_sql, actual_binds = derive_sql_for_test(
                question=q,
                namespace=namespace,
                test_binds=binds_hint
            )
            actual_sql = _normalize_sql(actual_sql)

        ok = (actual_sql == expect_sql)
        if ok: passed += 1

        results.append({
            "id": t["id"],
            "question": q,
            "ok": ok,
            "expected_sql": expect_sql,
            "actual_sql": actual_sql,
            "window": window_key,
            "binds_hint": binds_hint
        })

    return {
        "ok": passed == total,
        "passed": passed,
        "total": total,
        "results": results
    }
