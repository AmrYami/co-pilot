import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from apps.dw.rate_dates import build_date_clause, DateIntent
from apps.dw.rate.date_windows import compile_date_sql, detect_date_window


def _settings():
    return {
        "DW_OVERLAP_REQUIRE_BOTH_DATES": 1,
        "DW_OVERLAP_STRICT": 1,
    }


def test_requested_last_month():
    comment = "requested: last month"
    dt, sql, binds, dbg = build_date_clause(comment, _settings())
    assert isinstance(dt, DateIntent)
    assert dt.mode == "REQUEST"
    assert dt.column == "REQUEST_DATE"
    assert sql == "REQUEST_DATE BETWEEN :date_start AND :date_end"
    assert "date_start" in binds and "date_end" in binds


def test_active_last_quarter():
    comment = "active: last quarter"
    dt, sql, binds, dbg = build_date_clause(comment, _settings())
    assert isinstance(dt, DateIntent)
    assert dt.mode == "OVERLAP"
    assert "START_DATE <= :date_end" in sql
    assert "END_DATE >= :date_start" in sql


def test_expiring_next_90_days():
    comment = "expiring: next 90 days"
    dt, sql, binds, dbg = build_date_clause(comment, _settings())
    assert isinstance(dt, DateIntent)
    assert dt.mode == "END_ONLY"
    assert sql == "END_DATE BETWEEN :date_start AND :date_end"
    assert dt.order_by_override == "END_DATE ASC"


def test_between_iso():
    comment = "between: 2024-01-01..2024-03-31"
    dt, sql, binds, dbg = build_date_clause(comment, _settings())
    assert isinstance(dt, DateIntent)
    assert dt.mode == "OVERLAP"
    assert binds["date_start"].isoformat() == "2024-01-01"
    assert binds["date_end"].isoformat() == "2024-03-31"


def test_detect_between_arabic_expiring():
    today = date(2024, 5, 20)
    win = detect_date_window("العقود التي تنتهي بين 2024-01-01 و 2024-01-31", today=today)
    assert win is not None
    assert win.kind == "END_ONLY"
    assert win.start == date(2024, 1, 1)
    assert win.end == date(2024, 1, 31)
    frag, binds, order = compile_date_sql(win, overlap_require_both=True, overlap_strict=True)
    assert frag == "END_DATE BETWEEN :date_start AND :date_end"
    assert binds["date_start"] == date(2024, 1, 1)
    assert binds["date_end"] == date(2024, 1, 31)
    assert order == "END_DATE ASC"


def test_detect_arabic_digits_last_weeks():
    today = date(2024, 5, 20)
    win = detect_date_window("أريد العقود خلال آخر ٣ أسابيع", today=today)
    assert win is not None
    assert win.kind == "OVERLAP"
    assert win.start == date(2024, 4, 29)
    assert win.end == today
    frag, _, _ = compile_date_sql(win, overlap_require_both=True, overlap_strict=True)
    assert "START_DATE" in frag and "END_DATE" in frag


def test_detect_requested_last_two_weeks():
    today = date(2024, 5, 20)
    win = detect_date_window("requested last 2 weeks", today=today)
    assert win is not None
    assert win.kind == "REQUEST"
    frag, binds, order = compile_date_sql(win, overlap_require_both=False, overlap_strict=False)
    assert frag == "REQUEST_DATE BETWEEN :date_start AND :date_end"
    assert binds["date_start"] == date(2024, 5, 6)
    assert binds["date_end"] == today
    assert order == "REQUEST_DATE DESC"


def test_detect_next_quarter():
    today = date(2024, 5, 20)
    win = detect_date_window("show me next quarter", today=today)
    assert win is not None
    assert win.kind == "OVERLAP"
    assert win.start == date(2024, 7, 1)
    assert win.end == date(2024, 9, 30)
