import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from apps.dw.rate_dates import build_date_clause, DateIntent


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
