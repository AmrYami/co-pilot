from datetime import datetime, timezone
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.builder import build_sql
from apps.dw.intent import parse_intent_legacy


def _set_now(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr("apps.dw.intent.today_utc", lambda: when)


def test_build_sql_top_n_group(monkeypatch):
    now = datetime(2023, 6, 1, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent_legacy("Top 5 stakeholders by contract value last 3 months")
    sql, binds = build_sql(intent)
    assert "GROUP BY CONTRACT_STAKEHOLDER_1" in sql
    assert "ORDER BY MEASURE DESC" in sql
    assert "FETCH FIRST :top_n ROWS ONLY" in sql
    assert binds["date_start"] == "2023-03-01"
    assert binds["date_end"] == "2023-06-01"
    assert binds["top_n"] == 5


def test_build_sql_projection(monkeypatch):
    now = datetime(2023, 8, 5, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent_legacy(
        "List all contracts requested last month (contract_id, contract_owner, request_date)"
    )
    sql, binds = build_sql(intent)
    assert sql.startswith('SELECT CONTRACT_ID, CONTRACT_OWNER, REQUEST_DATE FROM "Contract"')
    assert binds == {"date_start": "2023-07-01", "date_end": "2023-07-31"}


def test_build_sql_expiring(monkeypatch):
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent_legacy("Contracts expiring in 30 days")
    sql, binds = build_sql(intent)
    assert "END_DATE BETWEEN :date_start AND :date_end" in sql
    assert binds == {"date_start": "2023-12-16", "date_end": "2024-01-15"}


def test_build_sql_count_by_status():
    intent = parse_intent_legacy("Count of contracts by status")
    sql, binds = build_sql(intent)
    assert sql.strip().upper() == 'SELECT COUNT(*) AS CNT FROM "CONTRACT"'
    assert binds == {}
