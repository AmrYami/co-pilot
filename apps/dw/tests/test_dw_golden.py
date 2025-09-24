from datetime import datetime, timezone
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from apps.dw.intent import parse_intent


def _set_now(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr("apps.dw.intent.today_utc", lambda: when)


def test_parse_intent_last_month(monkeypatch):
    now = datetime(2023, 2, 10, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent("top 10 stakeholders by contract value last month")
    assert intent.top_n == 10
    assert intent.user_requested_top_n is True
    assert intent.group_by == "CONTRACT_STAKEHOLDER_1"
    assert intent.explicit_dates == {"start": "2023-01-01", "end": "2023-01-31"}
    assert intent.measure_sql == "NVL(CONTRACT_VALUE_NET_OF_VAT,0)"


def test_parse_intent_expiring_window(monkeypatch):
    now = datetime(2023, 5, 1, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent("contracts expiring in 30 days")
    assert intent.expire is True
    assert intent.date_column == "END_DATE"
    assert intent.explicit_dates == {"start": "2023-04-01", "end": "2023-05-01"}


def test_parse_intent_by_status_sets_count():
    intent = parse_intent("Count of contracts by status")
    assert intent.agg == "count"
    assert intent.group_by == "CONTRACT_STATUS"


def test_parse_intent_projection(monkeypatch):
    now = datetime(2023, 7, 15, tzinfo=timezone.utc)
    _set_now(monkeypatch, now)
    intent = parse_intent(
        "List all contracts requested last month (contract_id, contract_owner, request_date)"
    )
    assert intent.date_column == "REQUEST_DATE"
    assert intent.explicit_dates == {"start": "2023-06-01", "end": "2023-06-30"}
    assert intent.wants_all_columns is False
    assert intent.notes["projection"] == [
        "CONTRACT_ID",
        "CONTRACT_OWNER",
        "REQUEST_DATE",
    ]
