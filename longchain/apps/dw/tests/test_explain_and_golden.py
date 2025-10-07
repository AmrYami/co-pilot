import pytest

pytest.importorskip("flask")

from longchain import app as flask_app


@pytest.fixture()
def client():
    if flask_app is None:
        pytest.skip("Flask application is unavailable")
    return flask_app.test_client()


def test_explain_contains_fts_and_order(client):
    payload = {
        "prefixes": [],
        "question": "list all contracts has it or home care",
        "auth_email": "dev@example.com",
        "full_text_search": True,
    }
    rv = client.post("/dw/answer", json=payload)
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    meta = data["meta"]
    assert "Fallback listing" not in (meta.get("explain") or "")
    assert "FTS(" in meta.get("user_explain", "")
    assert "ORDER BY REQUEST_DATE DESC" in data["sql"]


def test_rate_applies_eq_and_fts(client):
    rv = client.post(
        "/dw/answer",
        json={
            "prefixes": [],
            "question": "list all contracts has it or home care and ENTITY = DSFH",
            "auth_email": "dev@example.com",
            "full_text_search": True,
        },
    )
    ans = rv.get_json()
    inq = ans["inquiry_id"]

    rv2 = client.post(
        "/dw/rate",
        json={
            "inquiry_id": inq,
            "rating": 1,
            "comment": "fts: it | home care; eq: ENTITY = DSFH; order_by: REQUEST_DATE desc;",
        },
    )
    fix = rv2.get_json()
    sql = fix.get("sql", "")
    assert "LIKE UPPER(:fts_0)" in sql and "LIKE UPPER(:fts_1)" in sql
    assert "UPPER(TRIM(ENTITY))" in sql or "ENTITY = :eq_0" in sql
    assert "ORDER BY REQUEST_DATE DESC" in sql
