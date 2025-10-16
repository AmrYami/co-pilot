import json
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import text

from apps.common.db_mem import get_mem_engine


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def persist_feedback(
    inquiry_id: int,
    auth_email: str,
    rating: int,
    comment: str,
    resp: dict,
) -> dict:
    """
    Upsert into dw_feedback keyed by (inquiry_id).
    Stores intent, sql and binds so admin can approve later.
    Returns {"ok": True} or {"ok": False, "error": "..."} for debug.
    """

    if not inquiry_id:
        return {"ok": False, "error": "missing_inquiry_id"}

    resolved_sql = resp.get("sql")
    if not resolved_sql:
        resolved_sql = resp.get("debug", {}).get("final_sql", {}).get("sql")

    binds_json = resp.get("binds") or resp.get("debug", {}).get("final_sql", {}).get("binds") or {}
    intent_json = resp.get("debug", {}).get("intent") or {}

    row = {
        "inquiry_id": inquiry_id,
        "auth_email": (auth_email or "").strip(),
        "rating": int(rating or 0),
        "comment": (comment or "").strip(),
        "intent_json": json.dumps(intent_json, default=_json_default),
        "resolved_sql": resolved_sql or "",
        "binds_json": json.dumps(binds_json, default=_json_default),
        "status": "pending" if int(rating or 0) <= 3 else "auto-accepted",
    }

    sql = text(
        """
        INSERT INTO dw_feedback (
            inquiry_id, auth_email, rating, comment,
            intent_json, resolved_sql, binds_json,
            status, created_at, updated_at
        )
        VALUES (
            :inquiry_id, :auth_email, :rating, :comment,
            CAST(:intent_json AS jsonb), :resolved_sql, CAST(:binds_json AS jsonb),
            :status, NOW(), NOW()
        )
        ON CONFLICT (inquiry_id) DO UPDATE SET
            auth_email   = EXCLUDED.auth_email,
            rating       = EXCLUDED.rating,
            comment      = EXCLUDED.comment,
            intent_json  = EXCLUDED.intent_json,
            resolved_sql = EXCLUDED.resolved_sql,
            binds_json   = EXCLUDED.binds_json,
            -- keep 'rejected' if admin already rejected it
            status       = CASE WHEN dw_feedback.status='rejected'
                                THEN 'rejected' ELSE EXCLUDED.status END,
            updated_at   = NOW()
    """
    )

    eng = get_mem_engine()
    try:
        with eng.begin() as cx:
            cx.execute(sql, row)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e), "engine": str(eng.url)}
