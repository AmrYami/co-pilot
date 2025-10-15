from flask import Blueprint, request, jsonify, abort
from sqlalchemy import create_engine, text
import os
import json

bp = Blueprint("dw_admin_api", __name__)

MEM_URL = os.getenv("MEMORY_DB_URL", "postgresql+psycopg2://postgres:123456789@localhost/copilot_mem_dev")
eng = create_engine(MEM_URL, pool_pre_ping=True, future=True)


def _get_admin_emails():
    # نقرأ ADMIN_EMAILS من mem_settings (namespace=dw::common)
    sql = text(
        """
      SELECT value FROM mem_settings
      WHERE key='ADMIN_EMAILS' AND (namespace='dw::common' OR namespace IS NULL)
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
      LIMIT 1
    """
    )
    with eng.begin() as cn:
        row = cn.execute(sql).mappings().first()
    if not row:
        # fallback: من متغير بيئة أو قائمة فاضية
        return set(
            (
                os.getenv("ADMIN_EMAILS_CSV", "").lower().split(",")
                if os.getenv("ADMIN_EMAILS_CSV")
                else []
            )
        )
    try:
        val = row["value"]
        if isinstance(val, str):
            # value محفوظ نص JSON في بعض التنصيبات
            val = json.loads(val)
        return set([str(x).lower() for x in (val or [])])
    except Exception:
        return set()


def _require_admin():
    email = (
        request.headers.get("X-Auth-Email")
        or request.args.get("auth_email")
        or ""
    ).lower().strip()
    admins = _get_admin_emails()
    if email and email in admins:
        return email
    abort(403, description="Admin only")


@bp.get("/feedback")
def list_feedback():
    _require_admin()
    status = request.args.get("status", "pending").lower()
    limit = int(request.args.get("limit", 100))
    sql = text(
        """
      SELECT id, inquiry_id, auth_email, rating, comment,
             intent_json, resolved_sql, binds_json,
             status, approver_email, admin_note, rejected_reason,
             created_at, updated_at
      FROM dw_feedback
      WHERE (:status = '' OR LOWER(status) = :status)
      ORDER BY created_at DESC
      LIMIT :limit
    """
    )
    with eng.begin() as cn:
        rows = [dict(r) for r in cn.execute(sql, {"status": status, "limit": limit}).mappings().all()]
    return jsonify({"ok": True, "rows": rows})


@bp.get("/feedback/<int:fid>")
def get_feedback(fid: int):
    _require_admin()
    sql = text("SELECT * FROM dw_feedback WHERE id=:id")
    with eng.begin() as cn:
        row = cn.execute(sql, {"id": fid}).mappings().first()
    if not row:
        abort(404, description="feedback not found")
    return jsonify({"ok": True, "row": dict(row)})


@bp.post("/feedback/<int:fid>/approve")
def approve_feedback(fid: int):
    admin = _require_admin()
    payload = request.get_json(silent=True) or {}
    resolved_sql = payload.get("resolved_sql")
    note = payload.get("note")
    create_rule = bool(payload.get("create_rule", True))
    rule = payload.get("rule")  # {kind, pattern, payload, weight}

    with eng.begin() as cn:
        # تأكيد وجود feedback
        fb = cn.execute(
            text("SELECT * FROM dw_feedback WHERE id=:id FOR UPDATE"),
            {"id": fid},
        ).mappings().first()
        if not fb:
            abort(404, description="feedback not found")

        # تحديث الحالة
        upd = text(
            """
          UPDATE dw_feedback
          SET status='approved',
              approver_email=:admin,
              admin_note=:note,
              resolved_sql=COALESCE(:resolved_sql, resolved_sql),
              updated_at=NOW()
          WHERE id=:id
        """
        )
        cn.execute(
            upd,
            {
                "id": fid,
                "admin": admin,
                "note": note,
                "resolved_sql": resolved_sql,
            },
        )

        inserted_rule = None
        if create_rule and rule:
            ins = text(
                """
              INSERT INTO dw_rules(namespace, pattern, rule, weight, author_email, approved_by, created_at)
              VALUES(:ns, :pattern, :rule::jsonb, COALESCE(:weight,0.5), :author, :approved_by, NOW())
              RETURNING id
            """
            )
            params = {
                "ns": "dw::common",
                "pattern": rule.get("pattern") or f"feedback:{fid}",
                "rule": json.dumps(rule),
                "weight": rule.get("weight"),
                "author": fb.get("auth_email"),
                "approved_by": admin,
            }
            rid = cn.execute(ins, params).scalar_one()
            inserted_rule = {"id": rid, "pattern": params["pattern"]}
    return jsonify({"ok": True, "approved": True, "rule": inserted_rule})


@bp.post("/feedback/<int:fid>/reject")
def reject_feedback(fid: int):
    admin = _require_admin()
    payload = request.get_json(silent=True) or {}
    reason = (payload.get("reason") or "").strip() or "rejected by admin"
    with eng.begin() as cn:
        fb = cn.execute(
            text("SELECT id FROM dw_feedback WHERE id=:id FOR UPDATE"),
            {"id": fid},
        ).first()
        if not fb:
            abort(404, description="feedback not found")
        cn.execute(
            text(
                """
          UPDATE dw_feedback
          SET status='rejected', rejected_reason=:reason,
              approver_email=:admin, updated_at=NOW()
          WHERE id=:id
        """
            ),
            {"id": fid, "admin": admin, "reason": reason},
        )
    return jsonify({"ok": True, "rejected": True, "reason": reason})


@bp.get("/rules")
def list_rules():
    _require_admin()
    ns = request.args.get("namespace", "dw::common")
    sql = text(
        """
      SELECT id, namespace, pattern, rule, weight, author_email, approved_by, created_at, updated_at
      FROM dw_rules
      WHERE namespace=:ns
      ORDER BY created_at DESC
      LIMIT 1000
    """
    )
    with eng.begin() as cn:
        rows = [dict(r) for r in cn.execute(sql, {"ns": ns}).mappings().all()]
    return jsonify({"ok": True, "rows": rows})


@bp.post("/rules")
def create_rule():
    admin = _require_admin()
    body = request.get_json(force=True)
    ns = body.get("namespace", "dw::common")
    pattern = body.get("pattern")
    rule = body.get("rule")
    weight = body.get("weight", 0.5)
    if not pattern or not rule:
        abort(400, description="pattern and rule are required")
    sql = text(
        """
      INSERT INTO dw_rules(namespace, pattern, rule, weight, author_email, approved_by, created_at)
      VALUES(:ns, :pattern, :rule::jsonb, :w, :author, :admin, NOW())
      RETURNING id
    """
    )
    with eng.begin() as cn:
        rid = cn.execute(
            sql,
            {
                "ns": ns,
                "pattern": pattern,
                "rule": json.dumps(rule),
                "w": weight,
                "author": admin,
                "admin": admin,
            },
        ).scalar_one()
    return jsonify({"ok": True, "id": rid})


__all__ = ["bp"]
