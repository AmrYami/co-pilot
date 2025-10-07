from flask import Blueprint, render_template, redirect, url_for
from sqlalchemy import text

from apps.dw.learning import _engine

admin_rules_bp = Blueprint("admin_rules", __name__, url_prefix="/admin/rules")


@admin_rules_bp.route("/")
def list_rules():
    eng = _engine()
    patches = []
    if eng:
        with eng.begin() as cx:
            patches = cx.execute(
                text(
                    "SELECT id, inquiry_id, status, created_at, patch_json FROM dw_patches ORDER BY id DESC LIMIT 200"
                )
            ).mappings().all()
    return render_template("dw/admin_rules.html", patches=patches)


@admin_rules_bp.route("/approve/<int:pid>", methods=["POST"])
def approve(pid):
    eng = _engine()
    if eng:
        with eng.begin() as cx:
            cx.execute(text("UPDATE dw_patches SET status='approved' WHERE id=:i"), {"i": pid})
    return redirect(url_for("admin_rules.list_rules"))


@admin_rules_bp.route("/reject/<int:pid>", methods=["POST"])
def reject(pid):
    eng = _engine()
    if eng:
        with eng.begin() as cx:
            cx.execute(text("UPDATE dw_patches SET status='rejected' WHERE id=:i"), {"i": pid})
    return redirect(url_for("admin_rules.list_rules"))
