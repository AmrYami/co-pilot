"""Common admin endpoints for teaching synonyms and snippets."""

from __future__ import annotations

import json

from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, text

from core.settings import Settings

admin_bp = Blueprint("admin_common", __name__, url_prefix="/admin")


@admin_bp.post("/teach")
def teach():
    data = request.get_json(force=True) or {}
    namespace = data.get("namespace", "dw::common")
    updated_by = data.get("updated_by", "teacher")
    synonyms = data.get("synonyms") or []
    qna = data.get("qna") or []

    settings = Settings()
    mem = create_engine(settings.get("MEMORY_DB_URL", scope="global"), pool_pre_ping=True, future=True)

    ins_map = text(
        """
      INSERT INTO mem_mappings(namespace, alias, canonical, mapping_type, scope, source, confidence, created_at, updated_at)
      VALUES (:ns, :alias, :canon, :mtype, :scope, 'user', :conf, NOW(), NOW())
      ON CONFLICT (namespace, alias, mapping_type, scope)
      DO UPDATE SET canonical = EXCLUDED.canonical, confidence = EXCLUDED.confidence, updated_at = NOW()
    """
    )

    ins_snip = text(
        """
      INSERT INTO mem_snippets(namespace, title, description, sql_template, sql_raw, input_tables, tags, created_at, updated_at, is_verified, verified_by)
      VALUES (:ns, :title, :desc, :tmpl, :raw, CAST(:tables AS jsonb), CAST(:tags AS jsonb), NOW(), NOW(), true, :by)
      RETURNING id
    """
    )

    with mem.begin() as con:
        for mapping in synonyms:
            con.execute(
                ins_map,
                {
                    "ns": namespace,
                    "alias": mapping["alias"],
                    "canon": mapping["canonical"],
                    "mtype": mapping.get("mapping_type", "term"),
                    "scope": mapping.get("scope", "global"),
                    "conf": float(mapping.get("confidence", 0.9)),
                },
            )
        for item in qna:
            con.execute(
                ins_snip,
                {
                    "ns": namespace,
                    "title": item.get("question", "taught-sql"),
                    "desc": item.get("description", ""),
                    "tmpl": item.get("sql"),
                    "raw": item.get("sql"),
                    "tables": json.dumps(["Contract"]),
                    "tags": json.dumps(item.get("tags", ["dw", "teach"])),
                    "by": updated_by,
                },
            )

    return jsonify({"ok": True, "synonyms": len(synonyms), "snippets": len(qna)})
