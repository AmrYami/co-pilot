# core/admin_helpers.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
import base64, os, hmac, hashlib, json

from .settings import KEY_SETTINGS_ADMIN_KEY_HASH
from core.pipeline import Pipeline

def derive_sql_from_admin_reply(
    pipeline: Pipeline,
    inq: Dict[str, Any],
    admin_reply: str,
    *,
    source: str = "fa",
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Derive runnable SQL from an admin's natural-language reply for a specific inquiry.

    What this does (high level):
      1) Build an augmented question that appends the admin's hints to the original user question.
      2) Ask the pipeline planner to produce canonical SQL (unprefixed) using the normal context builder.
      3) Let the pipeline rewrite canonical SQL to tenant-prefixed SQL (based on inquiry prefixes).
      4) Validate the SQL using the pipeline's validator (EXPLAIN-only safety).
      5) Return (sql, meta) on success; otherwise (None, info) with status or error details.

    Parameters
    ----------
    pipeline : Pipeline
        The live pipeline instance (already holds settings, engines, LLM, validator, etc.).
    inq : dict
        A row-like mapping for the inquiry (must contain 'question' and optionally 'prefixes').
    admin_reply : str
        Admin free-text guidance (no SQL required).
    source : str
        Logical source handled by the pipeline. Defaults to "fa" but kept generic.

    Returns
    -------
    Tuple[Optional[str], Dict[str, Any]]
        On success: (sql_string, {"status":"ok", "context":..., "rationale":...})
        On failure: (None, {"status": "...", "questions":[...], "context":...}) OR
                    (None, {"error": "...", "details": ...})
    """
    # ---- Step 0: input guards (small note: we fail fast if the inquiry is malformed)
    prefixes = inq.get("prefixes") or []
    question = (inq.get("question") or "").strip()
    if not question:
        return None, {"error": "inquiry has no question"}

    # ---- Step 1: augment question with admin hints (small note: keeps planner prompt simple)
    augmented_q = f"{question}\n\nADMIN HINTS: {admin_reply}".strip()

    # ---- Step 2: plan via pipeline (small note: returns canonical SQL + rationale)
    plan_out = pipeline.answer(question=augmented_q, context={"prefixes": prefixes, "source": source}, hints=None)

    # If planner still needs clarification or failed, bubble that up unchanged
    if plan_out.get("status") != "ok":
        return None, {
            "status": plan_out.get("status"),
            "questions": plan_out.get("questions"),
            "context": plan_out.get("context"),
            "rationale": plan_out.get("rationale"),
        }

    sql = (plan_out.get("sql") or "").strip()
    if not sql:
        return None, {"error": "planner returned empty SQL", "context": plan_out.get("context")}

    # ---- Step 3/4: validate via pipeline’s validator (small note: EXPLAIN probe)
    ok, info = pipeline.validator.quick_validate(sql)
    if not ok:
        return None, {"error": "validation failed", "details": info, "context": plan_out.get("context")}

    # ---- Step 5: done (small note: return rationale + context to help the UI)
    return sql, {"status": "ok", "context": plan_out.get("context"), "rationale": plan_out.get("rationale")}


def _pbkdf2_hash(secret: str, salt: bytes | None = None, iterations: int = 200_000) -> str:
    salt = salt or os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def _pbkdf2_verify(secret: str, encoded: str) -> bool:
    try:
        scheme, iters, b64salt, b64hash = encoded.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(b64salt)
        iters = int(iters)
        expect = base64.b64decode(b64hash)
        dk = hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, iters)
        return hmac.compare_digest(dk, expect)
    except Exception:
        return False


def verify_admin_key(settings, provided: str) -> bool:
    # Prefer hashed key from DB; fallback to plaintext env (bootstrap only)
    h = settings.get(KEY_SETTINGS_ADMIN_KEY_HASH)
    if h and _pbkdf2_verify(provided, h):
        return True
    raw = settings.get("SETTINGS_ADMIN_KEY")
    return bool(raw and hmac.compare_digest(raw, provided))
