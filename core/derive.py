from __future__ import annotations
import re
from typing import Optional, Dict, Any


class DerivationError(RuntimeError):
    pass


_SQL_TAG = re.compile(r"(?is)<sql>\s*(.*?)\s*</sql>")
_SQL_FALLBACK = re.compile(r"(?is)\b(with\b.*?;|\bselect\b.*?;)\s*$")


def _extract_sql(text: str) -> Optional[str]:
    if not text:
        return None
    m = _SQL_TAG.search(text)
    if m:
        return m.group(1).strip()
    m = _SQL_FALLBACK.search(text)
    if m:
        return m.group(1).strip()
    return None


def derive_sql_from_admin_reply(
    pipeline,
    inquiry_id: int,
    question: str,
    admin_answer: str,
    prefixes: list[str] | None = None,
    auth_email: str | None = None,
    extra_hints: Dict[str, Any] | None = None,
) -> str:
    """
    Turn admin natural-language reply into a concrete SQL query.
    We re-run the planner with the admin clarification injected and force SQL-only output.
    """

    prefixes = prefixes or []
    hints = dict(extra_hints or {})
    hints["admin_clarification"] = admin_answer
    hints["force_sql_only"] = True

    sys_rules = (
        "You are a SQL generator for MySQL/MariaDB.\n"
        "Return ONLY a valid SQL query that answers the question.\n"
        "Do not include explanations. Output between <sql> and </sql> tags."
    )
    prompt = (
        f"{sys_rules}\n\n"
        f"Question:\n{question}\n\n"
        f"Admin clarification:\n{admin_answer}\n\n"
        f"Context: user prefixes={prefixes}\n"
        f"Return SQL only.\n"
        f"<sql>"
    )

    out = pipeline.llm.generate(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9)
    if "</sql>" not in out:
        out = out + "\n</sql>"

    sql = _extract_sql(out)
    if not sql:
        raise DerivationError("planner returned non-SQL text")

    return sql
