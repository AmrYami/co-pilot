from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import re

@dataclass(frozen=True)
class Intent:
    # allowed kinds consumed by Pipeline.answer:
    # 'smalltalk' | 'help' | 'admin_task' | 'raw_sql' | 'sql' | 'ambiguous'
    kind: str
    reason: str

# --- Patterns (EN) ---
_GREETING = re.compile(
    r'^\s*(hi|hello|hey|salam|salām)\b',
    re.I | re.U
)
_HELP = re.compile(
    r'\b(help|what\s+can\s+you\s+do|how\s+(?:do|can)\s+you\s+help|how\s+to\s+use)\b',
    re.I | re.U
)
_ADMIN = re.compile(
    r'\b(restart\s+copilot|reload\s+settings|ingest|bundle|export|config|health|/admin)\b',
    re.I
)
_SQL_TOKENS = re.compile(
    r'\b(select|from|where|group\s+by|order\s+by|join|limit|having|union|with)\b',
    re.I
)
_DOMAIN = re.compile(
    r'\b(customer|customers|client|invoice|invoices|sales|receipt|supplier|gl|aging|'
    r'dimension|bank|payment|voucher|stock|item|inventory)\b',
    re.I
)

class IntentRouter:
    """
    Lightweight intent classifier with strong fallbacks; optionally uses a small LLM
    (clarifier) when available for borderline domain questions.
    """
    def __init__(self, llm: Optional[object] = None):
        # llm must expose .generate(prompt, max_new_tokens=..., temperature=..., top_p=...)
        self.llm = llm

    def classify(self, text: str) -> Intent:
        t = (text or "").strip()
        if not t:
            return Intent("smalltalk", "empty")

        if _GREETING.search(t):
            return Intent("smalltalk", "greeting")
        if _HELP.search(t):
            return Intent("help", "help_keyword")
        if _ADMIN.search(t):
            return Intent("admin_task", "admin_keyword")
        if _SQL_TOKENS.search(t):
            return Intent("raw_sql", "sql_tokens")

        # One or two words, no domain word → treat as smalltalk.
        if len(t.split()) <= 3 and not _DOMAIN.search(t):
            return Intent("smalltalk", "very_short")

        # Domain words present → likely SQL. If we have a clarifier LLM, try it once.
        if _DOMAIN.search(t):
            if self.llm is not None:
                try:
                    prompt = (
                        "Decide if the message is SQL-related (data question), SMALLTALK, HELP, or ADMIN.\n"
                        "Respond with a single word: SQL, SMALLTALK, HELP, or ADMIN.\n"
                        f"Message: {t!r}\n"
                    )
                    out = self.llm.generate(
                        prompt, max_new_tokens=1, temperature=0.0, top_p=1.0
                    ).strip().upper()
                    if out.startswith("SMALL"):
                        return Intent("smalltalk", "clarifier_smalltalk")
                    if out.startswith("HELP"):
                        return Intent("help", "clarifier_help")
                    if out.startswith("ADMIN"):
                        return Intent("admin_task", "clarifier_admin")
                    # default SQL on any other output
                    return Intent("sql", "clarifier_sql")
                except Exception:
                    # fall through to heuristic
                    pass
            return Intent("sql", "domain_words")

        # Last resort
        return Intent("ambiguous", "default")


# Convenience global router for simple intent checks
_DEFAULT_ROUTER = IntentRouter()


def detect_intent(text: str) -> Intent:
    """Classify *text* into a lightweight Intent instance."""
    return _DEFAULT_ROUTER.classify(text)

