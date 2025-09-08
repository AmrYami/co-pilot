from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass
class Intent:
    kind: str  # 'smalltalk' | 'help' | 'raw_sql' | 'sql' | 'ambiguous'
    reason: str


_GREETING = re.compile(r"^\s*(hi|hello|hey|السلام عليكم|مرحبا|أهلًا|اهلا|ازيك)\b", re.I)
_HELP = re.compile(
    r"\b(help|what can you do|how (?:do|can) you help|ممكن تساعدني|تقدر تعمل ايه|ايه اللي بتعمله)\b",
    re.I,
)
_SQL_TOKENS = re.compile(
    r"\b(select|from|where|group\s+by|order\s+by|join|limit)\b", re.I
)
_DOMAIN = re.compile(
    r"\b(customer|customers|invoice|invoices|sales|receipt|supplier|gl|aging|dimension|bank|payment|voucher|stock|item|inventory)\b",
    re.I,
)

_CLF_PROMPT = """You are a strict classifier. Output one of:
smalltalk | help | raw_sql | sql | ambiguous
Text: {q}
Answer:"""


class IntentRouter:
    def __init__(self, clarifier_llm):
        self.clarifier_llm = clarifier_llm

    def classify(self, text: str) -> Intent:
        t = (text or "").strip()
        if not t:
            return Intent("smalltalk", "empty")
        if _GREETING.search(t):
            if not _DOMAIN.search(t) and len(t.split()) <= 3:
                return Intent("smalltalk", "greeting")
        if _HELP.search(t):
            return Intent("help", "help")
        if _SQL_TOKENS.search(t):
            return Intent("raw_sql", "sql_tokens")
        try:
            out = self.clarifier_llm.generate(
                _CLF_PROMPT.format(q=t), max_new_tokens=4, temperature=0.0, top_p=1.0
            )
            ans = (out or "").strip().split()[0].lower()
            if ans in {"smalltalk", "help", "raw_sql", "sql", "ambiguous"}:
                return Intent(ans, "clarifier")
        except Exception:
            pass
        if _DOMAIN.search(t):
            return Intent("sql", "domain_words")
        if len(t.split()) <= 3:
            return Intent("smalltalk", "very_short")
        return Intent("ambiguous", "default")
