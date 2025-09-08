from __future__ import annotations
from dataclasses import dataclass
import re
from functools import lru_cache
from typing import Optional, Tuple


@dataclass
class Intent:
    kind: str
    confidence: float
    reason: str


_GREETING = re.compile(r'^\s*(hi|hello|hey|السلام عليكم|مرحبا|أهلًا|اهلا|ازيك)\b', re.I)
_HELP = re.compile(r'\b(help|what can you do|how (?:do|can) you help|ممكن تساعدني|تقدر تعمل ايه|ايه اللي بتعمله)\b', re.I)
_SQL = re.compile(r'\b(select|insert|update|delete|from|where|group\s+by|order\s+by|join|limit)\b', re.I)
_DOMAIN = re.compile(r'\b(customer|customers|invoice|invoices|sales|receipt|supplier|gl|aging|dimension|bank|payment|voucher|stock|item|inventory)\b', re.I)
_ADMIN = re.compile(r'\b(prefix|ingest|re-ingest|reingest|approve|bundle|metrics|settings|config|admin)\b', re.I)


def _l0_rules(text: str) -> Optional[Intent]:
    t = (text or "").strip()
    if not t:
        return Intent("smalltalk", 0.9, "empty")
    if _GREETING.search(t):
        if _DOMAIN.search(t):
            return Intent("data_question", 0.55, "greeting+domain")
        return Intent("smalltalk", 0.95, "greeting")
    if _HELP.search(t):
        return Intent("help", 0.95, "help")
    if _ADMIN.search(t):
        return Intent("admin_task", 0.65, "admin_keywords")
    if _SQL.search(t):
        return Intent("raw_sql", 0.9, "sql_tokens")
    if _DOMAIN.search(t) and len(t.split()) >= 3:
        return Intent("data_question", 0.6, "domain_words")
    if len(t.split()) <= 3:
        return Intent("smalltalk", 0.7, "very_short")
    return None


_CLARIFIER_PROMPT = """You are an intent classifier. Output ONLY a JSON object on one line with keys: intent, confidence.
Valid intents = ["smalltalk","help","data_question","raw_sql","admin_task","unknown"].
Text: "{text}"
JSON:"""


class IntentRouter:
    """Hybrid router: rules first; small LLM if configured; safe fallbacks."""

    def __init__(self, clarifier_llm=None):
        self.llm = clarifier_llm

    @lru_cache(maxsize=512)
    def classify(self, text: str) -> Intent:
        r = _l0_rules(text)
        if r and (r.confidence >= 0.8 or self.llm is None):
            return r

        if self.llm is None:
            return r or Intent("unknown", 0.4, "no_clarifier_llm")

        prompt = _CLARIFIER_PROMPT.format(text=text.replace('"', '\\"'))
        try:
            out = self.llm.generate(prompt, max_new_tokens=24, temperature=0.0, top_p=1.0, stop=['\n'])
            intent, conf = _parse_json_line(out)
            if intent not in {"smalltalk","help","data_question","raw_sql","admin_task","unknown"}:
                raise ValueError("bad intent")
            return Intent(intent, conf, "llm")
        except Exception:
            return r or Intent("unknown", 0.4, "clarifier_error")


def _parse_json_line(s: str) -> Tuple[str, float]:
    s = s.strip()
    intent = "unknown"
    conf = 0.5
    m_int = re.search(r'"intent"\s*:\s*"([^"]+)"', s)
    m_con = re.search(r'"confidence"\s*:\s*([0-9]*\.?[0-9]+)', s)
    if m_int:
        intent = m_int.group(1)
    if m_con:
        try:
            conf = float(m_con.group(1))
        except Exception:
            pass
    return intent, max(0.0, min(1.0, conf))
