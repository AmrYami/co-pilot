from __future__ import annotations
import json
import re
from typing import Tuple, Dict, Any, Optional

from core.specs import QuestionSpec
from core.model_loader import load_model, load_clarifier


_SMALTALK = re.compile(r'^\s*(hi|hello|hey|السلام عليكم|مرحبا|أهلًا|اهلا|ازيك)\b', re.I)
_HELP = re.compile(r'help|what can you do|how (?:do|can) you help|ممكن تساعد|تقدر تعمل ايه', re.I)

SYS = (
    "You are a planning assistant. Output compact JSON only. "
    "Schema:\n"
    "{"
    "  \"intent\": \"smalltalk|help|raw_sql|sql_request|ambiguous\","
    "  \"datasource\": null|string,"
    "  \"date_column\": null|string,"
    "  \"date_range\": null|string,"
    "  \"entity\": null|string,"
    "  \"tables\": string[],"
    "  \"metric_key\": null|string,"
    "  \"metric_expr\": null|string,"
    "  \"group_by\": string[],"
    "  \"top_k\": null|int,"
    "  \"filters\": string[]"
    "}\n"
    "Rules: If user typed SQL, intent=raw_sql. If greeting/help, mark accordingly. "
    "If information is missing, leave fields null/empty (do NOT hallucinate)."
)

FEW_SHOT = (
    "Q: top 10 customers by sales last month\n"
    "JSON: {\"intent\":\"sql_request\",\"datasource\":null,\"date_column\":null,\"date_range\":\"last month\",\"entity\":\"customer\",\"tables\":[],\"metric_key\":\"net_sales\",\"metric_expr\":null,\"group_by\":[\"customer\"],\"top_k\":10,\"filters\":[]}\n"
    "Q: hello\n"
    "JSON: {\"intent\":\"smalltalk\",\"datasource\":null,\"date_column\":null,\"date_range\":null,\"entity\":null,\"tables\":[],\"metric_key\":null,\"metric_expr\":null,\"group_by\":[],\"top_k\":null,\"filters\":[]}\n"
)


def _to_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = re.sub(r'^\s*json', "", text, flags=re.I).strip()
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r'\{.*\}', text, re.S)
        if m:
            return json.loads(m.group(0))
        raise


class ClarifierAgent:
    """LLM-assisted intent classifier and spec extractor."""

    def __init__(self, settings):
        self.settings = settings
        self.llm = load_clarifier(settings) or load_model(settings)

    def classify_and_extract(
        self, question: str, prefixes, domain_hints: Dict[str, Any]
    ) -> QuestionSpec:
        qt = question.strip()
        if not qt:
            return QuestionSpec(intent="smalltalk", prefixes=list(prefixes or []))
        if _SMALTALK.search(qt):
            return QuestionSpec(intent="smalltalk", prefixes=list(prefixes or []))
        if _HELP.search(qt):
            return QuestionSpec(intent="help", prefixes=list(prefixes or []))
        if re.search(r'\bselect\b|\bfrom\b', qt, re.I):
            return QuestionSpec(intent="raw_sql", prefixes=list(prefixes or []))

        context = ""
        if domain_hints.get("entities"):
            context += f"Entities: {', '.join(domain_hints['entities'])}\n"
        if domain_hints.get("table_aliases"):
            context += f"Tables: {', '.join(domain_hints['table_aliases'])}\n"
        if domain_hints.get("metric_registry"):
            context += "Metrics: " + ", ".join(domain_hints["metric_registry"].keys()) + "\n"

        prompt = (
            f"{SYS}\n{FEW_SHOT}"
            f"Context:\n{context}\n"
            f"Q: {question}\nJSON:"
        )
        text = self.llm.generate(prompt, max_new_tokens=256, temperature=0.0, top_p=1.0)
        try:
            data = _to_json(text)
        except Exception:
            return QuestionSpec(intent="ambiguous", prefixes=list(prefixes or []))

        spec = QuestionSpec(
            intent=data.get("intent", "sql_request"),
            prefixes=list(prefixes or []),
            datasource=data.get("datasource"),
            date_column=data.get("date_column"),
            date_range=data.get("date_range"),
            entity=data.get("entity"),
            tables=list(data.get("tables") or []),
            metric_key=data.get("metric_key"),
            metric_expr=data.get("metric_expr"),
            group_by=list(data.get("group_by") or []),
            top_k=data.get("top_k"),
            filters=list(data.get("filters") or []),
        )
        return spec
