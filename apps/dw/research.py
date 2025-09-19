from typing import Any, Dict, List


class DWResearcher:
    """
    Minimal researcher for DocuWare.
    - No web crawl yet; just returns curated facts and can later read from mem_sources.
    - Contract table: stakeholder slots, gross value, date columns.
    """

    def __init__(self, settings=None):
        self.settings = settings or {}

    def search(self, question: str, prefixes: List[str], context: Dict[str, Any]) -> Dict[str, Any]:
        # Curated "facts" we want SQLCoder to know when it retries
        facts = [
            "The main table is Contract.",
            "There are 8 stakeholder/department pairs: (CONTRACT_STAKEHOLDER_1..8, DEPARTMENT_1..8).",
            "Gross value = NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0).",
            "Date columns available: REQUEST_DATE (requested), START_DATE (contract start), END_DATE (contract end).",
            "OWNER_DEPARTMENT is the contract owner department; DEPARTMENT_OUL is overall lead.",
            "Stakeholder filters should coalesce 8 slots via UNION ALL or a generated series.",
            "Use LISTAGG for department rollups; TRIM strings; exclude NULL/blank stakeholders.",
            "Use Oracle syntax (NVL, LISTAGG, FETCH FIRST N ROWS ONLY).",
        ]

        # Optional short summary for the UI
        summary = (
            "Added DocuWare contract facts: stakeholder slots (1–8), gross value formula, and date columns. "
            "Retry SQL generation with these hints."
        )

        # A pretend “source” we can store; later you can replace with real docs/links.
        sources = [
            {
                "source_type": "internal_doc",
                "locator": "dw://contract/cheatsheet",
                "title": "DW Contract cheatsheet",
                "raw_content": "\n".join(facts),
                "metadata": {"namespace": context.get("namespace", "dw::common")},
            }
        ]

        # Optional structured hints for the planner/LLM
        structured = {
            "primary_table": "Contract",
            "date_columns": ["REQUEST_DATE", "START_DATE", "END_DATE"],
            "gross_formula": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
            "stakeholder_slots": 8,
        }

        return {
            "facts": facts,
            "summary": summary,
            "sources": sources,
            "structured": structured,
        }
