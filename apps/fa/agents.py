from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple

# --- small helper: admin reply normalizer (kept for pipeline compatibility) ---
def normalize_admin_reply(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    # Keep simple; you can expand it later to parse YAML-ish admin hints.
    return {"raw": text}

# --- planner ---
class FAPlanner:
    """
    Very small rule-based planner that handles:
      - "top N customers by sales [last month]"
    Falls back to a single clarifying question for anything else.
    """

    def __init__(self, llm: Any, settings: Any) -> None:
        self.llm = llm
        self.settings = settings

    def get_prefix(self, context: Dict[str, Any]) -> str:
        prefixes = (context or {}).get("prefixes") or []
        return prefixes[0] if prefixes else ""

    def T(self, prefix: str, name: str) -> str:
        # Backticked + prefix, works for MySQL/MariaDB
        return f"`{prefix}{name}`"

    def fallback_clarifying_question(self, question: str, context: Dict[str, Any]) -> str:
        return "Which tables and date range should we use?"

    def plan(
        self,
        question: str,
        context: Dict[str, Any] | None = None,
        *,
        hints: Dict[str, Any] | None = None,
        admin_hints: Dict[str, Any] | None = None,
    ) -> Tuple[str, str]:

        q = (question or "").lower().strip()
        ctx = context or {}
        prefix = self.get_prefix(ctx)

        # detect "top N"
        m_top = re.search(r"\btop\s+(\d+)\b", q)
        top_n = int(m_top.group(1)) if m_top else 10

        wants_customers = ("customer" in q) or ("customers" in q)
        mentions_sales  = ("sale" in q) or ("sales" in q) or ("revenue" in q)
        mentions_month  = ("month" in q) or ("last month" in q) or ("previous month" in q)

        # We also accept "top customers by sales last ..." with no number (defaults to 10)
        if wants_customers and mentions_sales:
            date_filter = "AND DATE_FORMAT(dt.tran_date, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')" \
                          if mentions_month else \
                          "AND DATE_FORMAT(dt.tran_date, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')"

            # FrontAccounting types: 10=Sales Invoice, 11=Customer Credit Note
            sql = f"""
SELECT dm.name AS customer,
       SUM((CASE WHEN dt.type = 11 THEN -1 ELSE 1 END)
           * dtd.unit_price
           * (1 - COALESCE(dtd.discount_percent, 0))
           * dtd.quantity) AS net_sales
FROM {self.T(prefix, 'debtor_trans')} AS dt
JOIN {self.T(prefix, 'debtor_trans_details')} AS dtd
  ON dtd.debtor_trans_no = dt.trans_no
 AND dtd.debtor_trans_type = dt.type
JOIN {self.T(prefix, 'debtors_master')} AS dm
  ON dm.debtor_no = dt.debtor_no
WHERE dt.type IN (10, 11)  -- 10=invoice, 11=credit note
  {date_filter}
GROUP BY dm.name
ORDER BY net_sales DESC
LIMIT {top_n};
""".strip()
            return sql, "rule_based:net_sales_by_customer"

        # Anything else → let the pipeline ask one clarifying question
        raise RuntimeError("need_clarification")

# Factory used by core.pipeline
def get_planner(llm: Any, settings: Any) -> FAPlanner:
    return FAPlanner(llm, settings)
