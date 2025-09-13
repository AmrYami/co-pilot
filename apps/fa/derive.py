from __future__ import annotations
import re
from typing import Dict, List, Optional

# Simple last-month filter that works on MySQL/MariaDB
def _mysql_last_month(col: str) -> str:
    # e.g. DATE_FORMAT(dt.tran_date, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')
    return f"DATE_FORMAT({col}, '%Y-%m') = DATE_FORMAT(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')"


def _first_prefix(prefixes: List[str] | None) -> str:
    if not prefixes:
        return ""
    p = prefixes[0] or ""
    # user sends '579_' style; we want plain '579_' prefix to concat to table names
    return p


def _qt(table: str, pfx: str) -> str:
    # Backtick-quote for MySQL identifiers and prepend prefix if provided
    if pfx and not table.startswith(pfx):
        table = f"{pfx}{table}"
    return f"`{table}`"


def try_build_sql_from_hints(
    mem_engine, prefixes: List[str], question: str, hints: Dict
) -> Optional[str]:
    """
    Return a best-effort SQL string when admin notes (hints) clearly specify
    the FA tables/metric/time period. Keep conservative and only return SQL
    when we're confident; otherwise return None and the Pipeline will try
    other strategies.
    """

    text = f"{question} {hints.get('free_text','')}".lower()
    pfx = _first_prefix(prefixes)

    # Pull structured signals (if present) from hints
    main_table = (hints.get("main_table") or "debtor_trans").strip()
    detail_table = (hints.get("detail_table") or "debtor_trans_details").strip()
    dm_table = "debtors_master"
    date_col = (hints.get("date_col") or "tran_date").strip()

    # Detect "top N" requests (default N = 10)
    m_top = re.search(r"\btop\s+(\d{1,3})\b", text)
    top_n = int(m_top.group(1)) if m_top else 10

    # Heuristic: customer focus + sales last month + net of credit notes
    is_customer_focus = bool(re.search(r"\bcustomer(s)?\b", text))
    mentions_sales = bool(re.search(r"\bsale|revenue|net\b", text))
    mentions_last_mo = bool(re.search(r"\blast\s+month\b", text))
    mentions_invoice = "debtor_trans" in main_table or "invoice" in text

    # If admin note says invoices + customer + last month → build canonical FA net sales
    if is_customer_focus and mentions_sales and mentions_last_mo and mentions_invoice:
        dt = _qt(main_table, pfx)
        dtd = _qt(detail_table, pfx)
        dm = _qt(dm_table, pfx)
        date = f"dt.{date_col}"
        where_last_month = _mysql_last_month(date)

        sql = f"""
SELECT dm.name AS customer,
       SUM((CASE WHEN dt.type = 11 THEN -1 ELSE 1 END)
           * dtd.unit_price
           * (1 - COALESCE(dtd.discount_percent, 0))
           * dtd.quantity) AS net_sales
FROM {dt} AS dt
JOIN {dtd} AS dtd
  ON dtd.debtor_trans_no = dt.trans_no
 AND dtd.debtor_trans_type = dt.type
JOIN {dm} AS dm
  ON dm.debtor_no = dt.debtor_no
WHERE dt.type IN (1, 11) -- 1=invoice, 11=credit note
  AND {where_last_month}
GROUP BY dm.name
ORDER BY net_sales DESC
LIMIT {top_n};
""".strip()
        return sql

    # Add more patterns here over time (supplier spend, item qty, receipts, etc.)
    return None
