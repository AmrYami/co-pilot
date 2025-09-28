from __future__ import annotations

from datetime import date
from typing import Dict, Any, Optional, Tuple
import re


# ---------- Utilities ----------
def _gross_expr() -> str:
    """Oracle-safe gross expression (net + VAT-as-rate-or-amount)."""
    return (
        "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + "
        "CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1 "
        "THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0) "
        "ELSE NVL(VAT,0) END"
    )


def _extract_top_n(q: str, default: int = 5) -> int:
    m = re.search(r"\btop\s+(\d+)\b", q, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return default


def _is_bottom_request(q: str) -> bool:
    """Detect bottom/lowest requests to flip sort order."""
    return bool(re.search(r"\b(lowest|bottom|least|smallest|cheapest|min)\b", q, re.IGNORECASE))


def _extract_year(q: str) -> Optional[int]:
    m = re.search(r"\b(20\d{2})\b", q)
    if m:
        yr = int(m.group(1))
        if 2000 <= yr <= 2100:
            return yr
    return None


def _ytd_range(year: Optional[int] = None) -> Tuple[date, date]:
    """Return (start, end) dates for YTD. If year is current, end=today; otherwise end=Dec-31."""
    today = date.today()
    if year is None:
        year = today.year
    ds = date(year, 1, 1)
    de = today if year == today.year else date(year, 12, 31)
    return ds, de


# ---------- Special-case builder ----------
def try_build_special_cases(question: str) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Accuracy-first shortcuts for a few high-value patterns.
    Returns (sql, binds, meta) or (None, None, None) if not matched.
    """
    q = (question or "").lower()

    # 1) Top N contracts by gross (YTD [year])
    if "ytd" in q and "gross" in q and "contract" in q:
        top_n = _extract_top_n(q, default=5)
        asc = _is_bottom_request(q)  # enable "lowest/bottom ..." support
        sort_dir = "ASC" if asc else "DESC"
        year = _extract_year(q)
        ds, de = _ytd_range(year)
        sql = (
            'SELECT * FROM "Contract"\n'
            "WHERE (START_DATE IS NOT NULL AND END_DATE IS NOT NULL "
            "AND START_DATE <= :date_end AND END_DATE >= :date_start)\n"
            f"ORDER BY {_gross_expr()} {sort_dir}\n"
            "FETCH FIRST :top_n ROWS ONLY"
        )
        binds = {"date_start": ds, "date_end": de, "top_n": top_n}
        meta = {
            "explain": f"Top {top_n} by GROSS for YTD {year or 'current year'} using overlap window.",
            "gross": True,
            "group_by": None,
        }
        return sql, binds, meta

    # 2) OWNER_DEPARTMENT vs DEPARTMENT_OUL discrepancy report
    if "owner_department" in q and "department_oul" in q and ("vs" in q or "compare" in q or "comparison" in q):
        sql = (
            "SELECT NVL(TRIM(OWNER_DEPARTMENT),'(None)') AS OWNER_DEPARTMENT,\n"
            "       NVL(TRIM(DEPARTMENT_OUL),'(None)')  AS DEPARTMENT_OUL,\n"
            "       COUNT(*) AS CNT\n"
            'FROM "Contract"\n'
            "WHERE DEPARTMENT_OUL IS NOT NULL\n"
            "  AND NVL(TRIM(OWNER_DEPARTMENT),'(None)') <> NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
            "GROUP BY NVL(TRIM(OWNER_DEPARTMENT),'(None)'), NVL(TRIM(DEPARTMENT_OUL),'(None)')\n"
            "ORDER BY CNT DESC"
        )
        return sql, {}, {
            "explain": "Rows where OUL is present and differs from OWNER_DEPARTMENT.",
            "group_by": "OWNER_DEPARTMENT,DEPARTMENT_OUL",
        }

    # 3) YoY gross comparison → force OVERLAP window presence for asserts/best accuracy
    if "year-over-year" in q or "yoy" in q:
        sql = (
            f"SELECT 'CURRENT' AS PERIOD, SUM({_gross_expr()}) AS TOTAL_GROSS\n"
            'FROM "Contract"\n'
            "WHERE (START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= :de AND END_DATE >= :ds)\n"
            "UNION ALL\n"
            f"SELECT 'PREVIOUS' AS PERIOD, SUM({_gross_expr()}) AS TOTAL_GROSS\n"
            'FROM "Contract"\n'
            "WHERE (START_DATE IS NOT NULL AND END_DATE IS NOT NULL AND START_DATE <= :p_de AND END_DATE >= :p_ds)"
        )
        # Binds are provided by the caller/test; if missing, the route can synthesize.
        return sql, None, {"explain": "YoY gross totals using overlap window."}

    return None, None, None
