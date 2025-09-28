from __future__ import annotations

"""Lightweight intent parser for Contract domain questions."""

import re
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class ContractIntent:
    action: str
    top_n: Optional[int] = None
    wants_all_columns: bool = True
    last_month: bool = False
    last_n_months: Optional[int] = None
    next_n_days: Optional[int] = None
    year_literal: Optional[int] = None
    ytd: bool = False
    ytd_year: Optional[int] = None
    notes: Dict[str, Any] = field(default_factory=dict)


_re_top = re.compile(r"\btop\s+(\d+)\b", re.I)
_re_last_n_months = re.compile(r"\blast\s+(\d+)\s+months?\b", re.I)
_re_last_month = re.compile(r"\blast\s+month\b", re.I)
_re_next_n_days = re.compile(r"\bnext\s+(\d+)\s+days?\b", re.I)
_re_year = re.compile(r"\b(20\d{2})\b")
_re_year_ytd = re.compile(r"\b(20\d{2})\s*ytd\b", re.I)
_re_ytd_year_post = re.compile(r"\bytd\s+(20\d{2})\b", re.I)


def parse_contract_intent(q: str) -> Optional[ContractIntent]:
    text = (q or "").strip()
    if not text:
        return None

    ci = ContractIntent(action="unknown")

    match_top = _re_top.search(text)
    if match_top:
        ci.top_n = int(match_top.group(1))

    lowered = text.lower()

    if "contracts expiring" in lowered and "count" in lowered:
        ci.action = "expiring_count"
        window = _re_next_n_days.search(text)
        if window:
            ci.next_n_days = int(window.group(1))
        return ci

    if "contracts with end_date" in lowered:
        ci.action = "expiring_list"
        window = _re_next_n_days.search(text)
        if window:
            ci.next_n_days = int(window.group(1))
        return ci

    if "requested last month" in lowered:
        ci.action = "list_requested_basic"
        ci.last_month = True
        ci.wants_all_columns = False
        return ci

    if "owner_department" in lowered and "department_oul" in lowered and (
        "mismatch" in lowered
        or "vs" in lowered
        or "versus" in lowered
        or "different" in lowered
        or "diff" in lowered
        or "compare" in lowered
    ):
        ci.action = "owner_vs_oul_mismatch"
        ci.wants_all_columns = False
        return ci

    if "vat" in lowered and ("null" in lowered or "zero" in lowered) and (
        "contract value" in lowered or "value > 0" in lowered
    ):
        ci.action = "vat_zero_net_pos"
        return ci

    if "request type" in lowered and "renewal" in lowered:
        ci.action = "reqtype_year"
        year = _re_year.search(text)
        if year:
            ci.year_literal = int(year.group(1))
        return ci

    if "distinct entity" in lowered or "entity values" in lowered:
        ci.action = "entity_counts"
        return ci

    if "owner department" in lowered and (
        "list" in lowered or "owners department" in lowered or "owneres" in lowered
    ):
        ci.action = "owner_dept_counts"
        return ci

    if "gross" in lowered and (
        "per owner department" in lowered or "by owner department" in lowered
    ):
        if "last quarter" in lowered:
            ci.action = "group_gross_owner_dept_last_window"
        else:
            ci.action = "group_gross_owner_dept_all_time"
        return ci

    if "ytd" in lowered:
        ci.ytd = True
        year_match = _re_year_ytd.search(text)
        if year_match:
            ci.ytd_year = int(year_match.group(1))
        else:
            post_match = _re_ytd_year_post.search(text)
            if post_match:
                ci.ytd_year = int(post_match.group(1))
            else:
                year = _re_year.search(text)
                if year:
                    ci.ytd_year = int(year.group(1))

    if "top" in lowered and "contracts" in lowered and "contract value" in lowered:
        if "gross" in lowered:
            ci.action = "top_gross"
        else:
            ci.action = "top_net"
        if _re_last_month.search(text):
            ci.last_month = True
        else:
            window = _re_last_n_months.search(text)
            if window:
                ci.last_n_months = int(window.group(1))
        return ci

    return None

