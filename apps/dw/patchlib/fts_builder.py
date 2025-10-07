# -*- coding: utf-8 -*-
"""
FTS WHERE builder based on DW_FTS_COLUMNS and tokens.
Supports operator: OR / AND.
Engine "like" implemented here; "oracle-text" placeholder for future.
"""
from __future__ import annotations

from typing import Tuple, Dict, List


def build_like_fts(columns: List[str], tokens_groups: List[List[str]]) -> Tuple[str, Dict[str, str]]:
    """
    tokens_groups is a list of groups. Each group is ORed inside, ANDed between groups if len>1.
    Example:
        [["it"], ["home care"]] → (group1) OR (group2)  (if caller wants OR between groups)
        For AND behavior, caller should pass AND-mode; here we only build per-group OR block.
    We will return a generic AND over groups; caller decides to OR or AND by how he passes groups.
    For simplicity: we AND between groups (each group can contain multiple tokens ORed).
    If you want OR-between-groups, pass a single group with all tokens.
    """
    where_parts = []
    binds: Dict[str, str] = {}
    bidx = 0
    for group in tokens_groups:
        ors = []
        for tok in group:
            bind_name = f"fts_{bidx}"
            bidx += 1
            binds[bind_name] = f"%{tok}%"
            ors.append("(" + " OR ".join([f"UPPER(NVL({col},'')) LIKE UPPER(:{bind_name})" for col in columns]) + ")")
        if ors:
            where_parts.append("(" + " OR ".join(ors) + ")")
    if not where_parts:
        return "", {}
    # AND groups (see planner that chooses AND vs OR composition)
    return "(" + " AND ".join(where_parts) + ")", binds
