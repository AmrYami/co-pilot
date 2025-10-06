# -*- coding: utf-8 -*-
"""Lightweight access helpers for DW settings namespace."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _ns(d: Dict[str, Any], key: str, default=None):
    return d.get(key, default)


class DWSettings:
    """Thin wrapper that exposes helpers around raw namespace settings."""

    def __init__(self, ns: Dict[str, Any]):
        self.ns = ns or {}

    def get(self, key: str, default=None):
        return _ns(self.ns, key, default)

    def get_fts_engine(self) -> str:
        eng = self.get("DW_FTS_ENGINE", "like")
        if isinstance(eng, str):
            normalized = eng.strip().lower()
        else:
            normalized = ""
        if normalized in {"like", "oracle-text"}:
            return normalized
        return "like"

    def get_fts_columns(self) -> Tuple[List[str], List[str]]:
        value = self.get("DW_FTS_COLUMNS", {}) or {}
        contract_cols = []
        wildcard_cols = []
        if isinstance(value, dict):
            raw_contract = value.get("Contract") or value.get("CONTRACT")
            if isinstance(raw_contract, list):
                contract_cols = list(raw_contract)
            raw_wild = value.get("*")
            if isinstance(raw_wild, list):
                wildcard_cols = list(raw_wild)
        return contract_cols, wildcard_cols

    def get_explicit_eq_columns(self) -> List[str]:
        value = self.get("DW_EXPLICIT_FILTER_COLUMNS", []) or []
        if isinstance(value, list):
            return list(value)
        return []

    def get_request_type_synonyms(self) -> Dict[str, Dict[str, List[str]]]:
        enum_map = self.get("DW_ENUM_SYNONYMS", {}) or {}
        if not isinstance(enum_map, dict):
            return {}
        value = enum_map.get("Contract.REQUEST_TYPE")
        if isinstance(value, dict):
            return value
        return {}
