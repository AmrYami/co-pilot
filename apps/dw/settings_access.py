# -*- coding: utf-8 -*-
"""Lightweight access helpers for DW settings namespace."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

try:  # pragma: no cover - optional dependency in some environments
    from apps.dw.settings_util import get_setting as _get_setting
except Exception:  # pragma: no cover - fallback used in tests
    def _get_setting(key: str, *, scope=None, namespace=None, default=None):
        return default


def _ns(d: Dict[str, Any], key: str, default=None):
    return d.get(key, default)


def _dedupe_columns(columns: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for raw in columns:
        text = str(raw or "").strip()
        if not text:
            continue
        if text.startswith('"') and text.endswith('"'):
            key = text
        else:
            key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


class DWSettings:
    """Thin wrapper that exposes helpers around raw namespace settings."""

    def __init__(self, payload: Dict[str, Any] | None = None):
        payload = payload or {}
        namespace = payload.get("__namespace__") if isinstance(payload, dict) else None
        global_ns = payload.get("__global__") if isinstance(payload, dict) else None
        if namespace is None:
            namespace = payload or {}
        self.ns = namespace
        self.global_ns = global_ns or {}

    def get(self, key: str, default=None):
        return _ns(self.ns, key, default)

    def get_with_global(self, key: str, default=None):
        if key in self.ns:
            return self.ns[key]
        if key in self.global_ns:
            return self.global_ns[key]
        return default

    def _settings_lookup(self, key: str, namespace: str | None, default=None):
        ns = namespace or "dw::common"
        try:
            return _get_setting(key, scope="namespace", namespace=ns, default=default)
        except TypeError:
            # Legacy helper signature without ``default`` parameter.
            value = _get_setting(key, scope="namespace", namespace=ns)
            return value if value is not None else default

    def get_fts_engine(self) -> str:
        eng = self.get_with_global("DW_FTS_ENGINE", "like")
        if isinstance(eng, str):
            normalized = eng.strip().lower()
        else:
            normalized = ""
        if normalized in {"like", "oracle-text"}:
            return normalized
        return "like"

    def get_fts_columns(self) -> Tuple[List[str], List[str]]:
        value = self.get_with_global("DW_FTS_COLUMNS", {}) or {}
        contract_cols: List[str] = []
        wildcard_cols: List[str] = []
        if isinstance(value, dict):
            raw_contract = value.get("Contract") or value.get("CONTRACT")
            if isinstance(raw_contract, list):
                contract_cols = list(raw_contract)
            raw_wild = value.get("*")
            if isinstance(raw_wild, list):
                wildcard_cols = list(raw_wild)
        return contract_cols, wildcard_cols

    def get_explicit_eq_columns(self) -> List[str]:
        value = self.get_with_global("DW_EXPLICIT_FILTER_COLUMNS", []) or []
        if isinstance(value, list):
            return list(value)
        return []

    def get_request_type_synonyms(self) -> Dict[str, Dict[str, List[str]]]:
        enum_map = self.get_with_global("DW_ENUM_SYNONYMS", {}) or {}
        if not isinstance(enum_map, dict):
            return {}
        value = enum_map.get("Contract.REQUEST_TYPE")
        if isinstance(value, dict):
            return value
        return {}

    def resolve_fts_config(
        self,
        *,
        tokens: Sequence[str] | None,
        table_name: str = "Contract",
        namespace: str | None = None,
    ) -> Dict[str, Any]:
        """Resolve FTS configuration with namespace/global fallback and token filtering."""

        ns_key = namespace or "dw::common"
        engine = self.get_with_global("DW_FTS_ENGINE")
        if not engine:
            engine = self._settings_lookup("DW_FTS_ENGINE", ns_key, default=None)
        if not engine:
            engine = self._settings_lookup("DW_FTS_ENGINE", "global", default=None)
        engine_missing = not engine
        engine_text = str(engine or "like").strip().lower() or "like"

        columns_map = self.get_with_global("DW_FTS_COLUMNS")
        if not columns_map:
            columns_map = self._settings_lookup("DW_FTS_COLUMNS", ns_key, default={})
        if not columns_map:
            columns_map = self._settings_lookup("DW_FTS_COLUMNS", "global", default={})

        columns: List[str] = []
        if isinstance(columns_map, dict):
            canonical = table_name.strip("\"")
            candidates = [table_name, canonical, canonical.upper(), canonical.lower(), "*"]
            for key in candidates:
                raw = columns_map.get(key)
                if isinstance(raw, list) and raw:
                    columns = _dedupe_columns(str(col) for col in raw if isinstance(col, str))
                    if columns:
                        break

        min_len_raw = self.get_with_global("DW_FTS_MIN_TOKEN_LEN")
        if min_len_raw is None:
            min_len_raw = self._settings_lookup("DW_FTS_MIN_TOKEN_LEN", ns_key, default=None)
        if min_len_raw is None:
            min_len_raw = self._settings_lookup("DW_FTS_MIN_TOKEN_LEN", "global", default=None)
        try:
            min_len = max(1, int(min_len_raw)) if min_len_raw is not None else 2
        except (TypeError, ValueError):
            min_len = 2

        filtered_tokens: List[str] = []
        for token in tokens or []:
            if not isinstance(token, str):
                continue
            cleaned = token.strip()
            if len(cleaned) < min_len:
                continue
            filtered_tokens.append(cleaned)

        config: Dict[str, Any] = {
            "enabled": bool(filtered_tokens),
            "engine": engine_text or "like",
            "columns": columns,
            "tokens": filtered_tokens,
            "min_token_len": min_len,
        }
        if engine_missing:
            config["error"] = "no_engine"
        if not columns:
            config["error"] = "no_columns"
        return config
