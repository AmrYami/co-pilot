from __future__ import annotations
"""Column alias resolution helpers for the DW planner."""

from typing import Any, Dict

try:  # pragma: no cover - optional import path
    from apps.core.settings import get_setting  # type: ignore
except ImportError:  # pragma: no cover
    from apps.settings import get_setting_json as _get_setting_json  # type: ignore

    def get_setting(key: str, *, scope: str | None = None, namespace: str | None = None, default: Any | None = None) -> Any:
        target_namespace = namespace or "dw::common"
        try:
            return _get_setting_json(target_namespace, key, default)
        except TypeError:
            return _get_setting_json(target_namespace, key)


DEFAULT_ALIASES: Dict[str, str] = {
    "department": "OWNER_DEPARTMENT",
    "departments": "OWNER_DEPARTMENT",
    "owner department": "OWNER_DEPARTMENT",
    "owner_department": "OWNER_DEPARTMENT",
    "owner-department": "OWNER_DEPARTMENT",
    "oul": "DEPARTMENT_OUL",
    "department oul": "DEPARTMENT_OUL",
    "department_oul": "DEPARTMENT_OUL",
    "request type": "REQUEST_TYPE",
    "requesttype": "REQUEST_TYPE",
    "request_type": "REQUEST_TYPE",
    "requester": "REQUESTER",
    "status": "CONTRACT_STATUS",
    "contract status": "CONTRACT_STATUS",
    "contract_status": "CONTRACT_STATUS",
    "stakeholder": "STAKEHOLDER*",
    "stakeholders": "STAKEHOLDER*",
    "stackholder": "STAKEHOLDER*",
    "stackholders": "STAKEHOLDER*",
}


def _coerce_alias_map(settings: Any | None) -> Dict[str, str]:
    if isinstance(settings, dict):
        if "DW_COLUMN_ALIASES" in settings and isinstance(settings.get("DW_COLUMN_ALIASES"), dict):
            raw = settings["DW_COLUMN_ALIASES"]
        else:
            raw = settings
        if isinstance(raw, dict):
            return {
                str(k).strip().lower(): str(v).strip()
                for k, v in raw.items()
                if isinstance(k, str) and isinstance(v, str) and str(v).strip()
            }
    try:
        raw = get_setting("DW_COLUMN_ALIASES", scope="namespace")
    except TypeError:
        raw = get_setting("DW_COLUMN_ALIASES")
    if isinstance(raw, dict):
        return {
            str(k).strip().lower(): str(v).strip()
            for k, v in raw.items()
            if isinstance(k, str) and isinstance(v, str) and str(v).strip()
        }
    return {}


def resolve_column_alias(col: str, *, settings: Any | None = None) -> str:
    """Resolve a human-friendly column name to its canonical Contract column."""

    if not col:
        return col

    key = str(col).strip().lower()
    dyn = _coerce_alias_map(settings)
    if key in dyn:
        return dyn[key]
    return DEFAULT_ALIASES.get(key, col)


__all__ = ["resolve_column_alias", "DEFAULT_ALIASES"]
