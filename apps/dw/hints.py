"""Minimal DocuWare hint helpers used by the pipeline."""


def get_join_hints(namespace: str = "dw::common"):
    """Return join hints for the DocuWare namespace."""
    # Single-table for now; no joins required.
    return []


def get_metric_hints(namespace: str = "dw::common"):
    """Return metric key to SQL expression mappings."""
    return {
        "contract_value_gross": "NVL(CONTRACT_VALUE_NET_OF_VAT,0) + NVL(VAT,0)",
        "contract_value_net": "NVL(CONTRACT_VALUE_NET_OF_VAT,0)",
        "vat": "NVL(VAT,0)",
    }


def get_reserved_terms(namespace: str = "dw::common"):
    """Return reserved terms to help the planner map synonyms."""
    return {
        "contract": "Contract",
        "contracts": "Contract",
        "owner": "CONTRACT_OWNER",
        "stakeholder": "contract_stakeholder",
        "department": "department",
    }


def get_date_columns(namespace: str = "dw::common"):
    """Return date columns for the DocuWare Contract table."""
    return {
        "Contract": [
            "START_DATE",
            "END_DATE",
            "REQUEST_DATE",
            "EXPIERY_30",
            "EXPIERY_60",
            "EXPIERY_90",
        ]
    }
