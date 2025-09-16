from __future__ import annotations

"""DocuWare-specific hint helpers."""

DOCUWARE_PRIMARY_TABLE = "Contract"

MISSING_FIELD_QUESTIONS: dict[str, str] = {}
DOMAIN_HINTS: dict[str, dict] = {}

COLUMN_ALIASES = {
    "contract id": "CONTRACT_ID",
    "owner": "CONTRACT_OWNER",
    "stakeholder 1": "CONTRACT_STAKEHOLDER_1",
    "stakeholder 2": "CONTRACT_STAKEHOLDER_2",
    "stakeholder 3": "CONTRACT_STAKEHOLDER_3",
    "stakeholder 4": "CONTRACT_STAKEHOLDER_4",
    "stakeholder 5": "CONTRACT_STAKEHOLDER_5",
    "stakeholder 6": "CONTRACT_STAKEHOLDER_6",
    "stakeholder 7": "CONTRACT_STAKEHOLDER_7",
    "stakeholder 8": "CONTRACT_STAKEHOLDER_8",
    "department 1": "DEPARTMENT_1",
    "department 2": "DEPARTMENT_2",
    "department 3": "DEPARTMENT_3",
    "department 4": "DEPARTMENT_4",
    "department 5": "DEPARTMENT_5",
    "department 6": "DEPARTMENT_6",
    "department 7": "DEPARTMENT_7",
    "department 8": "DEPARTMENT_8",
    "owner department": "OWNER_DEPARTMENT",
    "net value": "CONTRACT_VALUE_NET_OF_VAT",
    "vat": "VAT",
    "gross value": None,
    "start date": "START_DATE",
    "end date": "END_DATE",
    "status": "CONTRACT_STATUS",
    "request date": "REQUEST_DATE",
    "request type": "REQUEST_TYPE",
    "department oul": "DEPARTMENT_OUL",
    "entity no": "ENTITY_NO",
    "year": "YEAR",
}


def make_fa_hints(*_args, **_kwargs) -> dict:
    return {}


def parse_admin_answer(text: str) -> dict:
    return parse_admin_reply_to_hints(text)


def parse_admin_reply_to_hints(text: str) -> dict:
    return {
        "tables": {"c": DOCUWARE_PRIMARY_TABLE},
        "date": {"column": "c.START_DATE"},
        "metric": {
            "key": "gross_value",
            "expr": "NVL(c.CONTRACT_VALUE_NET_OF_VAT,0) + NVL(c.VAT,0)",
        },
        "group_by": [],
        "order_by": [],
        "limit": 50,
    }


def seed_namespace(_settings, _namespace: str) -> dict:
    return {"join_graph": 0, "metrics": 0}
