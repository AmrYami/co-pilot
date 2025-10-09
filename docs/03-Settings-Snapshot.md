# DocuWare Settings Snapshot

Example namespace payload showing the equality alias configuration and token settings:

```json
{
  "namespace": "dw::common",
  "overrides": {
    "DW_EQ_ALIAS_COLUMNS": {
      "DEPARTMENT": [
        "DEPARTMENT_1",
        "DEPARTMENT_2",
        "DEPARTMENT_3",
        "DEPARTMENT_4",
        "DEPARTMENT_5",
        "DEPARTMENT_6",
        "DEPARTMENT_7",
        "DEPARTMENT_8",
        "OWNER_DEPARTMENT"
      ],
      "DEPARTMENTS": [
        "DEPARTMENT_1",
        "DEPARTMENT_2",
        "DEPARTMENT_3",
        "DEPARTMENT_4",
        "DEPARTMENT_5",
        "DEPARTMENT_6",
        "DEPARTMENT_7",
        "DEPARTMENT_8",
        "OWNER_DEPARTMENT"
      ],
      "STAKEHOLDER": [
        "CONTRACT_STAKEHOLDER_1",
        "CONTRACT_STAKEHOLDER_2",
        "CONTRACT_STAKEHOLDER_3",
        "CONTRACT_STAKEHOLDER_4",
        "CONTRACT_STAKEHOLDER_5",
        "CONTRACT_STAKEHOLDER_6",
        "CONTRACT_STAKEHOLDER_7",
        "CONTRACT_STAKEHOLDER_8"
      ],
      "STAKEHOLDERS": [
        "CONTRACT_STAKEHOLDER_1",
        "CONTRACT_STAKEHOLDER_2",
        "CONTRACT_STAKEHOLDER_3",
        "CONTRACT_STAKEHOLDER_4",
        "CONTRACT_STAKEHOLDER_5",
        "CONTRACT_STAKEHOLDER_6",
        "CONTRACT_STAKEHOLDER_7",
        "CONTRACT_STAKEHOLDER_8"
      ]
    },
    "DW_FTS_MIN_TOKEN_LEN": 2
  }
}
```
