"""DocuWare hint helpers used by the pipeline."""

DOCUWARE_DEFAULT_TABLE = '"Contract"'
PREFERRED_DATE_COLUMNS = ["START_DATE", "END_DATE", "REQUEST_DATE"]

METRIC_SQL = {
    "contract_value_net": "NVL(CONTRACT_VALUE_NET_OF_VAT, 0)",
    "contract_value_vat": "NVL(VAT, 0)",
    "contract_value_gross": "NVL(CONTRACT_VALUE_NET_OF_VAT, 0) + NVL(VAT, 0)",
}


def default_table() -> str:
    """Return the default DocuWare table name."""

    return DOCUWARE_DEFAULT_TABLE


def preferred_dates() -> list[str]:
    """Return preferred date column names for DocuWare tables."""

    return PREFERRED_DATE_COLUMNS


def metric_sql_map() -> dict[str, str]:
    """Return metric-key to SQL-expression mapping."""

    return METRIC_SQL
