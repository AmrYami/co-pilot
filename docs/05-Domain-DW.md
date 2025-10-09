# Domain DW Notes

## Equality alias expansion

`DW_EQ_ALIAS_COLUMNS` controls how equality filters expand user-friendly tokens into concrete database columns. For contracts we rely entirely on this database configuration:

- `DEPARTMENT` / `DEPARTMENTS` expand to `DEPARTMENT_1` … `DEPARTMENT_8` and `OWNER_DEPARTMENT`.
- `STAKEHOLDER` / `STAKEHOLDERS` expand to `CONTRACT_STAKEHOLDER_1` … `CONTRACT_STAKEHOLDER_8`.

The planner expands aliases first, then validates every resulting column against `DW_EXPLICIT_FILTER_COLUMNS`. Specific column references such as `DEPARTMENT_3` or `OWNER_DEPARTMENT` bypass alias fan-out and use only the requested column.
