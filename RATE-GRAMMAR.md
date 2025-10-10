# /dw/rate Grammar Guide

This document summarises the user-facing grammar that the `/dw/rate` endpoint and
DocuWare contract builder understand. The goal is to make quick copy/paste
instructions easy for analysts while keeping backwards compatibility with the
legacy pipe-delimited syntax.

## Full-text search (FTS)

* `fts:` accepts tokens separated by the human friendly word `or`.
  * Example: `fts: it or home care`.
* Commas and the legacy `|` separator are still accepted.
* Use `and` (or `&`) when you need the AND operator: `fts: acute and oncology`.

## Equality filters

* `eq:` still expects `COLUMN = value` clauses separated by semicolons.
* Each clause can contain multiple values joined with `or`, commas, or `|`.
  * Example: `eq: ENTITY = DSFH or AL FARABI` produces two comparisons that are
    OR-ed together.
* Case-insensitive (`ci`) and trimming (`trim`) flags remain available via
  parentheses: `eq: ENTITY = DSFH (ci, trim)`.
* Columns defined in `DW_EQ_ALIAS_COLUMNS` expand automatically (e.g.
  `DEPARTMENT` → the 9 department columns, `STAKEHOLDER` → the stakeholder slots).

## LIKE-style operators

* `contains:`, `has:`, and `have:` follow the same syntax as `eq:` but emit
  `LIKE` comparisons instead of `=`.
  * Example: `has: ENTITY = DSFH or Farabi` becomes
    `UPPER(TRIM(ENTITY)) LIKE UPPER(TRIM('%DSFH%')) OR ...`.

## Boolean grouping

The natural-language question is analysed into OR-groups:

* Terms joined with `and` stay inside the same group (AND-ed together).
* An `or` between different columns starts a new group.
* Groups are combined with `(...) OR (...)` in the generated SQL.
* Example question:
  ```
  list all contracts has it or home care and ENTITY = DSFH
  and REPRESENTATIVE_EMAIL = samer@procare-sa.com
  or stakeholder = Amr Taher A Maghrabi and department = AL FARABI
  ```
  Produces two groups joined by OR with the expected LIKE/`=` combinations.

## ORDER BY

* `order_by:` accepts `COLUMN asc|desc`. If the direction is omitted we default
  to `DESC` and render the SQL as `ORDER BY COLUMN DESC` (no `_DESC` suffix).

## Debug metadata

* When the configured full-text engine is `like`, debug output returns
  `debug["fts"]["engine"] = "like"` without the legacy `error: "no_engine"`
  flag to avoid confusion.

## Export script

`scripts/export_context.py` supports SQLAlchemy 2.0 and auto-detects column
names (e.g. `question_norm` vs `q_norm`, `created_at` vs `updated_at`). Set
`MEMORY_DB_URL` in the environment or `.env` file before running it.

