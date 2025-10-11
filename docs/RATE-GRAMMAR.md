# /dw/rate Grammar Cheatsheet

This note summarises the mini-language that rate feedback comments support. It
highlights the equality filter behaviour that now expands aliases and keeps OR
lists grouped in a single predicate.

## Equality filters

```
eq: ENTITY = DSFH | FARABI
```

* Columns accept either a single value (`=`) or an inline OR list separated by
  commas, the word `or`, or the `|` character.
* Filters are case-insensitive and trimmed by default.  This means we can group
  all values in one `IN` clause instead of emitting a separate comparison for
  every token.
* Aliases expand to all configured targets via `DW_EQ_ALIAS_COLUMNS`.  Some
  useful defaults:
  * `DEPARTMENT` → `DEPARTMENT_1 … DEPARTMENT_8` + `OWNER_DEPARTMENT`
  * `STAKEHOLDER` → `CONTRACT_STAKEHOLDER_1 … CONTRACT_STAKEHOLDER_8`
  * `ENTITY`, `REPRESENTATIVE_EMAIL` stay single-column fields.
* The generated SQL therefore looks like:

```
(UPPER(TRIM(DEPARTMENT_1)) IN (:eq_DEPARTMENT_0,:eq_DEPARTMENT_1)
 OR …
 OR UPPER(TRIM(OWNER_DEPARTMENT)) IN (:eq_DEPARTMENT_0,:eq_DEPARTMENT_1))
```

## LIKE filters

```
eq: ENTITY has AL FARABI
```

* `has`, `have`, and `contains` turn into a LIKE predicate.
* The same alias expansion occurs; each column receives an OR of LIKE checks.
* Bind values are wrapped in `%` automatically.

## Request type synonyms

`REQUEST_TYPE` preserves the legacy behaviour: configured `equals`, `prefix`,
and `contains` lists expand into `IN` / `LIKE` fragments, and any explicit value
from the hint joins the equality list.

## Full-text search hints

```
fts: "term one" | "term two"
```

* Tokens feed into the LIKE-based FTS engine when it is enabled.
* Each group becomes `(FTS(term one) OR FTS(term two))` in the boolean plan.

## Other knobs

* `group_by: REQUEST_TYPE`
* `order_by: REQUEST_DATE desc`
* `gross: true`

Every knob maps to the corresponding SELECT / ORDER BY / aggregation options in
`build_rate_sql`.

## Quick cheatsheet (AND/OR + groups)

Use the following mini-reference when writing `/dw/rate` comments that rely on
boolean groups:

* `fts: it or home care`
* `eq: ENTITY = DSFH or Farabi`
* `eq: REPRESENTATIVE_EMAIL = a@x.com or b@y.com`
* `eq: stakeholder = "Amr Taher" or "Abdulellah Fakeeh"`
  * expands to `CONTRACT_STAKEHOLDER_1 … CONTRACT_STAKEHOLDER_8`
* `eq: department = AL FARABI or SUPPORT SERVICES`
  * expands to `DEPARTMENT_1 … DEPARTMENT_8` plus `OWNER_DEPARTMENT`
* `order_by: REQUEST_DATE desc`

Grouping syntax:

* `group: or` starts a new block combined with OR against the previous block.
* Without `group: ...` hints, everything stays within the first (implicit) block
  and is AND-ed together.

Example:

```
fts: it or home care;
eq: ENTITY = DSFH or Farabi;
eq: REPRESENTATIVE_EMAIL = a@x.com or b@y.com;
group: or;
eq: stakeholder = Amr or Abdulellah;
eq: department = AL FARABI;
```

translates to:

```
WHERE (FTS ...) AND (ENTITY IN (...) AND REPRESENTATIVE_EMAIL IN (...))
   OR (CONTRACT_STAKEHOLDER_1..8 IN (...) AND DEPARTMENT_1..8/OWNER_DEPARTMENT IN (...))
```
