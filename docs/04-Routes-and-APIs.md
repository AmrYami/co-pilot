# المسارات والواجهات (Routes & APIs)

## POST /dw/answer
**Input**: `{question, full_text_search?, auth_email?, prefixes?}`  
**Output**: `{ok, rows, columns, sql, meta{intent, fts, binds, explain, ...}}`

## POST /dw/rate
> Grammar مختصرة داخل `comment`:
```
fts: tok1 | tok2
eq: COLUMN = VALUE (ci, trim)
group_by: COL1, COL2
order_by: COL asc|desc
top: N by COL   # أو bottom: N by COL
```
**Effect**: يعيد تخطيطًا مصححًا في نفس الرد.

## POST /admin/run_golden
يشغّل Golden suites ويعيد تقريرًا.

## POST /dw/admin/explain
يرسم صفحة HTML للـ rationale (أرسل JSON /dw/answer كـ body).
