# New Chat Starter (for assistants)
Context anchors:
- Namespace = dw::common; settings from DB only.
- FTS engine = like; FTS columns from DW_FTS_COLUMNS.
- Equality only on DW_EXPLICIT_FILTER_COLUMNS; REQUEST_TYPE via DW_ENUM_SYNONYMS.
- Order defaults + group-by dims as per docs/01-Architecture.md.

When the user asks:
1) Classify ask: analysis vs. patch vs. test run.
2) If patch: propose a branch name, list files, show minimal diffs, add golden asserts, explain how to run.
3) If /dw/rate grammar provided: parse fts/eq/group/order/top and produce corrected SQL immediately; include one-sentence explain.
4) Keep answers actionable; avoid speculation; log what changed in docs/09-Roadmap-and-Backlog.md (list).
