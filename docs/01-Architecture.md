# Architecture Overview

Equality filters now perform an "alias expansion" step: the planner loads the `DW_EQ_ALIAS_COLUMNS` mapping, expands user tokens (for example `DEPARTMENT` or `STAKEHOLDERS`) into their configured column lists, and then enforces the `DW_EXPLICIT_FILTER_COLUMNS` allow-list before emitting SQL.
