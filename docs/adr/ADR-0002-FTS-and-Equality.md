#!/usr/bin/env markdown
# ADR-0002: FTS & Equality Filters

**Decision**: FTS على أعمدة محدّدة بالإعدادات، Equality فقط للأعمدة المصرّح بها.  
**How**: `DW_FTS_COLUMNS`, `DW_EXPLICIT_FILTER_COLUMNS`, `DW_ENUM_SYNONYMS`.
