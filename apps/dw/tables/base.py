from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.settings import Settings


@dataclass
class TableSpec:
    """
    Generic, table-agnostic contract for DW tables.
    Everything that is table-specific lives in subclasses (e.g. ContractSpec).
    """
    name: str
    # Date semantics:
    #   "OVERLAP"      -> active window (START<=end && END>=start)
    #   "REQUEST_DATE" -> use REQUEST_DATE BETWEEN :start AND :end
    default_date_mode: str = "OVERLAP"
    request_date_col: str = "REQUEST_DATE"
    start_date_col: str   = "START_DATE"
    end_date_col: str     = "END_DATE"

    # Measures:
    value_col_net: str = "CONTRACT_VALUE_NET_OF_VAT"
    value_col_vat: str = "VAT"

    # Dimension synonyms (natural language -> column name)
    dimension_map: Dict[str, str] = field(default_factory=dict)

    # Optional default FTS columns if settings does not provide them
    fts_default: List[str] = field(default_factory=list)

    # ---------- value expressions ----------
    def net_expr(self) -> str:
        return f"NVL({self.value_col_net},0)"

    def gross_expr(self) -> str:
        # VAT can be a rate (0..1) or absolute; handle both.
        return (
            f"NVL({self.value_col_net},0) + "
            f"CASE WHEN NVL({self.value_col_vat},0) BETWEEN 0 AND 1 "
            f"THEN NVL({self.value_col_net},0) * NVL({self.value_col_vat},0) "
            f"ELSE NVL({self.value_col_vat},0) END"
        )

    # ---------- predicates ----------
    def overlap_predicate(self, start_bind=":date_start", end_bind=":date_end",
                          strict: bool = False) -> str:
        s, e = self.start_date_col, self.end_date_col
        if strict:
            return f"({s} <= {end_bind} AND {e} >= {start_bind})"
        return f"(({s} IS NULL OR {s} <= {end_bind}) AND ({e} IS NULL OR {e} >= {start_bind}))"

    def request_date_predicate(self, start_bind=":date_start", end_bind=":date_end") -> str:
        col = self.request_date_col
        return f"{col} BETWEEN {start_bind} AND {end_bind}"

    # ---------- FTS columns ----------
    def fts_columns(self, settings: "Settings") -> List[str]:
        cfg = (settings.get("DW_FTS_COLUMNS") or {})
        # Per-table
        if self.name in cfg and isinstance(cfg[self.name], list):
            return cfg[self.name]
        # Wildcard override
        if "*" in cfg and isinstance(cfg["*"], list):
            return cfg["*"]
        # Fallback
        return list(self.fts_default)

    # ---------- synonyms ----------
    def synonym(self, noun: str) -> Optional[str]:
        return self.dimension_map.get((noun or "").strip().lower())
