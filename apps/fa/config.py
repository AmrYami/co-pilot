"""
apps/fa/config.py — FA-specific configuration facade

Read-only getters that pull from the shared core Settings (DB→env→default).
This keeps FA-only keys separated so the core can be reused for other projects.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.settings import Settings

import os, json, time, yaml
from functools import lru_cache

def _read_yaml(p: str) -> dict:
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

@lru_cache(maxsize=32)
def load_metrics_yaml(path: str) -> dict:
    return _read_yaml(path)

def get_metrics(settings) -> dict:
    # precedence: DB setting -> env -> default path
    path = settings.get("FA_METRICS_PATH") or os.getenv("FA_METRICS_PATH") or "apps/fa/metrics"
    # support a directory (default) or a single file
    if os.path.isdir(path):
        # canonical file name
        full = os.path.join(path, "metrics.yaml")
    else:
        full = path
    try:
        return load_metrics_yaml(full)  # cached
    except Exception as e:
        return {"metrics": {}}


@dataclass
class FAConfig:
    db_url: str
    default_version: str
    prefix_regex: str
    sample_rows_per_table: int
    profile_stats: bool
    allow_multiprefix: bool
    collation_fallback: str
    join_graph_path: str | None
    metrics_path: str | None

    @classmethod
    def from_settings(cls, s: Settings) -> "FAConfig":
        return cls(
            db_url=s.get("FA_DB_URL"),
            default_version=s.get("FA_DEFAULT_VERSION", "2.4.17"),
            prefix_regex=s.get("FA_PREFIX_REGEX", r"^[0-9]+_$"),
            sample_rows_per_table=int(s.get("FA_SAMPLE_ROWS_PER_TABLE", 5)),
            profile_stats=str(s.get("FA_PROFILE_STATS", "false")).lower() in {"1","true","t","yes","y"},
            allow_multiprefix=str(s.get("FA_ALLOW_MULTIPREFIX", "true")).lower() in {"1","true","t","yes","y"},
            collation_fallback=s.get("FA_COLLATION_FALLBACK", "utf8mb4_general_ci"),
            join_graph_path=s.get("FA_JOIN_GRAPH_PATH"),
            metrics_path=s.get("FA_METRICS_PATH"),
        )
