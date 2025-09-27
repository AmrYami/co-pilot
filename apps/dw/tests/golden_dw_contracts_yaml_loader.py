from __future__ import annotations
import os
import yaml


def load_golden():
    path = os.path.join(os.path.dirname(__file__), "golden_dw_contracts.yaml")
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
