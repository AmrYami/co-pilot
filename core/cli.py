"""
Tiny CLI helper for ingestion and Q&A.

Usage:
  python -m core.cli ingest 2_ 3_
  python -m core.cli ask 2_ "top 10 customers by sales last month"
"""
from __future__ import annotations

import sys
from typing import List

from core.settings import Settings
from core.pipeline import Pipeline


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("usage: python -m core.cli <ingest|ask> <prefix> [more] [question]")
        return 2
    cmd = argv[1]
    if cmd == "ingest":
        prefixes = argv[2:]
        if not prefixes:
            print("provide at least one prefix")
            return 2
        settings = Settings(namespace=f"fa::{prefixes[0]}")
        pipe = Pipeline(settings=settings, namespace=f"fa::{prefixes[0]}")
        snaps = pipe.ensure_ingested("fa", prefixes)
        print(snaps)
        return 0
    elif cmd == "ask":
        if len(argv) < 4:
            print("usage: python -m core.cli ask <prefix> <question>")
            return 2
        prefix = argv[2]
        question = " ".join(argv[3:])
        settings = Settings(namespace=f"fa::{prefix}")
        pipe = Pipeline(settings=settings, namespace=f"fa::{prefix}")
        res = pipe.answer("fa", [prefix], question)
        print(res)
        return 0
    else:
        print(f"unknown command: {cmd}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
