#!/usr/bin/env python3
"""Apply table/column comments for QA core tables."""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute


def main() -> None:
    sql_path = Path(__file__).resolve().parent / "comments.sql"
    sql = sql_path.read_text(encoding="utf-8")
    execute(sql)
    print(f"Applied comments from: {sql_path}")


if __name__ == "__main__":
    main()
