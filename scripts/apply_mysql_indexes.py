#!/usr/bin/env python3
"""为已有 MySQL 库应用索引优化（init-db/mysql/migrate_indexes.sql）。

用法：
    ./venv/bin/python scripts/apply_mysql_indexes.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

import pymysql


def main():
    path = _PROJECT_ROOT / "init-db" / "mysql" / "migrate_indexes.sql"
    if not path.exists():
        print(f"Not found: {path}")
        sys.exit(1)
    sql = path.read_text(encoding="utf-8")
    lines = [l for l in sql.split("\n") if not l.strip().startswith("--")]
    sql = "\n".join(lines)
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "doubao"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                if not stmt:
                    continue
                try:
                    cur.execute(stmt)
                    print(f"OK: {stmt[:60]}...")
                except pymysql.err.OperationalError as e:
                    if "1061" in str(e) or "Duplicate key name" in str(e).lower():
                        print(f"SKIP (exists): {stmt[:50]}...")
                    else:
                        raise
        conn.commit()
        print("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
