#!/usr/bin/env python3
"""Run database initialization / migration scripts based on DB_TYPE.

Usage:
    python scripts/run_migrations.py init          # run init.sql
    python scripts/run_migrations.py migrate_v8    # run specific migration
    python scripts/run_migrations.py --list        # list available scripts
"""

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.config import CONFIG
from shared.db import get_connection


def _get_sql_dir() -> Path:
    db_type = CONFIG["db_type"]
    d = _PROJECT_ROOT / "init-db" / db_type
    if not d.exists():
        print(f"No SQL directory for DB_TYPE={db_type}: {d}")
        sys.exit(1)
    return d


def list_scripts():
    sql_dir = _get_sql_dir()
    files = sorted(sql_dir.glob("*.sql"))
    print(f"Available scripts for {CONFIG['db_type']} ({sql_dir}):")
    for f in files:
        print(f"  {f.stem}")


def run_script(name: str):
    sql_dir = _get_sql_dir()
    sql_file = sql_dir / f"{name}.sql"
    if not sql_file.exists():
        sql_file = sql_dir / name
        if not sql_file.exists():
            print(f"Script not found: {sql_file}")
            sys.exit(1)

    sql = sql_file.read_text(encoding="utf-8")
    conn = get_connection()
    try:
        if CONFIG["db_type"] == "postgresql":
            conn.autocommit = True
        cur = conn.cursor()
        try:
            if CONFIG["db_type"] == "mysql":
                for statement in _split_mysql_statements(sql):
                    if statement.strip():
                        cur.execute(statement)
                conn.commit()
            else:
                cur.execute(sql)
        finally:
            cur.close()
    finally:
        conn.close()
    print(f"Executed {sql_file.name} ({CONFIG['db_type']})")


def _split_mysql_statements(sql: str) -> list[str]:
    """Naive statement splitter for MySQL (split on ';' outside DELIMITER blocks)."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def main():
    parser = argparse.ArgumentParser(description="Run DB init/migration scripts")
    parser.add_argument("script", nargs="?", help="Script name (e.g. init, migrate_v8)")
    parser.add_argument("--list", action="store_true", help="List available scripts")
    args = parser.parse_args()

    if args.list or not args.script:
        list_scripts()
        return

    run_script(args.script)


if __name__ == "__main__":
    main()
