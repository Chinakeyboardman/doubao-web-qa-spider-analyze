#!/usr/bin/env python3
"""执行 migrate_v8.sql（乐观锁迁移）— 仅 PostgreSQL 需要存储函数迁移。

MySQL 模式下 claim_pending_* 逻辑由 shared/claim_functions.py 在 Python 层实现，
无需数据库端存储函数。
"""
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.config import CONFIG
from shared.db import get_connection


def main():
    db_type = CONFIG["db_type"]
    if db_type != "postgresql":
        print(f"当前 DB_TYPE={db_type}，claim_pending 逻辑由 Python 实现，无需执行此迁移。")
        return

    sql_path = _PROJECT_ROOT / "init-db" / "postgresql" / "migrate_v8.sql"
    if not sql_path.exists():
        sql_path = _PROJECT_ROOT / "init-db" / "migrate_v8.sql"
    sql = sql_path.read_text()
    try:
        conn = get_connection()
    except Exception as e:
        print("数据库连接失败，请确保 PostgreSQL 已启动：", e)
        sys.exit(1)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
    conn.close()
    print("migrate_v8.sql 执行成功")


if __name__ == "__main__":
    main()
