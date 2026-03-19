#!/usr/bin/env python3
"""将 PostgreSQL 的 QA 业务表数据迁移到 MySQL。

用法：
    ./venv/bin/python scripts/migrate_pg_to_mysql.py

    # 若目标表已有数据，先清空再迁移（默认）
    ./venv/bin/python scripts/migrate_pg_to_mysql.py --truncate

    # 不清空，仅追加（遇唯一键冲突则跳过）
    ./venv/bin/python scripts/migrate_pg_to_mysql.py --no-truncate

前置条件：
    1. .env 中配置好 PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD（源）
    2. .env 中配置好 MYSQL_HOST/MYSQL_PORT/MYSQL_DATABASE/MYSQL_USER/MYSQL_PASSWORD（目标）
    3. MySQL 已创建数据库（若不存在会自动创建）
    4. MySQL 中表结构已就绪（若不存在会执行 init-db/mysql/init.sql）
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(_PROJECT_ROOT / ".env")

import psycopg2
from psycopg2.extras import RealDictCursor
import pymysql


def _pg_conn():
    import os
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "doubao"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def _mysql_conn():
    import os
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "doubao"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def _ensure_mysql_db(db_name: str):
    """确保 MySQL 数据库存在。"""
    import os
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
    finally:
        conn.close()


def _run_mysql_init(mysql_conn):
    """执行 MySQL init.sql 创建表结构。"""
    init_path = _PROJECT_ROOT / "init-db" / "mysql" / "init.sql"
    if not init_path.exists():
        raise FileNotFoundError(f"init.sql not found: {init_path}")
    sql = init_path.read_text(encoding="utf-8")
    # 移除整行注释
    lines = [line for line in sql.split("\n") if not line.strip().startswith("--")]
    sql = "\n".join(lines)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with mysql_conn.cursor() as cur:
        for stmt in statements:
            if stmt:
                try:
                    cur.execute(stmt)
                except pymysql.err.OperationalError as e:
                    # 1050: table exists, 1061: duplicate key name
                    if "already exists" in str(e).lower() or "1050" in str(e) or "1061" in str(e):
                        pass
                    else:
                        raise
    mysql_conn.commit()


def _truncate_mysql_tables(mysql_conn):
    """按外键逆序清空 MySQL 表（避免 FK 约束）。"""
    tables = ["qa_link_video", "qa_link_content", "qa_link", "qa_answer", "qa_query"]
    with mysql_conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for t in tables:
            cur.execute(f"TRUNCATE TABLE {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    mysql_conn.commit()


# 单字段 JSON 最大字节数，避免超过 MySQL max_allowed_packet（默认 16MB）
_JSON_MAX_BYTES = 7 * 1024 * 1024


def _json_serialize(val):
    """将 dict/list 转为 JSON 字符串，None 保持 None。超长则替换为截断说明，避免 MySQL gone away。"""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        s = json.dumps(val, ensure_ascii=False)
        enc = s.encode("utf-8")
        if len(enc) > _JSON_MAX_BYTES:
            preview = s[:2000] + "..." if len(s) > 2000 else s
            s = json.dumps(
                {"_truncated": True, "original_bytes": len(enc), "preview": preview},
                ensure_ascii=False,
            )
        return s
    return val


def _adapt_value(val):
    """适配 PG -> MySQL 类型（date/datetime 转 str）。"""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _bool_to_int(val):
    """将 Python bool 转为 MySQL TINYINT(1)。"""
    if val is None:
        return None
    return 1 if val else 0


def migrate_table(pg_cur, conn_holder: dict, table: str, columns: list[str], *, json_cols: list[str] | None = None, bool_cols: list[str] | None = None):
    """迁移单表数据。连接断开时创建新连接。conn_holder 为 {"conn": mysql_conn} 便于替换。"""
    json_cols = json_cols or []
    bool_cols = bool_cols or []
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
    select_sql = f"SELECT {cols_str} FROM {table} ORDER BY id"
    pg_cur.execute(select_sql)
    rows = pg_cur.fetchall()
    count = 0
    mysql_conn = conn_holder["conn"]
    mysql_cur = mysql_conn.cursor()
    try:
        for row in rows:
            vals = []
            for c in columns:
                v = row.get(c)
                if c in json_cols:
                    v = _json_serialize(v)
                elif c in bool_cols:
                    v = _bool_to_int(v)
                else:
                    v = _adapt_value(v)
                vals.append(v)
            try:
                mysql_cur.execute(insert_sql, vals)
                if mysql_cur.rowcount > 0:
                    count += 1
            except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
                err_str = str(e).lower()
                if "gone away" in err_str or "2006" in str(e) or "broken pipe" in err_str or "interfaceerror" in err_str:
                    # 连接已死，创建全新连接
                    try:
                        mysql_cur.close()
                    except Exception:
                        pass
                    try:
                        mysql_conn.close()
                    except Exception:
                        pass
                    conn_holder["conn"] = _mysql_conn()
                    mysql_conn = conn_holder["conn"]
                    mysql_cur = mysql_conn.cursor()
                    try:
                        mysql_cur.execute(insert_sql, vals)
                        if mysql_cur.rowcount > 0:
                            count += 1
                    except Exception as retry_e:
                        print(f"  [WARN] {table} row id={row.get('id')} (retry failed): {retry_e}")
                else:
                    print(f"  [WARN] {table} row id={row.get('id')}: {e}")
            except Exception as e:
                print(f"  [WARN] {table} row id={row.get('id')}: {e}")
    finally:
        mysql_cur.close()
    return count


def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description="PostgreSQL -> MySQL 数据迁移")
    parser.add_argument("--no-truncate", action="store_true", help="不清空目标表，仅追加（默认会先清空）")
    args = parser.parse_args()
    do_truncate = not args.no_truncate

    db_name = os.getenv("MYSQL_DATABASE", "doubao")
    print("1. 确保 MySQL 数据库存在...")
    _ensure_mysql_db(db_name)

    print("2. 连接 PostgreSQL 和 MySQL...")
    pg_conn = _pg_conn()
    mysql_conn = _mysql_conn()
    conn_holder = {"conn": mysql_conn}

    try:
        print("3. 执行 MySQL init.sql（若表不存在）...")
        _run_mysql_init(mysql_conn)

        if do_truncate:
            print("4. 清空 MySQL 目标表...")
            _truncate_mysql_tables(mysql_conn)

        pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)

        try:
            # 按外键依赖顺序迁移
            tables_config = [
                ("qa_query", [
                    "query_id", "query_text", "category", "intent_type", "query_date", "time_slot",
                    "status", "error_message", "retry_count", "screenshot_path", "remark",
                    "created_at", "updated_at",
                ], [], []),
                ("qa_answer", [
                    "query_id", "answer_text", "answer_length", "status", "has_citation", "citation_count",
                    "raw_data", "created_at", "updated_at",
                ], ["raw_data"], ["has_citation"]),
                ("qa_link", [
                    "query_id", "link_id", "link_url", "platform", "content_format", "publish_time",
                    "popularity", "status", "error_message", "retry_count", "fetched_at",
                    "created_at", "updated_at",
                ], [], []),
                ("qa_link_content", [
                    "link_id", "content_json", "raw_json", "video_parse_status", "status",
                    "created_at", "updated_at",
                ], ["content_json", "raw_json"], []),
                ("qa_link_video", [
                    "link_id", "video_id", "play_url", "cover_url", "duration",
                    "video_path", "audio_path", "stt_text", "subtitles",
                    "transcript_model", "transcript_source", "model_api_file_id", "model_api_input_type",
                    "raw_api_response", "status", "error_message", "retry_count",
                    "fetched_at", "transcribed_at", "created_at", "updated_at",
                ], ["subtitles", "raw_api_response"], []),
            ]

            print("5. 迁移数据...")
            total = 0
            for table, columns, json_cols, bool_cols in tables_config:
                n = migrate_table(pg_cur, conn_holder, table, columns, json_cols=json_cols, bool_cols=bool_cols)
                total += n
                print(f"   {table}: {n} 行")

            conn_holder["conn"].commit()
            print(f"\n迁移完成，共 {total} 行。")
            print(f"请将 .env 中 DB_TYPE=mysql 后重启应用。")

        finally:
            pg_cur.close()
    finally:
        pg_conn.close()
        try:
            conn_holder["conn"].close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
