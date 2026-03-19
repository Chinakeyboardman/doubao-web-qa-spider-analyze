#!/usr/bin/env python3
"""
从 Query生成_测试集.xlsx 的 Sheet5_生成结果 导入 qa_query 表。

用法：
    python import_queries.py                          # 默认读取上级目录的 xlsx
    python import_queries.py --file /path/to/file.xlsx
    python import_queries.py --dry-run                # 只打印，不写库

依赖：
    pip install openpyxl psycopg2-binary python-dotenv
"""

import argparse
import sys
from pathlib import Path

import openpyxl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.db import get_connection, execute as db_execute
from shared.sql_builder import sb

DEFAULT_XLSX = PROJECT_ROOT / "Query生成_测试集.xlsx"
SHEET_NAME = "Sheet5_生成结果"


def parse_xlsx(filepath: Path):
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb[SHEET_NAME]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    header = rows[0]
    print(f"表头: {header}")
    print(f"数据行数: {len(rows) - 1}")

    records = []
    for row in rows[1:]:
        seq, industry, category, query_text, intent_type, tag_combo, rule_id = row
        query_id = f"Q{int(seq):04d}"
        records.append({
            "query_id": query_id,
            "query_text": str(query_text).strip(),
            "category": f"{industry}/{category}" if industry else str(category),
            "intent_type": str(intent_type).strip() if intent_type else None,
            "remark": f"标签组合={tag_combo}, 规则={rule_id}",
        })
    return records


def import_to_db(records, dry_run=False):
    if dry_run:
        for r in records[:5]:
            print(f"  [DRY-RUN] {r['query_id']} | {r['category']} | {r['intent_type']} | {r['query_text'][:40]}")
        print(f"  ... 共 {len(records)} 条")
        return

    inserted = 0
    skipped = 0
    _ignore_prefix = sb.insert_ignore_prefix()
    _do_nothing = sb.upsert_do_nothing(["query_id"])
    for r in records:
        if sb.is_mysql:
            n = db_execute(
                f"{_ignore_prefix} INTO qa_query (query_id, query_text, category, intent_type, remark) "
                "VALUES (%s, %s, %s, %s, %s)",
                (r["query_id"], r["query_text"], r["category"], r["intent_type"], r["remark"]),
            )
        else:
            n = db_execute(
                "INSERT INTO qa_query (query_id, query_text, category, intent_type, remark) "
                f"VALUES (%s, %s, %s, %s, %s) {_do_nothing}",
                (r["query_id"], r["query_text"], r["category"], r["intent_type"], r["remark"]),
            )
        if n == 1:
            inserted += 1
        else:
            skipped += 1
    print(f"导入完成: 新增 {inserted} 条, 跳过(已存在) {skipped} 条")


def main():
    parser = argparse.ArgumentParser(description="导入 Query 到 qa_query 表")
    parser.add_argument("--file", type=Path, default=DEFAULT_XLSX, help="xlsx 文件路径")
    parser.add_argument("--dry-run", action="store_true", help="只解析不写库")
    args = parser.parse_args()

    if not args.file.exists():
        print(f"文件不存在: {args.file}")
        sys.exit(1)

    print(f"读取文件: {args.file}")
    records = parse_xlsx(args.file)
    import_to_db(records, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
