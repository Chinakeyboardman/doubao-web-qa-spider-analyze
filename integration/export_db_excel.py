#!/usr/bin/env python3
"""Export core QA tables to one Excel workbook (5 sheets).

Default behavior:
- 导出 status 为 done 或 error 的数据（五张表均按此筛选）
- Batched reads for large tables
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from openpyxl import Workbook

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import get_connection
from shared.db_backend import get_backend

# (table_name, order_column, where_clause) — 导出 status 为 done 或 error 的数据
CORE_TABLES: list[tuple[str, str, str]] = [
    ("qa_query", "id", "status IN ('done', 'error')"),
    ("qa_answer", "id", "status IN ('done', 'error')"),
    ("qa_link", "id", "status IN ('done', 'error')"),
    ("qa_link_content", "id", "status IN ('done', 'error')"),
    ("qa_link_video", "id", "status IN ('done', 'error')"),
]


def _meta_column_name(row: dict[str, Any]) -> str:
    """information_schema 在 PG/MySQL 下字段名大小写可能不同。"""
    return str(
        row.get("column_name")
        or row.get("COLUMN_NAME")
        or row.get("Column_name")
        or ""
    )


def normalize_value(value: Any) -> Any:
    """Convert DB values into Excel-friendly values."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Decimal):
        iv = int(value)
        return iv if iv == value else float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def export_table(
    conn,
    wb: Workbook,
    table_name: str,
    order_column: str,
    batch_size: int,
    *,
    where_clause: str | None = None,
) -> int:
    """Stream one table into one worksheet."""
    ws = wb.create_sheet(title=table_name[:31])
    backend = get_backend()

    meta_cur = backend.get_dict_cursor(conn)
    try:
        if backend.dialect() == "postgresql":
            schema_filter = "table_schema = 'public' AND "
        elif backend.dialect() == "mysql":
            schema_filter = "table_schema = DATABASE() AND "
        else:
            schema_filter = ""
        meta_cur.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE {schema_filter}table_name = %s ORDER BY ordinal_position",
            (table_name,),
        )
        columns = [c for c in (_meta_column_name(r) for r in meta_cur.fetchall()) if c]
    finally:
        meta_cur.close()

    if not columns:
        ws.append(["warning", f"table {table_name} not found"])
        return 0

    ws.append(columns)

    total = 0
    # 使用 id 分批避免 ORDER BY 全表导致 sort buffer 不足
    if order_column == "id" and "id" in columns:
        last_id = 0
        cur = backend.get_dict_cursor(conn)
        try:
            while True:
                if where_clause:
                    sql = f"SELECT * FROM {table_name} WHERE ({where_clause}) AND {order_column} > %s ORDER BY {order_column} LIMIT {batch_size}"
                else:
                    sql = f"SELECT * FROM {table_name} WHERE {order_column} > %s ORDER BY {order_column} LIMIT {batch_size}"
                cur.execute(sql, (last_id,))
                rows = cur.fetchall()
                if not rows:
                    break
                rows = backend.adapt_rows([dict(r) if not isinstance(r, dict) else r for r in rows])
                for row in rows:
                    ws.append([normalize_value(row.get(col)) for col in columns])
                total += len(rows)
                last_id = rows[-1].get("id") or rows[-1].get(order_column)
                print(f"[{table_name}] exported {total} rows...")
        finally:
            cur.close()
    else:
        sql = f"SELECT * FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += f" ORDER BY {order_column}"
        ss_cur = backend.get_server_side_cursor(conn, f"export_{table_name}", itersize=batch_size)
        try:
            ss_cur.execute(sql)
            while True:
                rows = ss_cur.fetchmany(batch_size)
                if not rows:
                    break
                rows = backend.adapt_rows([dict(r) if not isinstance(r, dict) else r for r in rows])
                for row in rows:
                    ws.append([normalize_value(row.get(col)) for col in columns])
                total += len(rows)
                print(f"[{table_name}] exported {total} rows...")
        finally:
            ss_cur.close()
    return total


def build_output_path(output: str | None) -> Path:
    if output:
        return Path(output).resolve()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(__file__).resolve().parent.parent / "export"
    out_dir.mkdir(parents=True, exist_ok=True)
    return (out_dir / f"qa_core_tables_{ts}.xlsx").resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export qa_query/qa_answer/qa_link/qa_link_content/qa_link_video to one XLSX."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="fetchmany batch size for large table export (default: 2000)",
    )
    parser.add_argument(
        "--output",
        help="output xlsx path (default: export/qa_core_tables_YYYYmmdd_HHMMSS.xlsx)",
    )
    args = parser.parse_args()

    output_path = build_output_path(args.output)
    wb = Workbook(write_only=True)
    # Remove default sheet by creating workbook with write_only mode and not using active.

    conn = get_connection()
    try:
        summary: dict[str, int] = {}
        for table_name, order_column, where_clause in CORE_TABLES:
            count = export_table(
                conn, wb, table_name, order_column, args.batch_size,
                where_clause=where_clause,
            )
            summary[table_name] = count
        wb.save(str(output_path))
    finally:
        conn.close()

    print(f"\nExport completed: {output_path}")
    for table_name, count in summary.items():
        print(f"- {table_name}: {count} rows")


if __name__ == "__main__":
    main()
