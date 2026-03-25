#!/usr/bin/env python3
"""扫描 qa_link_content.content_json：不符合 JSON 规范或不符合项目约定时标记 error 并清空，便于 structure 重跑。

约定（默认）：
- 能完成 JSON 往返序列化（无不可序列化类型，如部分驱动返回的 Decimal 等）
- 根节点须为 **object (dict)**（与 docs 中 A/B/C/D 结构化结果一致）；可用 --allow-root-array 允许根为数组

PostgreSQL 的 JSONB 列本身不会存非法 JSON 语法；本工具主要捕获「根为标量 / 非 dict / 序列化失败」等逻辑问题。
MySQL 的 JSON 列若存在非法片段，也会在应用层被检出。

Usage:
    ./venv/bin/python integration/mark_invalid_content_json.py
    ./venv/bin/python integration/mark_invalid_content_json.py --apply
    ./venv/bin/python integration/run.py validate-content-json --apply
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute, fetch_all


def _dumps_kw() -> dict:
    kw: dict = {"ensure_ascii": False}
    if sys.version_info >= (3, 9):
        kw["allow_nan"] = False
    return kw


def validate_content_json_value(
    value,
    *,
    require_dict_root: bool = True,
    allow_root_array: bool = False,
) -> tuple[bool, str | None]:
    """若 (ok, None) 表示通过；(False, reason) 为不通过原因。"""
    if value is None:
        return True, None

    obj = value
    if isinstance(obj, (bytes, bytearray)):
        try:
            obj = obj.decode("utf-8")
        except Exception as exc:
            return False, f"bytes_decode:{exc}"

    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return False, "empty_string"
        try:
            obj = json.loads(s)
        except json.JSONDecodeError as exc:
            return False, f"json_decode:{exc}"

    if isinstance(obj, (str, int, bool)) or obj is None:
        return False, "root_scalar_not_object_or_array"
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return False, "root_nan_or_inf"

    if not isinstance(obj, (dict, list)):
        return False, f"root_type:{type(obj).__name__}"

    try:
        json.dumps(obj, **_dumps_kw())
    except (TypeError, ValueError) as exc:
        return False, f"not_json_serializable:{exc}"

    if isinstance(obj, dict):
        return True, None
    if isinstance(obj, list):
        if require_dict_root and not allow_root_array:
            return False, "root_is_array"
        return True, None
    return False, "root_not_dict"


def scan_invalid(
    *,
    require_dict_root: bool = True,
    allow_root_array: bool = False,
    batch_size: int = 500,
    link_ids: list[str] | None = None,
) -> list[dict]:
    """返回 [{'link_id', 'reason'}, ...]。"""
    invalid: list[dict] = []
    params: tuple | None = None
    where_extra = ""
    if link_ids:
        from shared.sql_builder import sb

        frag, plist = sb.expand_any("link_id", link_ids)
        where_extra = f" AND {frag}"
        params = tuple(plist)

    def _consume(rows: list) -> None:
        for r in rows:
            ok, reason = validate_content_json_value(
                r["content_json"],
                require_dict_root=require_dict_root,
                allow_root_array=allow_root_array,
            )
            if not ok:
                invalid.append({"link_id": r["link_id"], "reason": reason or "unknown"})

    if link_ids:
        sql = (
            "SELECT link_id, content_json FROM qa_link_content "
            "WHERE content_json IS NOT NULL "
            f"{where_extra} "
            "ORDER BY link_id"
        )
        _consume(fetch_all(sql, params))
        return invalid

    last_id: str | None = None
    while True:
        if last_id is None:
            rows = fetch_all(
                "SELECT link_id, content_json FROM qa_link_content "
                "WHERE content_json IS NOT NULL "
                "ORDER BY link_id "
                "LIMIT %s",
                (batch_size,),
            )
        else:
            rows = fetch_all(
                "SELECT link_id, content_json FROM qa_link_content "
                "WHERE content_json IS NOT NULL AND link_id > %s "
                "ORDER BY link_id "
                "LIMIT %s",
                (last_id, batch_size),
            )
        if not rows:
            break
        _consume(rows)
        last_id = rows[-1]["link_id"]
        if len(rows) < batch_size:
            break

    return invalid


def apply_mark_error(invalid: list[dict]) -> int:
    """清空 content_json，status=error，便于 step_structure 按 content_json IS NULL 重新结构化。"""
    if not invalid:
        return 0
    from shared.sql_builder import sb

    ids = [x["link_id"] for x in invalid]
    any_frag, any_params = sb.expand_any("link_id", ids)
    n = execute(
        f"UPDATE qa_link_content SET content_json = NULL, status = 'error', "
        f"updated_at = CURRENT_TIMESTAMP WHERE {any_frag}",
        any_params,
    )
    return int(n)


def run_mark(
    *,
    apply: bool,
    require_dict_root: bool = True,
    allow_root_array: bool = False,
    batch_size: int = 500,
    link_ids: list[str] | None = None,
) -> list[dict]:
    invalid = scan_invalid(
        require_dict_root=require_dict_root,
        allow_root_array=allow_root_array,
        batch_size=batch_size,
        link_ids=link_ids,
    )
    print(f"[validate content_json] 不符合规则: {len(invalid)} 条")
    for r in invalid[:30]:
        print(f"  {r['link_id']}  {r['reason']}")
    if len(invalid) > 30:
        print(f"  ... 另有 {len(invalid) - 30} 条")

    if apply and invalid:
        n = apply_mark_error(invalid)
        print(f"[validate content_json] 已更新 qa_link_content: {n} 行 (content_json=NULL, status=error)")
    elif not apply and invalid:
        print("[validate content_json] dry-run，加 --apply 写入")

    return invalid


def main():
    import argparse

    p = argparse.ArgumentParser(description="标记非法/不合规 content_json 并重跑 structure")
    p.add_argument("--apply", action="store_true", help="写入：清空 content_json 并 status=error")
    p.add_argument(
        "--allow-root-array",
        action="store_true",
        help="允许根节点为 JSON 数组（默认仅允许 object）",
    )
    p.add_argument(
        "--no-require-dict",
        action="store_true",
        help="不强制根为 dict（仍要求 object 或 array，见 --allow-root-array）",
    )
    p.add_argument("--batch-size", type=int, default=500, help="分页扫描行数")
    p.add_argument("--link-ids", nargs="+", metavar="ID", help="仅检查这些 link_id")
    args = p.parse_args()

    require_dict = not args.no_require_dict
    run_mark(
        apply=args.apply,
        require_dict_root=require_dict,
        allow_root_array=args.allow_root_array,
        batch_size=max(1, int(args.batch_size)),
        link_ids=list(args.link_ids) if args.link_ids else None,
    )
    if args.apply:
        print("\n下一步: ./venv/bin/python integration/run.py structure [--query-ids ...]")


if __name__ == "__main__":
    main()
