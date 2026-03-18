#!/usr/bin/env python3
"""一次性修复脚本：平台修正 + 空内容重置。

用完后手动删除此文件。

Usage:
    ./venv/bin/python integration/fix_empty_content.py          # dry-run (只打印)
    ./venv/bin/python integration/fix_empty_content.py --apply  # 实际执行
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute, fetch_all


def fix_csdn_platform(apply: bool) -> int:
    """Step 1: 将 platform='其他' 且 URL 含 csdn.net 的 link 更新为 platform='CSDN'。"""
    rows = fetch_all(
        "SELECT link_id, link_url FROM qa_link "
        "WHERE platform = '其他' AND link_url LIKE '%csdn.net%'"
    )
    print(f"\n[CSDN 平台修正] 找到 {len(rows)} 条 (platform='其他', url含csdn.net)")
    for r in rows:
        print(f"  {r['link_id']} | {r['link_url'][:80]}")

    if not apply or not rows:
        return len(rows)

    ids = [r["link_id"] for r in rows]
    cnt = execute(
        "UPDATE qa_link SET platform = 'CSDN' WHERE link_id = ANY(%s)",
        (ids,),
    )
    print(f"  => 已更新 {cnt} 条 platform -> 'CSDN'")

    # 清除 content_json 让 structure 重新生成（raw_json 保留）
    cnt2 = execute(
        "UPDATE qa_link_content SET content_json = NULL WHERE link_id = ANY(%s)",
        (ids,),
    )
    print(f"  => 已清除 {cnt2} 条 content_json (raw_json 保留，等待 re-structure)")
    return len(rows)


def fix_empty_toutiao_smzdm(apply: bool) -> int:
    """Step 2: 精确重置头条和什么值得买中空内容的 done 记录为 pending。"""
    rows = fetch_all(
        "SELECT l.link_id, l.platform, l.link_url "
        "FROM qa_link l "
        "JOIN qa_link_content lc ON l.link_id = lc.link_id "
        "WHERE l.status = 'done' "
        "AND l.platform IN ('头条', '什么值得买') "
        "AND (lc.raw_json->>'raw_text' IS NULL OR lc.raw_json->>'raw_text' = '') "
        "AND (lc.raw_json->'paragraphs' IS NULL "
        "     OR jsonb_array_length(COALESCE(lc.raw_json->'paragraphs', '[]'::jsonb)) = 0)"
    )
    by_plat: dict[str, list] = {}
    for r in rows:
        by_plat.setdefault(r["platform"], []).append(r)

    total = len(rows)
    print(f"\n[头条/什么值得买 空内容重置] 找到 {total} 条")
    for plat, items in sorted(by_plat.items()):
        print(f"  --- {plat}: {len(items)} 条 ---")
        for r in items[:5]:
            print(f"    {r['link_id']} | {r['link_url'][:70]}")
        if len(items) > 5:
            print(f"    ... 还有 {len(items) - 5} 条")

    if not apply or not rows:
        return total

    ids = [r["link_id"] for r in rows]
    cnt1 = execute(
        "UPDATE qa_link SET status = 'pending' WHERE link_id = ANY(%s)",
        (ids,),
    )
    print(f"  => 已重置 {cnt1} 条 qa_link.status -> 'pending'")

    cnt2 = execute(
        "UPDATE qa_link_content SET raw_json = NULL, content_json = NULL, status = 'pending' "
        "WHERE link_id = ANY(%s)",
        (ids,),
    )
    print(f"  => 已清除 {cnt2} 条 raw_json + content_json + status -> 'pending' (等待重爬)")
    return total


def main():
    apply = "--apply" in sys.argv
    mode = "APPLY" if apply else "DRY-RUN"
    print(f"=== fix_empty_content.py ({mode}) ===")

    n1 = fix_csdn_platform(apply)
    n2 = fix_empty_toutiao_smzdm(apply)

    print(f"\n=== 总结 ({mode}) ===")
    print(f"  CSDN 平台修正: {n1} 条")
    print(f"  头条/什么值得买 空内容重置: {n2} 条")
    if not apply:
        print("\n  (这是 dry-run，加 --apply 参数实际执行)")


if __name__ == "__main__":
    main()
