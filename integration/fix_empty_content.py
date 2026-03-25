#!/usr/bin/env python3
"""数据清洗：平台修正、Playwright 空壳/WAF 重跑、raw 里「通用-JS」标签修正。

Usage:
    ./venv/bin/python integration/fix_empty_content.py              # dry-run
    ./venv/bin/python integration/fix_empty_content.py --apply    # 执行
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute, fetch_all
from shared.sql_builder import sb


def fix_csdn_platform(apply: bool) -> int:
    """将 platform='其他' 且 URL 含 csdn.net 的 link 更新为 platform='CSDN'。"""
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
    any_frag, any_params = sb.expand_any("link_id", ids)
    cnt = execute(
        f"UPDATE qa_link SET platform = 'CSDN' WHERE {any_frag}",
        any_params,
    )
    print(f"  => 已更新 {cnt} 条 platform -> 'CSDN'")

    cnt2 = execute(
        f"UPDATE qa_link_content SET content_json = NULL WHERE {any_frag}",
        any_params,
    )
    print(f"  => 已清除 {cnt2} 条 content_json (raw_json 保留，等待 re-structure)")
    return len(rows)


def fix_playwright_shell_retries(
    apply: bool,
    *,
    limit: int | None = None,
    link_ids: list[str] | None = None,
) -> int:
    """头条 / 什么值得买：空壳、腾讯 WAF 错误、或「原始数据不足」结构化结果 → 重爬。

    覆盖：仅 done/error；重置为 pending 并清空 raw/content 以便整条链路重跑。
    *limit*: 仅处理前 N 条（分批重跑时可用）。
    """
    _raw_text = sb.json_extract_text("lc.raw_json", "raw_text")
    _raw_title = sb.json_extract_text("lc.raw_json", "title")
    _paragraphs = sb.json_extract("lc.raw_json", "paragraphs")
    _arr_len = sb.json_array_length(f"COALESCE({_paragraphs}, {sb.json_cast('[]')})")
    _raw_err = sb.json_extract_text("lc.raw_json", "error")
    _meta_note = sb.json_extract_path_text("lc.content_json", "元数据", "数据说明")

    empty_shell = (
        f"(COALESCE({_raw_text}, '') = '' AND COALESCE({_arr_len}, 0) = 0 "
        f"AND COALESCE({_raw_title}, '') = '')"
    )

    if link_ids:
        any_frag, any_params = sb.expand_any("l.link_id", link_ids)
        rows = fetch_all(
            "SELECT l.link_id, l.platform, l.link_url, l.status, "
            f"COALESCE({_raw_err}, '') AS raw_err "
            "FROM qa_link l "
            "JOIN qa_link_content lc ON l.link_id = lc.link_id "
            "WHERE l.platform IN ('头条', '什么值得买') "
            "AND l.status IN ('done', 'error') "
            f"AND ({any_frag})",
            any_params,
        )
    else:
        rows = fetch_all(
            "SELECT l.link_id, l.platform, l.link_url, l.status, "
            f"COALESCE({_raw_err}, '') AS raw_err "
            "FROM qa_link l "
            "JOIN qa_link_content lc ON l.link_id = lc.link_id "
            "WHERE l.platform IN ('头条', '什么值得买') "
            "AND l.status IN ('done', 'error') "
            "AND ( "
            f"  {empty_shell} "
            f"  OR COALESCE({_raw_err}, '') LIKE 'smzdm_blocked%%' "
            f"  OR COALESCE({_raw_err}, '') LIKE '%%tencent_waf%%' "
            f"  OR COALESCE({_meta_note}, '') LIKE '%%原始数据不足%%' "
            ")"
        )
    if limit is not None and limit > 0:
        rows = rows[:limit]

    by_plat: dict[str, list] = {}
    for r in rows:
        by_plat.setdefault(r["platform"], []).append(r)

    total = len(rows)
    print(f"\n[头条/什么值得买 空壳·WAF·数据不足 重跑] 找到 {total} 条")
    for plat, items in sorted(by_plat.items()):
        print(f"  --- {plat}: {len(items)} 条 ---")
        for r in items[:8]:
            err = (r.get("raw_err") or "")[:40]
            print(f"    {r['link_id']} | {r['status']} | {r['link_url'][:60]} | err={err}")
        if len(items) > 8:
            print(f"    ... 还有 {len(items) - 8} 条")

    if not apply or not rows:
        return total

    ids = [r["link_id"] for r in rows]
    any_frag, any_params = sb.expand_any("link_id", ids)
    cnt1 = execute(
        f"UPDATE qa_link SET status = 'pending', error_message = NULL WHERE {any_frag}",
        any_params,
    )
    print(f"  => 已重置 {cnt1} 条 qa_link.status -> 'pending'")

    cnt2 = execute(
        "UPDATE qa_link_content SET raw_json = NULL, content_json = NULL, status = 'pending' "
        f"WHERE {any_frag}",
        any_params,
    )
    print(f"  => 已清除 {cnt2} 条 raw_json + content_json (等待重爬)")
    return total


def patch_playwright_platform_labels(apply: bool) -> int:
    """将 raw_json.platform 从「通用-JS」改为 qa_link.platform，并清空 content_json 以便重算来源网站。"""
    rows = fetch_all(
        "SELECT l.link_id, l.platform, lc.raw_json "
        "FROM qa_link l "
        "JOIN qa_link_content lc ON l.link_id = lc.link_id "
        "WHERE l.platform IN ('头条', '什么值得买') "
        "AND lc.raw_json IS NOT NULL "
        "AND lc.status <> 'pending'"
    )
    to_fix: list[dict] = []
    for r in rows:
        raw = r["raw_json"]
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                continue
        if not isinstance(raw, dict) or raw.get("platform") != "通用-JS":
            continue
        to_fix.append({"link_id": r["link_id"], "platform": r["platform"], "raw": raw})

    print(f"\n[raw_json.platform 通用-JS → 业务平台] 找到 {len(to_fix)} 条")
    for r in to_fix[:10]:
        print(f"  {r['link_id']} -> {r['platform']}")
    if len(to_fix) > 10:
        print(f"  ... 还有 {len(to_fix) - 10} 条")

    if not apply or not to_fix:
        return len(to_fix)

    sql = (
        "UPDATE qa_link_content SET raw_json = %s::jsonb, content_json = NULL WHERE link_id = %s"
        if sb.is_pg
        else "UPDATE qa_link_content SET raw_json = %s, content_json = NULL WHERE link_id = %s"
    )
    for item in to_fix:
        raw = item["raw"]
        raw["platform"] = item["platform"]
        execute(sql, (json.dumps(raw, ensure_ascii=False), item["link_id"]))
    print(f"  => 已更新 {len(to_fix)} 条 raw_json.platform，并清空 content_json（请跑 structure）")
    return len(to_fix)


def main():
    import argparse

    p = argparse.ArgumentParser(description="清洗空壳 / 修正 Playwright 平台标签")
    p.add_argument("--apply", action="store_true", help="实际写入数据库")
    p.add_argument("--only-csdn", action="store_true", help="仅 CSDN 平台修正")
    p.add_argument("--only-shell", action="store_true", help="仅空壳/WAF/数据不足 重跑")
    p.add_argument("--only-patch", action="store_true", help="仅 raw_json.platform 通用-JS→业务名")
    p.add_argument("--shell-limit", type=int, default=0, metavar="N", help="重跑最多 N 条（0=不限制）")
    p.add_argument("--link-ids", nargs="+", metavar="ID", help="仅处理这些 link_id（须符合空壳 SQL 条件）")
    args = p.parse_args()
    apply = args.apply
    shell_limit = args.shell_limit if args.shell_limit > 0 else None
    mode = "APPLY" if apply else "DRY-RUN"
    print(f"=== fix_empty_content.py ({mode}) ===")

    run_all = not (args.only_csdn or args.only_shell or args.only_patch)

    n1 = n2 = n3 = 0
    if run_all or args.only_csdn:
        n1 = fix_csdn_platform(apply)
    # 先重置空壳/WAF（会清空 raw），再修正仍存留的「通用-JS」标签
    if run_all or args.only_shell:
        n2 = fix_playwright_shell_retries(
            apply,
            limit=shell_limit,
            link_ids=list(args.link_ids) if args.link_ids else None,
        )
    if run_all or args.only_patch:
        n3 = patch_playwright_platform_labels(apply)

    print(f"\n=== 总结 ({mode}) ===")
    if run_all or args.only_csdn:
        print(f"  CSDN 平台修正: {n1} 条")
    if run_all or args.only_shell:
        lim = f"（limit={shell_limit}）" if shell_limit else ""
        print(f"  头条/什么值得买 空壳·WAF·数据不足 重跑: {n2} 条{lim}")
    if run_all or args.only_patch:
        print(f"  raw_json.platform 通用-JS 修正: {n3} 条")
    if not apply:
        print("\n  (dry-run，加 --apply 执行；重跑后请: python integration/run.py crawl && structure)")
    else:
        print("\n  下一步: python integration/run.py crawl")
        print("         python integration/run.py structure")


if __name__ == "__main__":
    main()
