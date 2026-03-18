#!/usr/bin/env python3
"""CLI entry point for the QA data collection pipeline.

请使用项目 venv 运行，例如：./venv/bin/python integration/run.py run

Usage:
    python integration/run.py run            [--batch-size 10]   # 全流程，默认网页采集+模拟登陆，可拿到参考资料链接
    python integration/run.py collect        [--batch-size 10]   # 仅采集，默认网页+模拟登陆
    python integration/run.py run --api      [--batch-size 10]   # 使用 API 采集（无参考资料链接）
    python integration/run.py web-login [--manual]              # 首次或过期时登录；登录状态保存后可重复用
    python integration/run.py web-collect   [--batch-size 5]    # 直接网页采集（含深度思考/参考资料链接）
    python integration/run.py crawl         [--batch-size 50]
    python integration/run.py enrich-douyin                     # 从 douyin_videos/comments 补全抖音数据
    python integration/run.py audio-transcribe                 # 抖音视频下载+音频转写（Step 2.6）
    python integration/run.py structure / status / retry
    python integration/run.py regenerate-content           # 根据链接重新生成 qa_link_content（按文档规范）
    python integration/run.py export                            # 导出报告+完整数据到 export/（JSON + MD）
    python integration/run.py export-excel                      # 导出5张QA表到 XLSX
    python integration/run.py web-debug                         # 导出页面 HTML + 截图
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "web-crawler"))
sys.path.insert(0, str(_PROJECT_ROOT / "data-clean"))


def _setup_run_sync_logging(log_path: Path):
    """为 run-sync 添加文件日志，同时保留控制台输出。"""
    root = logging.getLogger()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root.addHandler(fh)


def _setup_error_log():
    """添加 ERROR 及以上级别到 output/error.log，便于排查。"""
    out_dir = _PROJECT_ROOT / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    err_path = out_dir / "error.log"
    root = logging.getLogger()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    eh = logging.FileHandler(err_path, encoding="utf-8", mode="a")
    eh.setLevel(logging.ERROR)
    eh.setFormatter(fmt)
    root.addHandler(eh)


def _probe_tcp(host: str, port: int, timeout_sec: float = 0.8) -> bool:
    """Fast TCP probe for dependency readiness."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def _check_postgres_or_hint() -> bool:
    """Check PostgreSQL early, print actionable hints on failure."""
    from shared.db import get_connection

    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception as exc:  # pragma: no cover - runtime guard
        print("[依赖检查] PostgreSQL 未就绪，无法继续执行。")
        print(f"[依赖检查] 错误: {exc}")
        print("[依赖检查] 先启动数据库后重试：")
        print("  - macOS: brew services start postgresql@17  (或 postgresql)")
        print("  - Linux: sudo systemctl start postgresql")
        print("[依赖检查] 同时请确认 .env 里的 PGHOST/PGPORT/PGUSER/PGPASSWORD。")
        return False


def _check_douyin_api_or_hint(*, required: bool) -> bool:
    """
    Check Douyin download API connectivity.

    required=True: fail-fast (audio-transcribe)
    required=False: warn only (run/crawl/run-sync)
    """
    api_url = (os.getenv("DOUYIN_DOWNLOAD_API_URL") or "http://localhost:8080").strip()
    host = "localhost"
    port = 8080
    try:
        from urllib.parse import urlparse

        parsed = urlparse(api_url)
        host = parsed.hostname or host
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except Exception:
        pass

    if _probe_tcp(host, int(port)):
        return True

    print(f"[依赖检查] 抖音下载 API 不可达: {api_url}")
    print("[依赖检查] 启动后可重试：")
    print("  cd Douyin_TikTok_Download_API && python start.py")
    if required:
        return False
    print("[依赖检查] 当前命令继续执行，但抖音链接可能出现 error/skip。")
    return True


def _preflight_dependencies(command: str) -> bool:
    """Dependency checks before expensive pipeline bootstrap."""
    db_required = {
        "run", "collect", "crawl", "enrich-douyin", "audio-transcribe", "structure",
        "regenerate-content", "status", "retry", "recollect", "recollect-web-only",
        "run-until", "run-sync", "export", "export-excel", "web-collect",
    }
    douyin_api_required = {"audio-transcribe", "run-sync"}
    douyin_api_optional = {"run", "crawl"}

    if command in db_required and not _check_postgres_or_hint():
        return False
    if command in douyin_api_required and not _check_douyin_api_or_hint(required=True):
        return False
    if command in douyin_api_optional:
        _check_douyin_api_or_hint(required=False)
    return True


def cmd_run(args):
    from integration.pipeline import QAPipeline
    use_web = not getattr(args, "api", False)
    pipeline = QAPipeline(use_web=use_web, web_headless=not getattr(args, "headed", False))
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    pipeline.run(
        batch_size=args.batch_size,
        query_ids=qids or None,
        query_limit=args.limit,
        category_prefix=args.category_prefix,
        crawl_concurrency=args.crawl_concurrency,
    )


def cmd_collect(args):
    from integration.pipeline import QAPipeline
    use_web = not getattr(args, "api", False)
    pipeline = QAPipeline(use_web=use_web, web_headless=not getattr(args, "headed", False))
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    processed = pipeline.step_collect(
        batch_size=args.batch_size,
        query_ids=qids or None,
        query_limit=args.limit,
        category_prefix=args.category_prefix,
    )
    print(f"Collected {len(processed)} answers: {processed}")


def cmd_crawl(args):
    from integration.pipeline import QAPipeline
    pipeline = QAPipeline()
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    crawled = asyncio.run(
        pipeline.step_crawl(
            batch_size=args.batch_size,
            query_ids=qids or None,
            concurrency=args.crawl_concurrency,
        )
    )
    print(f"Crawled {len(crawled)} links: {crawled}")


def cmd_enrich_douyin(args):
    from integration.pipeline import QAPipeline
    pipeline = QAPipeline()
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    count = pipeline.step_enrich_douyin(query_ids=qids or None)
    print(f"Enriched {count} Douyin link(s) from douyin_videos/douyin_comments")


def cmd_audio_transcribe(args):
    from integration.pipeline import QAPipeline

    pipeline = QAPipeline()
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    count = pipeline.step_audio_transcribe(
        query_ids=qids or None,
        concurrency=max(1, int(args.audio_concurrency)),
        batch_size=max(1, int(args.audio_batch_size)),
    )
    print(f"Audio transcribed {count} Douyin link(s)")


def cmd_structure(args):
    from integration.pipeline import QAPipeline
    pipeline = QAPipeline()
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    lids = [x.strip() for x in (getattr(args, "link_ids", "") or "").split(",") if x.strip()]
    concurrency = getattr(args, "structure_concurrency", 5)
    count = pipeline.step_structure(query_ids=qids or None, link_ids=lids or None, concurrency=concurrency)
    print(f"Structured {count} items")


def cmd_regenerate_content(args):
    from integration.pipeline import QAPipeline
    pipeline = QAPipeline()
    link_ids = [x.strip() for x in (args.link_ids or "").split(",") if x.strip()]
    count = pipeline.step_regenerate_content(
        link_ids=link_ids or None,
        include_all=args.all,
        force=args.force,
    )
    print(f"Regenerated {count} content rows (按 docs/大规模qa数据获取.md 规范)")


def cmd_status(args):
    from integration.pipeline import QAPipeline
    stats = QAPipeline.status()
    print(json.dumps(stats, ensure_ascii=False, indent=2))


def cmd_retry(args):
    from integration.pipeline import QAPipeline
    result = QAPipeline.retry_failed(all_errors=args.all)
    print(f"Reset: {result}")


def cmd_recollect(args):
    from shared.db import execute
    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    if not qids:
        print("No query_ids provided.")
        return
    for qid in qids:
        execute(
            "DELETE FROM qa_link_content WHERE link_id IN (SELECT link_id FROM qa_link WHERE query_id = %s)",
            (qid,),
        )
        execute("DELETE FROM qa_link WHERE query_id = %s", (qid,))
        execute("DELETE FROM qa_answer WHERE query_id = %s", (qid,))
        execute(
            "UPDATE qa_query SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE query_id = %s",
            (qid,),
        )
    print(f"Recollect reset done for {len(qids)} queries: {qids}")


def cmd_recollect_web_only(args):
    """Reset then recollect answers strictly via Web collector (no API fallback)."""
    from shared.db import execute
    from integration.pipeline import QAPipeline

    qids = [x.strip() for x in (args.query_ids or "").split(",") if x.strip()]
    if not qids:
        qids = [
            "Q0011", "Q0012", "Q0013", "Q0014", "Q0015", "Q0016",
            "Q0305", "Q0306", "Q0307", "Q0308", "Q0309",
        ]
    for qid in qids:
        execute(
            "DELETE FROM qa_link_content WHERE link_id IN (SELECT link_id FROM qa_link WHERE query_id = %s)",
            (qid,),
        )
        execute("DELETE FROM qa_link WHERE query_id = %s", (qid,))
        execute("DELETE FROM qa_answer WHERE query_id = %s", (qid,))
        execute(
            "UPDATE qa_query SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE query_id = %s",
            (qid,),
        )

    pipeline = QAPipeline(use_web=True, web_headless=not getattr(args, "headed", False))
    processed = pipeline.step_collect(batch_size=len(qids), query_ids=qids)
    print(f"Web-only recollect reset done for {len(qids)} queries: {qids}")
    print(f"Web-only collected {len(processed)} answers: {processed}")


def cmd_run_until(args):
    from integration.pipeline import QAPipeline
    from shared.db import fetch_one
    use_web = not getattr(args, "api", False)
    pipeline = QAPipeline(use_web=use_web, web_headless=not getattr(args, "headed", False))
    target = int(args.target_done_with_links)
    while True:
        row = fetch_one(
            "SELECT count(*) AS cnt FROM qa_query q "
            "WHERE q.status = 'done' "
            "AND EXISTS (SELECT 1 FROM qa_link l WHERE l.query_id = q.query_id)",
        ) or {"cnt": 0}
        done_with_links = int(row.get("cnt") or 0)
        print(f"[monitor] done_with_links={done_with_links}/{target}")
        if done_with_links >= target:
            print("Target reached.")
            break

        picked = pipeline.select_query_ids(
            limit=args.batch_size,
            category_prefix=args.category_prefix,
        )
        if not picked:
            print("No more pending queries matching filter.")
            break
        print(f"[monitor] next batch query_ids={picked}")
        collected = pipeline.step_collect(batch_size=len(picked), query_ids=picked)
        print(f"[monitor] collected={len(collected)}")
        crawled = asyncio.run(
            pipeline.step_crawl(
                batch_size=max(len(picked) * 5, 20),
                query_ids=picked,
                concurrency=args.crawl_concurrency,
            )
        )
        print(f"[monitor] crawled={len(crawled)}")
        enriched = pipeline.step_enrich_douyin(query_ids=picked)
        print(f"[monitor] enriched={enriched}")
        time.sleep(max(1, int(args.poll_seconds)))


def _select_query_ids_in_range(
    *,
    start_query_id: str,
    end_query_id: str,
    limit: int | None = None,
    status: str | None = None,
) -> list[str]:
    """Select query_ids in [start_query_id, end_query_id] by qa_query.id order."""
    from shared.db import fetch_all

    sql = (
        "SELECT query_id FROM qa_query "
        "WHERE query_id >= %s AND query_id <= %s "
    )
    params: list[object] = [start_query_id, end_query_id]
    if status:
        sql += "AND status = %s "
        params.append(status)
    sql += "ORDER BY id "
    if limit is not None:
        sql += "LIMIT %s"
        params.append(int(limit))
    rows = fetch_all(sql, tuple(params))
    return [r["query_id"] for r in rows]


def _range_status_snapshot(start_query_id: str, end_query_id: str) -> dict:
    """Return status counters for the selected query_id range."""
    from shared.db import fetch_one

    q = fetch_one(
        "SELECT "
        "COUNT(*) FILTER (WHERE status = 'pending') AS pending, "
        "COUNT(*) FILTER (WHERE status = 'processing') AS processing, "
        "COUNT(*) FILTER (WHERE status = 'done') AS done, "
        "COUNT(*) FILTER (WHERE status = 'error') AS error "
        "FROM qa_query WHERE query_id >= %s AND query_id <= %s",
        (start_query_id, end_query_id),
    ) or {}

    l = fetch_one(
        "SELECT "
        "COUNT(*) FILTER (WHERE l.status = 'pending') AS pending, "
        "COUNT(*) FILTER (WHERE l.status = 'processing') AS processing, "
        "COUNT(*) FILTER (WHERE l.status = 'done') AS done, "
        "COUNT(*) FILTER (WHERE l.status = 'error') AS error "
        "FROM qa_link l "
        "WHERE EXISTS ("
        "  SELECT 1 FROM qa_query q "
        "  WHERE q.query_id = l.query_id AND q.query_id >= %s AND q.query_id <= %s"
        ")",
        (start_query_id, end_query_id),
    ) or {}

    v = fetch_one(
        "SELECT "
        "COUNT(*) FILTER (WHERE v.status = 'pending') AS pending, "
        "COUNT(*) FILTER (WHERE v.status = 'processing') AS processing, "
        "COUNT(*) FILTER (WHERE v.status IN ('done','skip')) AS done, "
        "COUNT(*) FILTER (WHERE v.status = 'error') AS error "
        "FROM qa_link_video v "
        "JOIN qa_link lnk ON lnk.link_id = v.link_id "
        "WHERE EXISTS ("
        "  SELECT 1 FROM qa_query q "
        "  WHERE q.query_id = lnk.query_id AND q.query_id >= %s AND q.query_id <= %s"
        ")",
        (start_query_id, end_query_id),
    ) or {}

    return {
        "queries": {k: int(q.get(k) or 0) for k in ("pending", "processing", "done", "error")},
        "links": {k: int(l.get(k) or 0) for k in ("pending", "processing", "done", "error")},
        "video": {k: int(v.get(k) or 0) for k in ("pending", "processing", "done", "error")},
    }


def _resolve_run_sync_range(args) -> tuple[str, str]:
    """解析 run-sync 的范围：--start/--end 或 --limit 二选一。"""
    start = getattr(args, "start_query_id", None)
    end = getattr(args, "end_query_id", None)
    limit = getattr(args, "limit", None)

    if start and end:
        return start, end
    if (start and not end) or (end and not start):
        raise SystemExit("--start-query-id 和 --end-query-id 需同时指定")
    if limit and limit > 0:
        from shared.db import fetch_all
        rows = fetch_all(
            "SELECT query_id FROM qa_query WHERE status = 'pending' ORDER BY id LIMIT %s",
            (int(limit),),
        )
        if not rows:
            raise SystemExit("No pending queries found (--limit=%d)" % limit)
        ids = [r["query_id"] for r in rows]
        return min(ids), max(ids)
    raise SystemExit("Need --start-query-id + --end-query-id, or --limit N")


def cmd_run_sync(args):
    """Run long-lived concurrent workers for one query_id range."""
    from integration.pipeline import QAPipeline

    use_web = not getattr(args, "api", False)
    pipeline = QAPipeline(use_web=use_web, web_headless=not getattr(args, "headed", False))
    start_id, end_id = _resolve_run_sync_range(args)
    poll = max(1, int(args.poll_seconds))

    # 日志输出到 output/ 目录
    log_file = getattr(args, "log_file", None)  # --log-file
    if not log_file:
        out_dir = _PROJECT_ROOT / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = out_dir / f"run_sync_{start_id}_{end_id}_{ts}.log"
    else:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

    _setup_run_sync_logging(log_file)
    logger = logging.getLogger(__name__)
    logger.info("run-sync started: %s ~ %s, log=%s", start_id, end_id, log_file)

    async def _collect_worker():
        if use_web:
            # Web 采集：浏览器在 worker 生命周期内保持打开，崩溃时由 pipeline 内部重建
            try:
                while True:
                    qids = _select_query_ids_in_range(
                        start_query_id=start_id,
                        end_query_id=end_id,
                        limit=args.collect_batch_size,
                        status="pending",
                    )
                    if not qids:
                        await asyncio.sleep(poll)
                        continue
                    processed = await pipeline.collect_queries_persistent(qids)
                    if processed:
                        logger.info("[sync][collect] processed=%d ids=%s", len(processed), processed)
                    await asyncio.sleep(1)
            finally:
                try:
                    await pipeline.collector.stop()
                except Exception:
                    pass
        else:
            # API 采集：无状态，每轮独立调用即可
            while True:
                qids = _select_query_ids_in_range(
                    start_query_id=start_id,
                    end_query_id=end_id,
                    limit=args.collect_batch_size,
                    status="pending",
                )
                if not qids:
                    await asyncio.sleep(poll)
                    continue
                processed = await asyncio.to_thread(
                    pipeline.step_collect,
                    len(qids),
                    qids,
                )
                if processed:
                    logger.info("[sync][collect] processed=%d ids=%s", len(processed), processed)
                await asyncio.sleep(1)

    async def _crawl_worker():
        idle_sleep = 1
        while True:
            qids = _select_query_ids_in_range(
                start_query_id=start_id,
                end_query_id=end_id,
                limit=args.crawl_query_window,
            )
            if not qids:
                await asyncio.sleep(poll)
                continue
            crawled = await pipeline.step_crawl(
                batch_size=args.crawl_batch_size,
                query_ids=qids,
                concurrency=args.crawl_concurrency,
            )
            if crawled:
                logger.info("[sync][crawl] crawled=%d", len(crawled))
                idle_sleep = 1
            else:
                idle_sleep = min(idle_sleep * 2, 30)
            await asyncio.sleep(idle_sleep)

    async def _enrich_worker():
        idle_sleep = 1
        while True:
            qids = _select_query_ids_in_range(
                start_query_id=start_id,
                end_query_id=end_id,
                limit=args.enrich_query_window,
            )
            if not qids:
                await asyncio.sleep(poll)
                continue
            enriched = await asyncio.to_thread(pipeline.step_enrich_douyin, query_ids=qids)
            if enriched:
                logger.info("[sync][enrich] enriched=%s", enriched)
                idle_sleep = 1
            else:
                idle_sleep = min(idle_sleep * 2, 30)
            await asyncio.sleep(idle_sleep)

    async def _structure_worker():
        idle_sleep = 1
        while True:
            qids = _select_query_ids_in_range(
                start_query_id=start_id,
                end_query_id=end_id,
                limit=args.structure_query_window,
            )
            if not qids:
                await asyncio.sleep(poll)
                continue
            concurrency = getattr(args, "structure_concurrency", 5)
            structured = await asyncio.to_thread(
                pipeline.step_structure, query_ids=qids, concurrency=concurrency
            )
            if structured:
                logger.info("[sync][structure] structured=%s", structured)
                idle_sleep = 1
            else:
                idle_sleep = min(idle_sleep * 2, 30)
            await asyncio.sleep(idle_sleep)

    async def _audio_worker():
        idle_sleep = 1
        while True:
            qids = _select_query_ids_in_range(
                start_query_id=start_id,
                end_query_id=end_id,
                limit=args.audio_query_window,
            )
            if not qids:
                await asyncio.sleep(poll)
                continue
            transcribed = await asyncio.to_thread(
                pipeline.step_audio_transcribe,
                query_ids=qids,
                concurrency=args.audio_concurrency,
                batch_size=args.audio_batch_size,
            )
            if transcribed:
                logger.info("[sync][audio] transcribed=%s", transcribed)
                idle_sleep = 1
            else:
                idle_sleep = min(idle_sleep * 2, 30)
            await asyncio.sleep(idle_sleep)

    async def _monitor_and_stop(tasks: list[asyncio.Task]):
        idle_rounds = 0
        _prev_status = None
        _same_count = 0
        while True:
            snap = _range_status_snapshot(start_id, end_id)
            q = snap["queries"]
            l = snap["links"]
            vd = snap.get("video", {"pending": 0, "processing": 0, "done": 0, "error": 0})

            cur_status = (
                q["pending"], q["processing"], q["done"], q["error"],
                l["pending"], l["processing"], l["done"], l["error"],
                vd["pending"], vd["processing"], vd["done"], vd["error"],
            )
            if cur_status == _prev_status:
                _same_count += 1
            else:
                _same_count = 0
            _prev_status = cur_status

            if _same_count == 0 or _same_count % max(1, 30 // poll) == 0:
                logger.info(
                    "[sync][status] queries(p=%d,proc=%d,done=%d,err=%d) "
                    "links(p=%d,proc=%d,done=%d,err=%d) "
                    "video(p=%d,proc=%d,done=%d,err=%d)",
                    *cur_status,
                )

            done_now = (
                q["pending"] == 0
                and q["processing"] == 0
                and l["pending"] == 0
                and l["processing"] == 0
                and vd["pending"] == 0
                and vd["processing"] == 0
            )
            if done_now:
                idle_rounds += 1
            else:
                idle_rounds = 0

            if idle_rounds >= 2:
                logger.info("[sync] range drained, stopping workers.")
                for t in tasks:
                    t.cancel()
                return
            await asyncio.sleep(poll)

    async def _pre_check_login():
        """启动 worker 前验证登录，失败则提前退出。"""
        if not use_web:
            return
        await pipeline.collector.start()
        try:
            if not await pipeline.collector.ensure_logged_in():
                logger.error(
                    "登录检查失败。请先执行: python integration/run.py web-login --manual"
                )
                raise SystemExit(1)
            logger.info("登录预检查通过")
        finally:
            await pipeline.collector.stop()

    async def _run():
        await _pre_check_login()
        tasks = [
            asyncio.create_task(_collect_worker(), name="collect_worker"),
            asyncio.create_task(_crawl_worker(), name="crawl_worker"),
            asyncio.create_task(_enrich_worker(), name="enrich_worker"),
            asyncio.create_task(_audio_worker(), name="audio_worker"),
            asyncio.create_task(_structure_worker(), name="structure_worker"),
        ]
        monitor_task = asyncio.create_task(_monitor_and_stop(tasks), name="monitor")
        try:
            await monitor_task
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(_run())


def cmd_export(args):
    from integration.export_qa import export_all
    report_path, json_path, full_md_path = export_all()
    print(f"报告:   {report_path}")
    print(f"JSON:   {json_path}")
    print(f"完整MD: {full_md_path}")


def cmd_export_excel(args):
    from integration.export_db_excel import build_output_path, export_table, CORE_TABLES
    from shared.db import get_connection
    from openpyxl import Workbook

    output_path = build_output_path(args.output)
    wb = Workbook(write_only=True)
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

    print(f"Excel:  {output_path}")
    for table_name, count in summary.items():
        print(f"- {table_name}: {count} rows")


def cmd_web_login(args):
    from integration.doubao_web_collector import DoubaoWebCollector
    async def _run():
        c = DoubaoWebCollector(headless=False)
        await c.start()
        try:
            if args.manual:
                await c.manual_login()
            else:
                ok = await c.auto_login()
                if not ok:
                    print("Auto-login failed. Try: web-login --manual")
        finally:
            await c.stop()
    asyncio.run(_run())


def cmd_web_collect(args):
    from integration.doubao_web_collector import DoubaoWebCollector
    async def _run():
        c = DoubaoWebCollector(headless=not args.headed)
        await c.start()
        try:
            if args.query_id:
                from shared.db import fetch_one
                row = fetch_one(
                    "SELECT query_id, query_text FROM qa_query WHERE query_id = %s",
                    (args.query_id,),
                )
                if not row:
                    print(f"Query {args.query_id} not found.")
                    return
                result = await c.collect_one(row["query_id"], row["query_text"])
                print(f"Deep-thinking links: {len(result['deep_thinking_links'])}")
                print(f"Total links: {len(result['all_links'])}")
            else:
                done = await c.batch_collect(args.batch_size)
                print(f"Collected {len(done)} queries: {done}")
        finally:
            await c.stop()
    asyncio.run(_run())


def cmd_web_test(args):
    from integration.doubao_web_collector import DoubaoWebCollector
    async def _run():
        c = DoubaoWebCollector(headless=not args.headed)
        await c.start()
        try:
            result = await c.test_one_query(
                args.query_text or "低糖水果坚果麦片推荐"
            )
        finally:
            await c.stop()
    asyncio.run(_run())


def cmd_web_debug(args):
    from integration.doubao_web_collector import DoubaoWebCollector
    async def _run():
        c = DoubaoWebCollector(headless=False)
        await c.start()
        try:
            await c.dump_page()
        finally:
            await c.stop()
    asyncio.run(_run())


def main():
    parser = argparse.ArgumentParser(description="QA Data Collection Pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Full pipeline; default=网页采集后台静默模式")
    p_run.add_argument("--batch-size", type=int, default=10)
    p_run.add_argument("--api", action="store_true", help="改用 API 采集（无参考资料链接）")
    p_run.add_argument("--query-ids", help="逗号分隔 query_id，仅跑这些 query 的全流程")
    p_run.add_argument("--limit", type=int, help="未传 query-ids 时，按顺序挑选未跑过 query 数量")
    p_run.add_argument("--category-prefix", help="category 前缀过滤，如 3C数码")
    p_run.add_argument("--crawl-concurrency", type=int, default=3, help="Step2 爬取并发数（保守默认3）")
    p_run.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_run.set_defaults(func=cmd_run)

    p_collect = sub.add_parser("collect", help="仅采集回答；默认=网页+模拟登陆，含参考资料链接")
    p_collect.add_argument("--batch-size", type=int, default=10)
    p_collect.add_argument("--api", action="store_true", help="改用 API 采集（无参考资料链接）")
    p_collect.add_argument("--query-ids", help="逗号分隔 query_id，仅采集这些 query")
    p_collect.add_argument("--limit", type=int, help="未传 query-ids 时，按顺序挑选未跑过 query 数量")
    p_collect.add_argument("--category-prefix", help="category 前缀过滤，如 3C数码")
    p_collect.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_collect.set_defaults(func=cmd_collect)

    p_crawl = sub.add_parser("crawl", help="Crawl pending links only")
    p_crawl.add_argument("--batch-size", type=int, default=50)
    p_crawl.add_argument("--query-ids", help="逗号分隔 query_id，仅抓这些 query 的链接")
    p_crawl.add_argument("--crawl-concurrency", type=int, default=3, help="爬取并发数（默认3）")
    p_crawl.set_defaults(func=cmd_crawl)

    p_enrich = sub.add_parser("enrich-douyin", help="从 douyin_videos/comments 补全抖音链接数据")
    p_enrich.add_argument("--query-ids", help="逗号分隔 query_id，仅补全这些 query 的链接")
    p_enrich.set_defaults(func=cmd_enrich_douyin)

    p_audio = sub.add_parser("audio-transcribe", help="下载抖音视频并转写音频（Step 2.6）")
    p_audio.add_argument("--query-ids", help="逗号分隔 query_id，仅处理这些 query 的抖音链接")
    p_audio.add_argument("--audio-concurrency", type=int, default=2, help="音频下载+ASR 转写并发数（默认2，建议≤2 避免 ASR 并发配额）")
    p_audio.add_argument("--audio-batch-size", type=int, default=1000, help="单次队列消费 link 数（默认1000）")
    p_audio.set_defaults(func=cmd_audio_transcribe)

    p_struct = sub.add_parser("structure", help="Structure raw content only")
    p_struct.add_argument("--query-ids", help="逗号分隔 query_id，仅结构化这些 query 的链接")
    p_struct.add_argument("--link-ids", help="逗号分隔 link_id，仅结构化这些链接")
    p_struct.add_argument("--structure-concurrency", type=int, default=5, help="LLM 并发数（默认 5）")
    p_struct.set_defaults(func=cmd_structure)

    p_regen = sub.add_parser("regenerate-content", help="根据链接重新生成 qa_link_content（默认仅最近更新的 done 链接）")
    p_regen.add_argument("--link-ids", help="逗号分隔的 link_id 列表，仅重生这些链接")
    p_regen.add_argument("--all", action="store_true", help="重生全量 qa_link_content（谨慎使用）")
    p_regen.add_argument("--force", action="store_true", help="允许低质量覆盖（关闭防降级保护）")
    p_regen.set_defaults(func=cmd_regenerate_content)

    p_status = sub.add_parser("status", help="Show pipeline status")
    p_status.set_defaults(func=cmd_status)

    p_retry = sub.add_parser("retry", help="Reset failed items to pending")
    p_retry.add_argument(
        "--all",
        action="store_true",
        help="重置所有 error 链接；默认仅重置可重试错误",
    )
    p_retry.set_defaults(func=cmd_retry)

    p_recollect = sub.add_parser("recollect", help="重置指定 query 的 answer/link/content 后重新采集")
    p_recollect.add_argument("--query-ids", required=True, help="逗号分隔 query_id")
    p_recollect.set_defaults(func=cmd_recollect)

    p_recollect_web = sub.add_parser(
        "recollect-web-only",
        help="重置后仅用 Web 采集 answer（禁止 API 兜底）",
    )
    p_recollect_web.add_argument(
        "--query-ids",
        help="逗号分隔 query_id；不传则默认 Q0011~Q0016,Q0305~Q0309",
    )
    p_recollect_web.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_recollect_web.set_defaults(func=cmd_recollect_web_only)

    p_run_until = sub.add_parser("run-until", help="按过滤条件持续跑批，直到 done且有链接达到目标")
    p_run_until.add_argument("--target-done-with-links", type=int, default=50)
    p_run_until.add_argument("--batch-size", type=int, default=3)
    p_run_until.add_argument("--category-prefix", help="category 前缀过滤，如 3C数码")
    p_run_until.add_argument("--crawl-concurrency", type=int, default=3, help="Step2 并发数")
    p_run_until.add_argument("--poll-seconds", type=int, default=2)
    p_run_until.add_argument("--api", action="store_true", help="改用 API 采集（无参考资料链接）")
    p_run_until.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_run_until.set_defaults(func=cmd_run_until)

    p_run_sync = sub.add_parser(
        "run-sync",
        help="按 query_id 范围常驻并发跑批：collect/crawl/enrich/audio/structure 分环节持续消费",
    )
    p_run_sync.add_argument("--start-query-id", help="起始 query_id（与 --end 搭配）")
    p_run_sync.add_argument("--end-query-id", help="结束 query_id（与 --start 搭配）")
    p_run_sync.add_argument("--limit", type=int, help="不指定范围时，取前 N 条 pending query 跑批")
    p_run_sync.add_argument("--poll-seconds", type=int, default=2, help="各 worker 空转轮询间隔")
    p_run_sync.add_argument("--api", action="store_true", help="改用 API 采集（无参考资料链接）")
    p_run_sync.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_run_sync.add_argument("--collect-batch-size", type=int, default=1, help="collect 单批 query 数（默认1，串行）")
    p_run_sync.add_argument("--crawl-batch-size", type=int, default=50, help="crawl 单批 link 数")
    p_run_sync.add_argument("--crawl-concurrency", type=int, default=3, help="crawl 并发数")
    p_run_sync.add_argument("--crawl-query-window", type=int, default=100, help="crawl 每轮扫描 query 窗口")
    p_run_sync.add_argument("--enrich-query-window", type=int, default=100, help="enrich 每轮扫描 query 窗口")
    p_run_sync.add_argument("--audio-query-window", type=int, default=100, help="audio 每轮扫描 query 窗口")
    p_run_sync.add_argument("--audio-concurrency", type=int, default=2, help="audio 下载+ASR 转写并发数（默认2，建议≤2 避免 ASR 并发配额）")
    p_run_sync.add_argument("--audio-batch-size", type=int, default=5, help="audio 每轮队列消费 link 数（默认5）")
    p_run_sync.add_argument("--structure-query-window", type=int, default=100, help="structure 每轮扫描 query 窗口")
    p_run_sync.add_argument("--structure-concurrency", type=int, default=5, help="structure LLM 并发数（默认 5）")
    p_run_sync.add_argument(
        "--log-file",
        help="日志文件路径（默认 output/run_sync_{start}_{end}_{时间戳}.log）",
    )
    p_run_sync.set_defaults(func=cmd_run_sync)

    p_export = sub.add_parser("export", help="Export report + full data to export/ (JSON + MD)")
    p_export.set_defaults(func=cmd_export)

    p_export_excel = sub.add_parser(
        "export-excel",
        help="导出 qa_query/qa_answer/qa_link/qa_link_content/qa_link_video 到一个 XLSX（5个sheet）",
    )
    p_export_excel.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="大表分批导出大小（默认 2000）",
    )
    p_export_excel.add_argument(
        "--output",
        help="输出文件路径（默认 export/qa_core_tables_时间戳.xlsx）",
    )
    p_export_excel.set_defaults(func=cmd_export_excel)

    p_wlogin = sub.add_parser("web-login", help="Login to Doubao web UI (saves session)")
    p_wlogin.add_argument("--manual", action="store_true", help="Manual login in browser")
    p_wlogin.set_defaults(func=cmd_web_login)

    p_wcollect = sub.add_parser("web-collect", help="Collect via web UI (with deep-thinking links)")
    p_wcollect.add_argument("--query-id", help="Specific query_id")
    p_wcollect.add_argument("--batch-size", type=int, default=5)
    p_wcollect.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_wcollect.set_defaults(func=cmd_web_collect)

    p_wtest = sub.add_parser("web-test", help="Test one query (no DB write)")
    p_wtest.add_argument("--query-text", default="低糖水果坚果麦片推荐")
    p_wtest.add_argument("--headed", action="store_true", help="显示浏览器窗口（默认后台静默）")
    p_wtest.set_defaults(func=cmd_web_test)

    p_wdebug = sub.add_parser("web-debug", help="Dump Doubao page HTML + screenshot")
    p_wdebug.set_defaults(func=cmd_web_debug)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    _setup_error_log()

    if not _preflight_dependencies(args.command):
        raise SystemExit(2)

    args.func(args)


if __name__ == "__main__":
    main()
