"""End-to-end QA data collection pipeline.

Orchestrates: query collection -> link crawling -> content structuring.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "web-crawler"))
sys.path.insert(0, str(_PROJECT_ROOT / "data-clean"))

from shared.db import execute, fetch_all, fetch_one
from shared.sql_builder import sb
from integration.parsing_routing import use_douyin_download_llm, use_agent_web_summary
from integration.douyin_data_merger import (
    DouyinDataMerger,
    _content_quality_score,
)

logger = logging.getLogger(__name__)

_HTTP_STATUS_RE = re.compile(r"\b(\d{3})\b")


def _is_retryable_link_error(raw_error: str) -> bool:
    """Heuristic: whether a link crawl error is worth retrying."""
    text = (raw_error or "").lower()
    if not text:
        return True

    # Deterministic TLS/domain failures.
    if "certificate_verify_failed" in text or "hostname mismatch" in text:
        return False
    # Explicitly unsupported / blocked pages usually not recoverable soon.
    if "method not allowed" in text:
        return False

    # HTTP status based decision: 4xx are mostly non-retriable except 408/429.
    # Error text may contain multiple numbers; pick the first 3-digit code.
    m = _HTTP_STATUS_RE.search(text)
    if m:
        status = int(m.group(1))
        if 400 <= status < 500 and status not in (408, 429):
            return False
    return True


def _raw_data_sufficient(raw: dict, platform: str) -> tuple[bool, str]:
    """Check if raw data has enough substance to justify an LLM call.

    Returns (sufficient, reason).
    """
    title = (raw.get("title") or "").strip()
    raw_text = (raw.get("raw_text") or "").strip()
    comments = raw.get("comments") or []
    subtitles = raw.get("subtitles") or []
    metadata = raw.get("metadata") or {}

    non_empty_comments = sum(
        1 for c in comments
        if isinstance(c, dict) and len((c.get("text") or "").strip()) > 2
    )
    has_subtitles = any(
        isinstance(s, dict) and len((s.get("text") or "").strip()) > 2
        for s in subtitles
    )
    has_author = bool((metadata.get("author") or "").strip())
    has_stats = bool(metadata.get("digg_count") or metadata.get("comment_count"))

    if not title and not raw_text and non_empty_comments == 0 and not has_subtitles:
        return False, "无标题、无正文、无评论、无字幕"

    total_chars = len(title) + len(raw_text)
    if total_chars < 10 and non_empty_comments == 0 and not has_subtitles:
        return False, f"文本仅{total_chars}字且无评论/字幕"

    return True, ""


def _post_process_by_platform(platform: str, raw: dict, structured: dict, link_id: str) -> dict:
    """按平台做后处理：抖音→LLM 解析文案和特征，通用→Agent 网页摘要。"""
    if not platform:
        return structured

    sufficient, reason = _raw_data_sufficient(raw, platform)
    if not sufficient:
        logger.info("[llm_guard] skip LLM for %s (%s): %s", link_id, platform, reason)
        structured.setdefault("元数据", {})["数据说明"] = f"原始数据不足（{reason}），已跳过 LLM 调用。"
        return structured

    try:
        if use_douyin_download_llm(platform):
            from llm_extractor import enrich_douyin_video_llm
            structured = enrich_douyin_video_llm(raw, structured, link_id)
        elif use_agent_web_summary(platform):
            from llm_extractor import summarise_text
            text = raw.get("raw_text") or raw.get("title", "")
            if text:
                summary = summarise_text(text, max_length=300)
                structured.setdefault("元数据", {})["网页摘要"] = summary
    except Exception as exc:
        logger.warning("Post-process for %s %s failed: %s", platform, link_id, exc)
    return structured


class QAPipeline:
    """Orchestrate the full QA data collection workflow.

    Default: use web collector (DoubaoWebCollector) so answers include
    real reference links from 深度思考/参考资料. Use use_web=False to fall
    back to API (DoubaoQueryCollector) which does not return retrieved URLs.
    """

    def __init__(self, use_web: bool = True, web_headless: bool = True):
        from crawler_manager import CrawlerManager
        from structurer import ContentStructurer

        self.use_web = use_web
        self.web_headless = web_headless
        if use_web:
            from integration.doubao_web_collector import DoubaoWebCollector
            self.collector = DoubaoWebCollector(headless=web_headless)
        else:
            from integration.doubao_query import DoubaoQueryCollector
            self.collector = DoubaoQueryCollector()
        self.crawler_mgr = CrawlerManager()
        self.structurer = ContentStructurer()
        self.douyin_merger = DouyinDataMerger()
        # Fail-fast guard for repeated CAPTCHA/human-verification failures.
        # In long-running sync jobs, continuing after many consecutive risk blocks
        # usually burns quota/time without real progress.
        self.abort_on_repeated_risk = (
            str(os.getenv("PIPELINE_ABORT_ON_REPEATED_RISK", "true")).strip().lower()
            in ("1", "true", "yes", "on")
        )
        self.max_consecutive_risk_failures = max(
            1, int(os.getenv("PIPELINE_MAX_CONSECUTIVE_RISK_FAILURES", "5"))
        )
        self._consecutive_risk_failures = 0

    @staticmethod
    def _looks_like_human_verification_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        if not text:
            return False
        keys = [
            "captcha",
            "human verification",
            "人机验证",
            "安全验证",
            "访问受限",
            "behavior",
            "timed out waiting for doubao answer content",
            "session expired",
            "login required",
            "not logged in",
            "请登录",
            "target page, context or browser has been closed",
        ]
        return any(k in text for k in keys)

    @staticmethod
    def _is_captcha_or_suspected_error(exc: Exception) -> bool:
        """Captcha signals or timeout-like symptoms that should escalate to headed immediately."""
        text = str(exc or "").lower()
        if not text:
            return False
        keys = [
            "captcha",
            "human verification",
            "人机验证",
            "安全验证",
            "访问受限",
            "behavior",
            "timed out waiting for doubao answer content",
        ]
        return any(k in text for k in keys)

    async def _manual_verify_then_resume(self) -> None:
        """Switch to headed mode, send test message to trigger captcha,
        then auto-solve (up to 5 attempts). Falls back to manual if auto fails.

        Mirrors the proven manual recovery flow:
          login → new chat → 思考 mode → send message → detect & solve captcha
        """
        from integration.doubao_web_collector import DoubaoWebCollector
        from integration.captcha_solver import try_solve_captcha

        await self.collector.stop()

        headed = DoubaoWebCollector(headless=False)
        await headed.start()
        page = headed._page
        try:
            logger.warning(
                "Opening headed browser to trigger & solve captcha "
                "(same manual recovery flow)."
            )
            print(
                "\n"
                + "=" * 60
                + "\n  [人机验证恢复] 弹出浏览器窗口，自动尝试求解验证码\n"
                "  若自动求解失败，将等待手动完成 (最多 10 分钟)\n"
                "  ⚠ 验证完成后请勿关闭浏览器，程序将自动保存状态并关闭\n"
                + "=" * 60
                + "\n"
            )

            # --- Step 1: ensure login ---
            if not await headed._is_logged_in():
                logger.info("[captcha_recovery] Not logged in, performing auto-login...")
                ok = await headed.ensure_logged_in()
                if not ok:
                    logger.warning("[captcha_recovery] Auto-login failed, fallback to manual")
                    ok = await headed.manual_login(timeout=600)
                    if not ok:
                        raise RuntimeError("Login not completed in time (600s).")
            else:
                logger.info("[captcha_recovery] Already logged in (session restored)")

            # --- Step 2: new chat + 思考 mode ---
            logger.info("[captcha_recovery] Navigating to new chat + 思考 mode")
            await headed._navigate_new_chat()
            await page.wait_for_timeout(2000)
            await headed._switch_to_think_mode()
            await page.wait_for_timeout(1000)

            # --- Step 3: ensure chat ready ---
            await headed._ensure_default_chat_ready()
            logger.info("[captcha_recovery] Chat ready")

            # --- Step 4: send test message to trigger captcha ---
            test_msg = "你好，请简单介绍一下自己"
            logger.info("[captcha_recovery] Sending test message to trigger captcha: '%s'", test_msg)
            try:
                await headed._send_message(test_msg)
                logger.info("[captcha_recovery] Message sent (no captcha raised by _send_message)")
            except Exception as send_exc:
                if "captcha" in str(send_exc).lower() or "human verification" in str(send_exc).lower():
                    logger.info("[captcha_recovery] _send_message raised captcha error — expected, proceeding to solve")
                else:
                    logger.warning("[captcha_recovery] _send_message failed: %s", send_exc)

            # --- Step 5: post-send captcha detection + auto-solve (40s, 1Hz) ---
            logger.info("[captcha_recovery] Starting post-send captcha detection (40s window)")
            MAX_SOLVE = 5
            solve_count = 0
            auto_solved = False
            generation_started = False

            for tick in range(40):
                detected = await headed._check_captcha()

                if detected and solve_count < MAX_SOLVE:
                    solve_count += 1
                    logger.warning(
                        "[captcha_recovery] CAPTCHA detected at tick=%d, auto-solve attempt %d/%d",
                        tick, solve_count, MAX_SOLVE,
                    )
                    try:
                        solved, err = await try_solve_captcha(page)
                    except Exception as solve_exc:
                        logger.warning(
                            "[captcha_recovery] try_solve_captcha raised: %s", solve_exc
                        )
                        solved, err = False, str(solve_exc)
                    if solved:
                        auto_solved = True
                        logger.info(
                            "[captcha_recovery] CAPTCHA SOLVED at tick=%d (attempt %d/%d)",
                            tick, solve_count, MAX_SOLVE,
                        )
                        await page.wait_for_timeout(2000)
                        break
                    logger.warning("[captcha_recovery] Solve attempt %d failed: %s", solve_count, err)
                    await page.wait_for_timeout(2000)
                    continue

                # Check if generation has started (captcha cleared naturally or no captcha)
                try:
                    stop_btn = await page.locator(
                        'button:has-text("停止"), button:has-text("停止生成"), [class*="stop"]'
                    ).count()
                    answer_len = 0
                    for sel in [".markdown-body", '[class*="flow-markdown"]', '[class*="message-content"]']:
                        loc = page.locator(sel)
                        if await loc.count() > 0:
                            txt = (await loc.last.inner_text()).strip()
                            if txt:
                                answer_len = len(txt)
                                break
                    if stop_btn > 0 or answer_len > 50:
                        logger.info(
                            "[captcha_recovery] Generation started at tick=%d (stop=%d, answer_len=%d)",
                            tick, stop_btn, answer_len,
                        )
                        generation_started = True
                        break
                except Exception:
                    pass

                if tick % 5 == 0:
                    logger.info("[captcha_recovery] tick=%d detected=%s", tick, detected)

                await page.wait_for_timeout(1000)

            # --- Step 6: if captcha solved or generation started, save state immediately ---
            # Save BEFORE the stabilize wait: if user closes browser during wait, we already persisted.
            session_ok = auto_solved or generation_started
            ok = False
            if session_ok:
                try:
                    if await headed._is_logged_in():
                        logger.info("[captcha_recovery] Auto-recovery successful, saving state (do not close browser)")
                        await headed.save_state()
                        ok = True
                except Exception as save_exc:
                    err_text = str(save_exc).lower()
                    if "target page" in err_text or "browser has been closed" in err_text:
                        logger.warning(
                            "[captcha_recovery] Browser was closed before save; "
                            "please do not close the window until 'Browser state saved' appears."
                        )
                        raise
                    raise

            if ok:
                logger.info("[captcha_recovery] Waiting for test answer to stabilize (60s max)...")
                prev = ""
                stable = 0
                t0 = time.time()
                while time.time() - t0 < 60:
                    try:
                        cur = ""
                        for sel in [".markdown-body", '[class*="flow-markdown"]', '[class*="message-content"]']:
                            loc = page.locator(sel)
                            if await loc.count() > 0:
                                cur = (await loc.last.inner_text()).strip()
                                if cur:
                                    break
                        if cur and cur == prev:
                            stable += 1
                            if stable >= 3:
                                logger.info("[captcha_recovery] Answer stabilized, session is clean")
                                break
                        else:
                            stable = 0
                        prev = cur
                    except Exception as e:
                        err_text = str(e).lower()
                        if "target page" in err_text or "browser has been closed" in err_text:
                            logger.warning(
                                "[captcha_recovery] Browser was closed during stabilize; "
                                "state already saved, proceeding."
                            )
                            break
                        raise
                    await page.wait_for_timeout(2000)
            else:
                logger.warning(
                    "[captcha_recovery] Auto-solve not effective (solved=%s, gen=%s). "
                    "Waiting for manual intervention (600s)...",
                    auto_solved, generation_started,
                )
                ok = await headed.manual_login(timeout=600)

            if not ok:
                raise RuntimeError(
                    "Manual verification/login not completed in time (600s)."
                )
        finally:
            try:
                await headed.stop()
            except Exception as stop_exc:
                err_text = str(stop_exc).lower()
                if "target page" in err_text or "browser has been closed" in err_text:
                    logger.info("[captcha_recovery] Browser already closed by user, skipping stop")
                else:
                    raise

        self.collector = DoubaoWebCollector(headless=self.web_headless)
        await self.collector.start()
        if not await self.collector.ensure_logged_in():
            raise RuntimeError("Failed to restore logged-in session after manual verification.")

    async def _switch_account_then_resume(self) -> None:
        """Try switching account in headless mode; if failed caller may fallback to manual."""
        await self.collector.stop()
        from integration.doubao_web_collector import DoubaoWebCollector

        self.collector = DoubaoWebCollector(headless=self.web_headless)
        await self.collector.start()
        ok = await self.collector.switch_account()
        if not ok:
            raise RuntimeError("Account switch failed: auto-login with a new phone was not successful.")
        logger.info("Account switched successfully; will retry current query")

    async def _rebuild_browser(self) -> None:
        """Stop current collector and start a fresh one with re-login."""
        from integration.doubao_web_collector import DoubaoWebCollector

        logger.warning("Rebuilding browser session...")
        try:
            await self.collector.stop()
        except Exception:
            pass
        self.collector = DoubaoWebCollector(headless=self.web_headless)
        await self.collector.start()
        if not await self.collector.ensure_logged_in():
            raise RuntimeError("Failed to re-login after browser rebuild.")
        logger.info("Browser rebuilt and logged in successfully")

    @staticmethod
    def _is_browser_closed_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        return "target page, context or browser has been closed" in text

    async def _collect_one_with_risk_recovery(
        self, query_id: str, query_text: str, *,
        _skip_claim: bool = False, _query_updated_at=None,
    ) -> None:
        """Collect one query with ordered recovery:
        1. TargetClosedError -> rebuild browser + retry
        2. captcha/suspected -> headed solve/manual -> retry
        3. switch account -> retry
        """
        try:
            result = await self.collector.collect_one(
                query_id, query_text,
                _skip_claim=_skip_claim, _query_updated_at=_query_updated_at,
            )
            if result and result.get("skipped"):
                logger.info("Collect %s: skipped (already claimed by another worker)", query_id)
            return
        except Exception as exc:
            # Browser crashed — rebuild and retry once before other recovery
            if self._is_browser_closed_error(exc):
                logger.warning(
                    "Collect %s: browser closed unexpectedly, rebuilding...", query_id
                )
                await self._rebuild_browser()
                try:
                    await self.collector.collect_one(
                        query_id, query_text, _skip_claim=True,
                    )
                    return
                except Exception as rebuild_exc:
                    logger.warning(
                        "Collect %s still failed after rebuild: %s", query_id, rebuild_exc
                    )
                    if not self._looks_like_human_verification_error(rebuild_exc):
                        raise
                    exc = rebuild_exc

            if (not self.web_headless) or (not self._looks_like_human_verification_error(exc)):
                raise
            logger.warning(
                "Collect %s hit risk-like failure: %s", query_id, exc
            )
            risk_exc = exc

        if self._is_captcha_or_suspected_error(risk_exc):
            # Set query back to 'processing' so the monitor doesn't see
            # pending=0,processing=0 and kill the process during recovery.
            execute(
                "UPDATE qa_query SET status = 'processing' WHERE query_id = %s",
                (query_id,),
            )
            await self._manual_verify_then_resume()
            logger.info("Retrying %s after headed captcha/manual handling...", query_id)
            try:
                await self.collector.collect_one(
                    query_id, query_text, _skip_claim=True,
                )
                return
            except Exception as headed_exc:
                logger.warning(
                    "Collect %s still failed after headed handling: %s; fallback to account switch",
                    query_id,
                    headed_exc,
                )

        await self._switch_account_then_resume()
        logger.info("Retrying %s after account switch...", query_id)
        await self.collector.collect_one(
            query_id, query_text, _skip_claim=True,
        )

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------
    def run(
        self,
        batch_size: int = 10,
        query_ids: list[str] | None = None,
        *,
        query_limit: int | None = None,
        category_prefix: str | None = None,
        crawl_concurrency: int = 3,
    ):
        """Execute the complete pipeline: collect -> crawl -> structure."""
        selected_query_ids = query_ids or self.select_query_ids(
            limit=query_limit or batch_size,
            category_prefix=category_prefix,
        )
        if not selected_query_ids:
            logger.info("No target queries selected for run.")
            return
        logger.info("=== Pipeline START (batch_size=%d) ===", batch_size)

        # Step 1: Collect Doubao answers
        logger.info("--- Step 1: Collecting answers ---")
        collected = self.step_collect(batch_size, selected_query_ids)
        logger.info("Step 1 done: %d queries processed", len(collected))

        # Step 2: Crawl cited links
        logger.info("--- Step 2: Crawling links ---")
        link_limit = batch_size * 5
        crawled = asyncio.run(
            self.step_crawl(link_limit, query_ids=selected_query_ids, concurrency=crawl_concurrency)
        )
        logger.info("Step 2 done: %d links crawled", len(crawled))

        # Step 2.5: Enrich Douyin data from douyin_videos / douyin_comments
        logger.info("--- Step 2.5: Enriching Douyin data from crawler DB ---")
        enriched = self.step_enrich_douyin(query_ids=selected_query_ids)
        logger.info("Step 2.5 done: %d Douyin links enriched", enriched)

        # Step 2.6: Transcribe Douyin audio (download -> extract/compress -> Seed2)
        logger.info("--- Step 2.6: Transcribing Douyin audio ---")
        transcribed = self.step_audio_transcribe(
            query_ids=selected_query_ids,
            concurrency=max(1, crawl_concurrency),
        )
        logger.info("Step 2.6 done: %d Douyin links transcribed", transcribed)

        # Step 3: Structure content
        logger.info("--- Step 3: Structuring content ---")
        structured = self.step_structure(query_ids=selected_query_ids)
        logger.info("Step 3 done: %d items structured", structured)

        logger.info("=== Pipeline COMPLETE ===")
        self._print_summary()

    # ------------------------------------------------------------------
    # Individual steps (can be run independently)
    # ------------------------------------------------------------------
    def step_collect(
        self,
        batch_size: int = 10,
        query_ids: list[str] | None = None,
        *,
        query_limit: int | None = None,
        category_prefix: str | None = None,
    ) -> list[str]:
        """Step 1: Collect Doubao answers for pending queries (default: web UI with 深度思考 links)."""
        selected_query_ids = query_ids or self.select_query_ids(
            limit=query_limit or batch_size,
            category_prefix=category_prefix,
        )
        if not selected_query_ids:
            return []
        if self.use_web:
            return asyncio.run(
                self._step_collect_web_async(batch_size, selected_query_ids)
            )
        # API path (sync)
        if selected_query_ids:
            processed = []
            for qid in selected_query_ids:
                row = fetch_one(
                    "SELECT query_id, query_text FROM qa_query WHERE query_id = %s",
                    (qid,),
                )
                if row:
                    try:
                        self.collector.collect_answer(row["query_id"], row["query_text"])
                        processed.append(qid)
                    except Exception:
                        logger.exception("Failed %s", qid)
            return processed
        return self.collector.batch_collect(batch_size)

    async def _ensure_browser_ready(self) -> None:
        """Verify collector is started and logged in; rebuild if not."""
        try:
            if self.collector._page and not self.collector._page.is_closed():
                if await self.collector._is_logged_in():
                    return
        except Exception:
            pass
        logger.warning("Browser not ready, rebuilding...")
        await self._rebuild_browser()

    async def _ensure_collector_started_and_logged_in(self) -> None:
        """Start collector if not started, ensure logged in."""
        if not self.collector._browser:
            await self.collector.start()
        if not await self.collector.ensure_logged_in():
            raise RuntimeError(
                "Web 采集需要登录。请先执行: python integration/run.py web-login\n"
                "或配置 .env 中 SMS_API_TOKEN 后由脚本自动模拟登陆（登录状态会保存复用）。"
            )

    async def _collect_query_list(self, query_ids: list[str]) -> list[str]:
        """Collect a list of queries, with browser-ready check before each one."""
        processed = []
        for qid in query_ids:
            row = fetch_one(
                "SELECT query_id, query_text FROM qa_query WHERE query_id = %s",
                (qid,),
            )
            if row:
                try:
                    await self._ensure_browser_ready()
                    await self._collect_one_with_risk_recovery(
                        row["query_id"], row["query_text"]
                    )
                    processed.append(qid)
                    self._consecutive_risk_failures = 0
                except Exception:
                    logger.exception("Failed %s", qid)
                    if self.abort_on_repeated_risk:
                        err_text = sys.exc_info()[1]
                        if err_text and self._looks_like_human_verification_error(err_text):
                            self._consecutive_risk_failures += 1
                            logger.error(
                                "Consecutive risk failures: %d/%d (latest=%s)",
                                self._consecutive_risk_failures,
                                self.max_consecutive_risk_failures,
                                qid,
                            )
                            if self._consecutive_risk_failures >= self.max_consecutive_risk_failures:
                                raise RuntimeError(
                                    "Aborted run due to repeated CAPTCHA/human verification failures "
                                    f"({self._consecutive_risk_failures} consecutive). "
                                    "Please solve verification in headed mode and resume later."
                                )
                        else:
                            self._consecutive_risk_failures = 0
        return processed

    async def _step_collect_web_async(
        self, batch_size: int = 10, query_ids: list[str] | None = None
    ) -> list[str]:
        """Run web collector: start browser -> 确保登录(复用 state 或模拟登陆) -> collect -> stop."""
        await self._ensure_collector_started_and_logged_in()
        try:
            if query_ids:
                return await self._collect_query_list(query_ids)
            from shared.claim_functions import claim_pending_queries
            rows = claim_pending_queries(batch_size)
            if not rows:
                logger.info("No pending queries")
                return []
            processed = []
            for i, row in enumerate(rows):
                try:
                    await self._ensure_browser_ready()
                    await self._collect_one_with_risk_recovery(
                        row["query_id"], row["query_text"],
                        _skip_claim=True, _query_updated_at=row.get("updated_at"),
                    )
                    processed.append(row["query_id"])
                    self._consecutive_risk_failures = 0
                except Exception:
                    logger.exception("Failed %s", row["query_id"])
                    if self.abort_on_repeated_risk:
                        err_text = sys.exc_info()[1]
                        if err_text and self._looks_like_human_verification_error(err_text):
                            self._consecutive_risk_failures += 1
                            logger.error(
                                "Consecutive risk failures: %d/%d (latest=%s)",
                                self._consecutive_risk_failures,
                                self.max_consecutive_risk_failures,
                                row["query_id"],
                            )
                            if self._consecutive_risk_failures >= self.max_consecutive_risk_failures:
                                raise RuntimeError(
                                    "Aborted run due to repeated CAPTCHA/human verification failures "
                                    f"({self._consecutive_risk_failures} consecutive). "
                                    "Please solve verification in headed mode and resume later."
                                )
                        else:
                            self._consecutive_risk_failures = 0
                if i < len(rows) - 1:
                    await asyncio.sleep(60)
            return processed
        finally:
            await self.collector.stop()

    async def collect_queries_persistent(self, query_ids: list[str]) -> list[str]:
        """Collect queries keeping the browser alive (for run-sync long-lived worker).

        Unlike _step_collect_web_async, does NOT stop the browser afterwards.
        Caller is responsible for calling collector.stop() when finished.
        """
        await self._ensure_collector_started_and_logged_in()
        return await self._collect_query_list(query_ids)

    async def step_crawl(
        self,
        batch_size: int = 50,
        *,
        query_ids: list[str] | None = None,
        concurrency: int = 3,
    ) -> list[str]:
        """Step 2: Crawl pending links."""
        return await self.crawler_mgr.batch_crawl(
            batch_size,
            query_ids=query_ids,
            concurrency=concurrency,
        )

    @staticmethod
    def select_query_ids(
        *,
        limit: int,
        category_prefix: str | None = None,
    ) -> list[str]:
        """Select not-yet-run queries in id order, with optional category prefix filter."""
        sql = (
            "SELECT query_id FROM qa_query "
            "WHERE status = 'pending' "
        )
        params: list[object] = []
        if category_prefix:
            sql += "AND category LIKE %s "
            params.append(f"{category_prefix}%")
        sql += "ORDER BY id LIMIT %s"
        params.append(int(limit))
        rows = fetch_all(sql, tuple(params))
        return [r["query_id"] for r in rows]

    def step_enrich_douyin(self, *, query_ids: list[str] | None = None) -> int:
        """Step 2.5: Enrich Douyin links with data from douyin_videos / douyin_comments.

        Only processes links that haven't been enriched yet (no raw_json in qa_link_content)
        or have no qa_link_content row at all.
        """
        if query_ids:
            any_frag, any_params = sb.expand_any("l.query_id", query_ids)
            rows = fetch_all(
                "SELECT l.link_id FROM qa_link l "
                "LEFT JOIN qa_link_content lc ON l.link_id = lc.link_id "
                f"WHERE l.platform = '抖音' AND {any_frag} "
                "AND (lc.link_id IS NULL OR lc.raw_json IS NULL)",
                any_params,
            )
        else:
            rows = fetch_all(
                "SELECT l.link_id FROM qa_link l "
                "LEFT JOIN qa_link_content lc ON l.link_id = lc.link_id "
                "WHERE l.platform = '抖音' "
                "AND (lc.link_id IS NULL OR lc.raw_json IS NULL)",
            )
        if not rows:
            return 0
        link_ids = [r["link_id"] for r in rows]
        return self.douyin_merger.merge_all(link_ids=link_ids)

    @staticmethod
    def step_audio_transcribe(
        *,
        query_ids: list[str] | None = None,
        concurrency: int = 2,
        batch_size: int = 1000,
    ) -> int:
        """Step 2.6: Download Douyin video, extract/compress audio, transcribe to text."""
        from integration.douyin_audio_transcriber import batch_process

        return batch_process(
            query_ids=query_ids,
            concurrency=max(1, int(concurrency)),
            batch_size=max(1, int(batch_size)),
        )

    def step_structure(
        self,
        *,
        query_ids: list[str] | None = None,
        link_ids: list[str] | None = None,
        concurrency: int = 5,
    ) -> int:
        """Step 3: Read raw content from qa_link_content, re-structure,
        and update the content_json to the canonical format.
        按平台走不同解析：抖音→LLM 文案与特征，通用→Agent 网页摘要（见 parsing_routing）。
        并发 concurrency 条（默认 5）。失败时动态降级：单条重试（指数退避）、chunk 失败则降低并发并增加批间延迟。
        """
        # Only select records where:
        # - crawl is done (l.status = 'done')
        # - raw material exists (raw_json IS NOT NULL)
        # - LLM structuring hasn't been done yet (content_json IS NULL)
        if link_ids:
            any_frag, any_params = sb.expand_any("lc.link_id", link_ids)
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id "
                f"WHERE l.status = 'done' AND {any_frag} "
                "AND lc.raw_json IS NOT NULL AND lc.content_json IS NULL",
                any_params,
            )
        elif query_ids:
            any_frag, any_params = sb.expand_any("l.query_id", query_ids)
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id "
                f"WHERE l.status = 'done' AND {any_frag} "
                "AND lc.raw_json IS NOT NULL AND lc.content_json IS NULL",
                any_params,
            )
        else:
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id "
                "WHERE l.status = 'done' "
                "AND lc.raw_json IS NOT NULL AND lc.content_json IS NULL"
            )

        to_process: list[dict] = []
        for row in rows:
            raw = row.get("raw_json")
            if isinstance(raw, str):
                raw = json.loads(raw)
            if raw is None:
                raw = {}
            row["_raw"] = raw
            to_process.append(row)

        def _process_one(r: dict) -> None:
            raw = r["_raw"]
            content_format = r["content_format"] or "图文B"
            platform = (r.get("platform") or "").strip()
            structured = self.structurer.structure(raw, content_format, r["link_id"])
            structured = _post_process_by_platform(platform, raw, structured, r["link_id"])
            execute(
                "UPDATE qa_link_content "
                "SET content_json = %s, status = 'done' "
                "WHERE link_id = %s",
                (json.dumps(structured, ensure_ascii=False), r["link_id"]),
            )

        def _is_rate_limit(exc: Exception) -> bool:
            t = str(exc).lower()
            return "429" in t or "rate" in t or "limit" in t or "quota" in t or "throttl" in t

        def _process_with_retry(r: dict, max_retries: int = 3) -> bool:
            for attempt in range(max_retries):
                try:
                    _process_one(r)
                    return True
                except Exception as exc:
                    if attempt < max_retries - 1:
                        backoff = 2 ** attempt
                        if _is_rate_limit(exc):
                            backoff = backoff * 3
                            logger.warning("Rate limit detected for %s, extended backoff %ds", r["link_id"], backoff)
                        else:
                            logger.warning(
                                "Structure %s attempt %d/%d failed: %s, retry in %ds",
                                r["link_id"], attempt + 1, max_retries, exc, backoff,
                            )
                        time.sleep(backoff)
                    else:
                        logger.error("Structure %s failed after %d retries: %s", r["link_id"], max_retries, exc)
                        try:
                            execute(
                                "UPDATE qa_link_content SET status = 'error' WHERE link_id = %s",
                                (r["link_id"],),
                            )
                        except Exception:
                            pass
                        raise
            return False

        count = 0
        current_concurrency = min(concurrency, len(to_process)) or 1
        base_delay = 0.0
        remaining = list(to_process)

        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            while remaining:
                chunk_size = min(current_concurrency, len(remaining))
                chunk = remaining[:chunk_size]
                remaining = remaining[chunk_size:]

                if base_delay > 0:
                    logger.info("Structure backoff: sleeping %.1fs, concurrency=%d", base_delay, chunk_size)
                    time.sleep(base_delay)

                futures = {ex.submit(_process_with_retry, r): r for r in chunk}
                chunk_failures = 0
                for fut in as_completed(futures):
                    try:
                        fut.result()
                        count += 1
                    except Exception:
                        chunk_failures += 1

                if chunk_failures > 0:
                    current_concurrency = max(1, current_concurrency - 2)
                    base_delay = base_delay + 2.0 * chunk_failures
                    logger.warning(
                        "Structure chunk: %d failed, degrade to concurrency=%d, next_delay=%.1fs",
                        chunk_failures, current_concurrency, base_delay,
                    )
                else:
                    base_delay = max(0, base_delay - 0.5)
                    if current_concurrency < concurrency and base_delay <= 0:
                        current_concurrency = min(concurrency, current_concurrency + 1)

        return count

    def step_regenerate_content(
        self,
        *,
        link_ids: list[str] | None = None,
        include_all: bool = False,
        force: bool = False,
    ) -> int:
        """根据链接重新生成 qa_link_content，默认仅处理最近更新且 done 的链接。

        Args:
            link_ids: 指定要重生的 link_id 列表；提供后仅处理这些行。
            include_all: True 时处理全量 qa_link_content；默认 False。
            force: True 时允许低质量覆盖；默认 False（防降级覆盖）。
        """
        from structurer import structured_to_raw

        if link_ids:
            any_frag, any_params = sb.expand_any("lc.link_id", link_ids)
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id "
                f"WHERE {any_frag}",
                any_params,
            )
        elif include_all:
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id"
            )
        else:
            rows = fetch_all(
                "SELECT lc.link_id, lc.raw_json, lc.content_json, l.content_format, l.platform "
                "FROM qa_link_content lc "
                "JOIN qa_link l ON l.link_id = lc.link_id "
                "WHERE l.status = 'done' "
                f"AND l.updated_at >= {sb.interval_ago(2)}"
            )
        count = 0
        skipped = 0
        for row in rows:
            content = row.get("content_json") or row.get("raw_json")
            if isinstance(content, str):
                content = json.loads(content)
            content = content or {}
            content_format = row["content_format"] or "图文B"
            platform = (row.get("platform") or "").strip()
            link_id = row["link_id"]

            if "链接ID" in content:
                raw = structured_to_raw(content, content_format)
            else:
                raw = content

            structured = self.structurer.structure(raw, content_format, link_id)
            structured = _post_process_by_platform(platform, raw, structured, link_id)

            if not force:
                old_score = _content_quality_score(content)
                new_score = _content_quality_score(structured)
                if new_score < old_score:
                    skipped += 1
                    logger.info(
                        "Skip regenerate for %s: new score %d < old score %d",
                        link_id,
                        new_score,
                        old_score,
                    )
                    continue

            execute(
                "UPDATE qa_link_content "
                "SET content_json = %s, status = 'done' "
                "WHERE link_id = %s",
                (json.dumps(structured, ensure_ascii=False), link_id),
            )
            count += 1
            logger.info("Regenerated content for %s", link_id)
        if skipped:
            logger.info("Regenerate skipped %d rows due to downgrade protection", skipped)
        return count

    # ------------------------------------------------------------------
    # Status & reporting
    # ------------------------------------------------------------------
    @staticmethod
    def status() -> dict:
        """Return a summary of pipeline progress."""
        query_stats = fetch_all(
            "SELECT status, count(*) as cnt FROM qa_query GROUP BY status ORDER BY status"
        )
        link_stats = fetch_all(
            "SELECT status, count(*) as cnt FROM qa_link GROUP BY status ORDER BY status"
        )
        answer_count = fetch_one("SELECT count(*) as cnt FROM qa_answer")
        content_count = fetch_one("SELECT count(*) as cnt FROM qa_link_content")
        video_parse_stats = fetch_all(
            "SELECT COALESCE(v.status, 'null') as status, count(*) as cnt "
            "FROM qa_link_video v "
            "GROUP BY COALESCE(v.status, 'null') "
            "ORDER BY status"
        )

        return {
            "queries": {r["status"]: r["cnt"] for r in query_stats},
            "links": {r["status"]: r["cnt"] for r in link_stats},
            "douyin_video_parse": {r["status"]: r["cnt"] for r in video_parse_stats},
            "answers": answer_count["cnt"] if answer_count else 0,
            "link_contents": content_count["cnt"] if content_count else 0,
        }

    @staticmethod
    def _print_summary():
        stats = QAPipeline.status()
        print("\n=== Pipeline Status ===")
        print(f"Queries:  {stats['queries']}")
        print(f"Answers:  {stats['answers']}")
        print(f"Links:    {stats['links']}")
        print(f"VideoParse(抖音): {stats.get('douyin_video_parse', {})}")
        print(f"Contents: {stats['link_contents']}")

    @staticmethod
    def retry_failed(*, all_errors: bool = False):
        """Reset failed items to pending.

        - queries: always reset from error -> pending
        - links:
          - all_errors=False (default): only reset retryable link errors
          - all_errors=True: reset all link errors
        - processing 超过 2 小时：视为卡死，重置为 pending
        """
        # 卡死的 processing（超过 2 小时）重置为 pending
        _2h_ago = sb.interval_ago(2)
        q_stuck = execute(
            f"UPDATE qa_query SET status = 'pending' "
            f"WHERE status = 'processing' AND updated_at < {_2h_ago}"
        )
        l_stuck = execute(
            f"UPDATE qa_link SET status = 'pending' "
            f"WHERE status = 'processing' AND updated_at < {_2h_ago}"
        )
        v_stuck = execute(
            f"UPDATE qa_link_video "
            f"SET status = 'pending' "
            f"WHERE status = 'processing' "
            f"AND updated_at < {_2h_ago}"
        )
        if sb.is_pg:
            execute(
                "UPDATE qa_link_content lc "
                "SET video_parse_status = 'pending' "
                "FROM qa_link_video v "
                "WHERE v.link_id = lc.link_id AND v.status = 'pending' "
                "AND lc.video_parse_status = 'processing'"
            )
        else:
            execute(
                "UPDATE qa_link_content lc "
                "JOIN qa_link_video v ON v.link_id = lc.link_id "
                "SET lc.video_parse_status = 'pending' "
                "WHERE v.status = 'pending' "
                "AND lc.video_parse_status = 'processing'"
            )
        if q_stuck or l_stuck or v_stuck:
            logger.info(
                "Reset stuck processing (>2h): queries=%d links=%d douyin_video_parse=%d",
                q_stuck,
                l_stuck,
                v_stuck,
            )

        # Clean up orphan data for error queries before resetting:
        # delete answer/link/link_content that have no real content.
        err_qids_rows = fetch_all("SELECT query_id FROM qa_query WHERE status = 'error'")
        err_qids = [r["query_id"] for r in err_qids_rows]
        if err_qids:
            any_frag, any_params = sb.expand_any("query_id", err_qids)
            execute(
                "DELETE FROM qa_link_content WHERE link_id IN "
                f"(SELECT link_id FROM qa_link WHERE {any_frag} AND status = 'error')",
                any_params,
            )
            execute(
                f"DELETE FROM qa_link WHERE {any_frag} AND status = 'error'",
                any_params,
            )
            execute(
                f"DELETE FROM qa_answer WHERE {any_frag} AND "
                "(answer_text IS NULL OR answer_text = '')",
                any_params,
            )

        q_count = execute(
            "UPDATE qa_query SET status = 'pending', error_message = NULL, retry_count = 0 "
            "WHERE status = 'error'"
        )

        if all_errors:
            l_count = execute("UPDATE qa_link SET status = 'pending' WHERE status = 'error'")
            execute("UPDATE qa_link_video SET status = 'pending' WHERE status = 'error'")
            if sb.is_pg:
                execute(
                    "UPDATE qa_link_content lc "
                    "SET video_parse_status = 'pending' "
                    "FROM qa_link_video v "
                    "WHERE v.link_id = lc.link_id AND v.status = 'pending' "
                    "AND lc.video_parse_status = 'error'"
                )
            else:
                execute(
                    "UPDATE qa_link_content lc "
                    "JOIN qa_link_video v ON v.link_id = lc.link_id "
                    "SET lc.video_parse_status = 'pending' "
                    "WHERE v.status = 'pending' "
                    "AND lc.video_parse_status = 'error'"
                )
            logger.info("Reset %d queries and %d links from error to pending (all_errors)", q_count, l_count)
            return {
                "queries_stuck_reset": q_stuck,
                "links_stuck_reset": l_stuck,
                "video_parse_stuck_reset": v_stuck,
                "queries_reset": q_count,
                "links_reset": l_count,
                "mode": "all_errors",
            }

        rows = fetch_all(
            "SELECT l.link_id, lc.raw_json, lc.content_json "
            "FROM qa_link l "
            "LEFT JOIN qa_link_content lc ON lc.link_id = l.link_id "
            "WHERE l.status = 'error'"
        )
        retryable_ids: list[str] = []
        skipped_non_retryable = 0
        for row in rows:
            raw = row.get("raw_json") or row.get("content_json") or {}
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    raw = {}
            err = ""
            if isinstance(raw, dict):
                err = str(raw.get("error") or "")
            if _is_retryable_link_error(err):
                retryable_ids.append(row["link_id"])
            else:
                skipped_non_retryable += 1

        l_count = 0
        if retryable_ids:
            any_frag, any_params = sb.expand_any("link_id", retryable_ids)
            l_count = execute(
                f"UPDATE qa_link SET status = 'pending' WHERE {any_frag}",
                any_params,
            )
            execute(
                f"UPDATE qa_link_video SET status = 'pending' "
                f"WHERE {any_frag} AND status = 'error'",
                any_params,
            )
            if sb.is_pg:
                any_frag2, any_params2 = sb.expand_any("lc.link_id", retryable_ids)
                execute(
                    "UPDATE qa_link_content lc SET video_parse_status = 'pending' "
                    "FROM qa_link_video v "
                    f"WHERE v.link_id = lc.link_id AND {any_frag2} AND v.status = 'pending'",
                    any_params2,
                )
            else:
                any_frag2, any_params2 = sb.expand_any("lc.link_id", retryable_ids)
                execute(
                    "UPDATE qa_link_content lc "
                    "JOIN qa_link_video v ON v.link_id = lc.link_id "
                    f"SET lc.video_parse_status = 'pending' "
                    f"WHERE {any_frag2} AND v.status = 'pending'",
                    any_params2,
                )
        logger.info(
            "Reset %d queries and %d retryable links to pending; skipped_non_retryable=%d",
            q_count,
            l_count,
            skipped_non_retryable,
        )
        return {
            "queries_stuck_reset": q_stuck,
            "links_stuck_reset": l_stuck,
            "video_parse_stuck_reset": v_stuck,
            "queries_reset": q_count,
            "links_reset": l_count,
            "links_skipped_non_retryable": skipped_non_retryable,
            "mode": "retryable_only",
        }
