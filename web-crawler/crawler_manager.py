"""Crawler dispatcher: 对 qa_link 中的链接爬虫、收集内容、写入 qa_link_content。

按平台路由到对应爬虫（详见 docs/PARSING_ROUTING.md）：
- 抖音 → 抖音视频下载项目 + LLM 解析文案和特征（DouyinVideoCrawler）
- 通用/其他 → 能直接抓取网页摘要和基本信息的网页（如官网），通用爬虫+摘要（GenericWebCrawler）
- B站/小红书 → 各自专用爬虫
- 淘宝/京东 → 不抓正文（skip）
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute, fetch_all
from shared.sql_builder import sb
from integration.parsing_routing import should_crawl_content
from integration.raw_content_postprocess import postprocess_raw_for_storage
from crawlers.base import BaseCrawler
from crawlers.generic_web import GenericWebCrawler
from crawlers.douyin_video import DouyinVideoCrawler
from crawlers.bilibili_video import BilibiliCrawler
from crawlers.xiaohongshu import XiaohongshuCrawler
from crawlers.playwright_web import PlaywrightWebCrawler

logger = logging.getLogger(__name__)


class CrawlerManager:
    """Dispatch links to platform-specific crawlers and persist raw content."""

    def __init__(self):
        _pw_crawler = PlaywrightWebCrawler()
        self._crawlers: dict[str, BaseCrawler] = {
            "抖音": DouyinVideoCrawler(),
            "B站": BilibiliCrawler(),
            "小红书": XiaohongshuCrawler(),
            "头条": _pw_crawler,
            "什么值得买": _pw_crawler,
        }
        self._default = GenericWebCrawler()

    def get_crawler(self, platform: str) -> BaseCrawler:
        return self._crawlers.get(platform, self._default)

    async def crawl_link(self, link_record: dict) -> dict:
        """Crawl a single link, returning raw_content dict."""
        platform = link_record.get("platform", "")
        url = link_record.get("link_url", "")
        link_id = link_record.get("link_id", "")

        normalized_url = _normalize_url_for_crawl(url)
        if normalized_url != url:
            execute(
                "UPDATE qa_link SET link_url = %s, updated_at = CURRENT_TIMESTAMP WHERE link_id = %s",
                (normalized_url, link_id),
            )
            logger.info("Normalized URL for %s: %s -> %s", link_id, url[:80], normalized_url[:80])
            url = normalized_url

        if not should_crawl_content(platform):
            logger.info("Skipping %s link %s (%s)", platform, link_id, url[:60])
            return {"url": url, "platform": platform, "skipped": True}

        crawler = self.get_crawler(platform)
        logger.info("Crawling [%s] %s: %s", crawler.platform, link_id, url[:80])

        result = await crawler.crawl_with_retry(url)
        return result

    async def batch_crawl(
        self,
        batch_size: int = 20,
        *,
        query_ids: list[str] | None = None,
        concurrency: int = 3,
    ) -> list[str]:
        """对 qa_link 中 pending 的链接爬虫、收集内容、入库到 qa_link_content。

        Returns list of successfully crawled link_ids.
        """
        from shared.claim_functions import claim_pending_links
        rows = claim_pending_links(batch_size, query_ids=query_ids)
        if not rows:
            logger.debug("No pending links to crawl.")
            return []

        succeeded: list[str] = []
        sem = asyncio.Semaphore(max(1, int(concurrency or 1)))

        async def _crawl_one(row: dict) -> str | None:
            link_id = row["link_id"]
            try:
                async with sem:
                    raw_content = await self.crawl_link(row)
                # 评论 Top20、超长正文 LLM 去噪、JSON 字节上限（integration/raw_content_postprocess.py）
                raw_content = await asyncio.to_thread(
                    postprocess_raw_for_storage, raw_content, link_id
                )
                self._save_raw_content(link_id, raw_content)

                link_updated_at = row.get("updated_at")
                ol = " AND updated_at = %s" if link_updated_at else ""
                if raw_content.get("skipped"):
                    params = (link_id, link_updated_at) if link_updated_at else (link_id,)
                    n = execute(
                        f"UPDATE qa_link SET status = 'done' WHERE link_id = %s{ol}",
                        params,
                    )
                    if link_updated_at and n == 0:
                        logger.warning("Link %s optimistic lock failed (done/skip)", link_id)
                elif raw_content.get("error"):
                    err_msg = str(raw_content.get("error", ""))[:500]
                    params = (err_msg, link_id, link_updated_at) if link_updated_at else (err_msg, link_id)
                    n = execute(
                        "UPDATE qa_link SET status = 'error', error_message = %s, "
                        f"retry_count = retry_count + 1 WHERE link_id = %s{ol}",
                        params,
                    )
                    if link_updated_at and n == 0:
                        logger.warning("Link %s optimistic lock failed (error)", link_id)
                else:
                    params = (link_id, link_updated_at) if link_updated_at else (link_id,)
                    n = execute(
                        "UPDATE qa_link SET status = 'done', fetched_at = CURRENT_TIMESTAMP "
                        f"WHERE link_id = %s{ol}",
                        params,
                    )
                    if link_updated_at and n == 0:
                        logger.warning("Link %s optimistic lock failed (done)", link_id)
                    else:
                        return link_id
            except Exception as exc:
                logger.exception("Unexpected error crawling %s", link_id)
                link_updated_at = row.get("updated_at")
                ol = " AND updated_at = %s" if link_updated_at else ""
                params = (str(exc)[:500], link_id, link_updated_at) if link_updated_at else (str(exc)[:500], link_id)
                n = execute(
                    "UPDATE qa_link SET status = 'error', error_message = %s, "
                    f"retry_count = retry_count + 1 WHERE link_id = %s{ol}",
                    params,
                )
                if link_updated_at and n == 0:
                    logger.warning("Link %s optimistic lock failed (error)", link_id)
            return None

        logger.info("Batch crawl start: total=%d, concurrency=%d", len(rows), max(1, int(concurrency or 1)))
        results = await asyncio.gather(*[_crawl_one(row) for row in rows], return_exceptions=False)
        succeeded = [x for x in results if isinstance(x, str) and x]

        logger.info("Batch crawl done: %d / %d succeeded", len(succeeded), len(rows))
        return succeeded

    @staticmethod
    def _save_raw_content(link_id: str, raw_content: dict):
        """将爬取到的内容入库到 qa_link_content（JSONB），抖音同时写 qa_link_video。"""
        import json

        if raw_content.get("skipped"):
            return
        content_status = "error" if raw_content.get("error") else "done"

        # Do not overwrite a good row with an obvious empty shell payload.
        existing = fetch_all(
            "SELECT raw_json, content_json FROM qa_link_content WHERE link_id = %s",
            (link_id,),
        )
        if existing:
            old = existing[0].get("raw_json") or existing[0].get("content_json")
            if isinstance(old, str):
                try:
                    old = json.loads(old)
                except json.JSONDecodeError:
                    old = {}
            if _is_shell_payload(raw_content) and _raw_quality_score(old) > _raw_quality_score(raw_content):
                logger.info("Skip raw overwrite for %s: new payload is lower quality", link_id)
                return

        raw_json_str = json.dumps(raw_content, ensure_ascii=False)
        if sb.is_pg:
            execute(
                "INSERT INTO qa_link_content (link_id, raw_json, content_json, video_parse_status, status) "
                "VALUES ("
                "%s, %s, NULL, "
                "(SELECT CASE WHEN l.platform = '抖音' THEN 'pending' ELSE NULL END "
                " FROM qa_link l WHERE l.link_id = %s), "
                "%s"
                ") "
                "ON CONFLICT (link_id) DO UPDATE SET "
                "raw_json = EXCLUDED.raw_json, content_json = NULL, status = EXCLUDED.status, "
                "video_parse_status = CASE "
                "WHEN (SELECT platform FROM qa_link l WHERE l.link_id = EXCLUDED.link_id) = '抖音' "
                "THEN 'pending' ELSE NULL END",
                (link_id, raw_json_str, link_id, content_status),
            )
        else:
            vps_row = fetch_all(
                "SELECT CASE WHEN l.platform = '抖音' THEN 'pending' ELSE NULL END AS vps "
                "FROM qa_link l WHERE l.link_id = %s",
                (link_id,),
            )
            vps = vps_row[0]["vps"] if vps_row else None
            execute(
                "INSERT INTO qa_link_content (link_id, raw_json, content_json, video_parse_status, status) "
                "VALUES (%s, %s, NULL, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "raw_json = VALUES(raw_json), content_json = NULL, status = VALUES(status), "
                "video_parse_status = VALUES(video_parse_status)",
                (link_id, raw_json_str, vps, content_status),
            )

        _upsert_link_video(link_id, raw_content)


def _upsert_link_video(link_id: str, raw_content: dict) -> None:
    """For Douyin links, create/update qa_link_video with video metadata."""
    import json

    platform_row = fetch_all(
        "SELECT platform FROM qa_link WHERE link_id = %s", (link_id,)
    )
    if not platform_row or platform_row[0].get("platform") != "抖音":
        return

    vi = raw_content.get("video_info") or {}
    subtitles = raw_content.get("subtitles") or []
    has_subtitles = any(
        isinstance(s, dict) and (s.get("text") or "").strip()
        for s in subtitles
    )
    initial_status = "skip" if has_subtitles else "pending"

    if sb.is_pg:
        execute(
            "INSERT INTO qa_link_video "
            "(link_id, model_api_input_type, video_id, play_url, cover_url, duration, subtitles, "
            " raw_api_response, status, fetched_at) "
            "VALUES (%s, 'input_audio', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (link_id, model_api_input_type) DO UPDATE SET "
            "video_id   = COALESCE(NULLIF(EXCLUDED.video_id, ''), qa_link_video.video_id), "
            "play_url   = COALESCE(NULLIF(EXCLUDED.play_url, ''), qa_link_video.play_url), "
            "cover_url  = COALESCE(NULLIF(EXCLUDED.cover_url, ''), qa_link_video.cover_url), "
            "duration   = GREATEST(EXCLUDED.duration, qa_link_video.duration), "
            "subtitles  = COALESCE(EXCLUDED.subtitles, qa_link_video.subtitles), "
            "raw_api_response = COALESCE(EXCLUDED.raw_api_response, qa_link_video.raw_api_response), "
            "fetched_at = COALESCE(qa_link_video.fetched_at, EXCLUDED.fetched_at), "
            "status     = CASE WHEN qa_link_video.status IN ('done','skip') "
            "            THEN qa_link_video.status ELSE EXCLUDED.status END",
            (
                link_id,
                vi.get("aweme_id") or "",
                vi.get("play_url") or "",
                vi.get("cover_url") or "",
                int(vi.get("duration") or 0),
                json.dumps(subtitles, ensure_ascii=False) if subtitles else None,
                json.dumps(raw_content, ensure_ascii=False),
                initial_status,
            ),
        )
    else:
        execute(
            "INSERT INTO qa_link_video "
            "(link_id, model_api_input_type, video_id, play_url, cover_url, duration, subtitles, "
            " raw_api_response, status, fetched_at) "
            "VALUES (%s, 'input_audio', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) "
            "ON DUPLICATE KEY UPDATE "
            "video_id   = COALESCE(NULLIF(VALUES(video_id), ''), video_id), "
            "play_url   = COALESCE(NULLIF(VALUES(play_url), ''), play_url), "
            "cover_url  = COALESCE(NULLIF(VALUES(cover_url), ''), cover_url), "
            "duration   = GREATEST(VALUES(duration), duration), "
            "subtitles  = COALESCE(VALUES(subtitles), subtitles), "
            "raw_api_response = COALESCE(VALUES(raw_api_response), raw_api_response), "
            "fetched_at = COALESCE(fetched_at, VALUES(fetched_at)), "
            "status     = CASE WHEN status IN ('done','skip') "
            "            THEN status ELSE VALUES(status) END",
            (
                link_id,
                vi.get("aweme_id") or "",
                vi.get("play_url") or "",
                vi.get("cover_url") or "",
                int(vi.get("duration") or 0),
                json.dumps(subtitles, ensure_ascii=False) if subtitles else None,
                json.dumps(raw_content, ensure_ascii=False),
                initial_status,
            ),
        )


def _is_shell_payload(raw: dict) -> bool:
    """Heuristic: empty title + empty comments + empty video_info."""
    if not isinstance(raw, dict):
        return True
    if "链接ID" in raw:
        inner = raw.get("结构化内容") or {}
        title = (inner.get("视频标题") or "").strip()
        comments = inner.get("评论") or []
        duration = (inner.get("视频时长") or "").strip()
        has_comment_text = any((c.get("内容") or "").strip() for c in comments if isinstance(c, dict))
        return not title and not has_comment_text and duration in ("", "00:00")

    title = (raw.get("title") or "").strip()
    comments = raw.get("comments") or []
    video_info = raw.get("video_info") or {}
    has_comment_text = any((c.get("text") or "").strip() for c in comments if isinstance(c, dict))
    has_video_info = bool(
        (video_info.get("aweme_id") or "")
        or (video_info.get("play_url") or "")
        or (video_info.get("cover_url") or "")
        or int(video_info.get("duration") or 0) > 0
    )
    return not title and not has_comment_text and not has_video_info


def _raw_quality_score(raw: dict) -> int:
    if not isinstance(raw, dict):
        return 0
    if "链接ID" in raw:
        inner = raw.get("结构化内容") or {}
        title = (inner.get("视频标题") or "").strip()
        comments = inner.get("评论") or []
        duration_text = (inner.get("视频时长") or "").strip()
        duration = int(duration_text not in ("", "00:00"))
        non_empty_comments = sum(1 for c in comments if isinstance(c, dict) and (c.get("内容") or "").strip())
        return int(bool(title)) + non_empty_comments + duration

    title = (raw.get("title") or "").strip()
    comments = raw.get("comments") or []
    video_info = raw.get("video_info") or {}
    duration = int((video_info.get("duration") or 0) > 0)
    non_empty_comments = sum(1 for c in comments if isinstance(c, dict) and (c.get("text") or "").strip())
    return int(bool(title)) + non_empty_comments + duration


_TRAILING_PUNCT_RE = re.compile(r"[，。,；;：:!！\)\]）】]+$")
_DATE_SUFFIX_RE = re.compile(r",\d{4}-\d{2}-\d{2}$")


def _normalize_url_for_crawl(url: str) -> str:
    """Best-effort URL cleanup for malformed citation tails.

    Examples:
    - https://a.com/x.html,2024-02-14 -> https://a.com/x.html
    - https://a.com/x.html, -> https://a.com/x.html
    - trim Chinese/English trailing punctuation.
    """
    u = (url or "").strip()
    if not u:
        return u

    # Remove common malformed date suffix pasted after comma.
    u = _DATE_SUFFIX_RE.sub("", u)

    # Remove trailing commas / punctuation that are not part of URL.
    u = u.rstrip(",")
    u = _TRAILING_PUNCT_RE.sub("", u)

    # Normalize path punctuation without breaking query string.
    try:
        parts = urlsplit(u)
        path = (parts.path or "").rstrip(",")
        path = _TRAILING_PUNCT_RE.sub("", path)
        u = urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))
    except Exception:
        pass
    return u


# ------------------------------------------------------------------
# CLI quick-test
# ------------------------------------------------------------------
if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    mgr = CrawlerManager()

    if len(sys.argv) > 1:
        # Crawl a single URL: python crawler_manager.py <url>
        test_url = sys.argv[1]
        from integration.citation_parser import identify_platform, determine_content_format

        plat = identify_platform(test_url)
        fmt = determine_content_format(test_url, plat)
        result = asyncio.run(mgr.crawl_link({
            "link_id": "TEST",
            "link_url": test_url,
            "platform": plat,
            "content_format": fmt,
        }))
        print(json.dumps(result, ensure_ascii=False, indent=2)[:3000])
    else:
        # Batch crawl pending links
        done = asyncio.run(mgr.batch_crawl())
        print(f"Crawled: {done}")
