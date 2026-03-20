"""Douyin video crawler using local douyin-crawler DB as source of truth.

Flow:
1) Read from douyin_videos / douyin_comments
2) If missing, trigger local node scraper (simulated-login crawler) for target URL
3) Re-read DB and return
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from .base import BaseCrawler

logger = logging.getLogger(__name__)

_VIDEO_ID_RE = re.compile(r"/video/(\d+)")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DOUYIN_SCRAPER_JS = _PROJECT_ROOT / "douyin-crawler" / "douyin-scraper.js"


class DouyinVideoCrawler(BaseCrawler):
    platform = "抖音"
    min_interval = 1.0

    async def crawl(self, url: str) -> dict:
        # 1. Try Douyin Download API (fastest, richest data)
        api_result = await self._fetch_from_api(url)
        if api_result:
            return api_result

        # 2. Fallback to local douyin_videos DB
        db_result = self._fallback_from_db(url)
        if db_result:
            return db_result

        # 3. Try local node scraper → re-read DB
        if self._run_local_scraper(url):
            db_result = self._fallback_from_db(url)
            if db_result:
                return db_result

        return self._empty_result()

    async def _fetch_from_api(self, url: str) -> dict | None:
        """Call dedicated Douyin API endpoints (fetch_one_video + fetch_video_comments)."""
        import httpx

        try:
            from shared.config import CONFIG
            api_base = CONFIG.get("douyin_api", {}).get("url", "http://localhost:8081")
        except Exception:
            api_base = os.getenv("DOUYIN_DOWNLOAD_API_URL", "http://localhost:8081")

        video_id = self._extract_video_id(url)
        if not video_id:
            video_id = await self._resolve_aweme_id(api_base, url)
        if not video_id:
            logger.debug("Cannot extract aweme_id from %s", url[:80])
            return None

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # --- Step 1: fetch video detail ---
                vresp = await client.get(
                    f"{api_base}/api/douyin/web/fetch_one_video",
                    params={"aweme_id": video_id},
                )
                if vresp.status_code != 200:
                    logger.debug("Douyin fetch_one_video returned %d for %s", vresp.status_code, video_id)
                    return None
                vdata = vresp.json()

                aweme = (vdata.get("data", {}).get("aweme_detail")
                         or vdata.get("data", {})
                         or {})
                if not aweme or not aweme.get("desc"):
                    logger.debug("Douyin fetch_one_video returned empty aweme for %s", video_id)
                    return None

                # --- Step 2: fetch comments (best-effort) ---
                comments = await self._fetch_comments_from_api(client, api_base, video_id)

        except Exception as exc:
            logger.debug("Douyin API unavailable: %s", exc)
            return None

        desc = aweme.get("desc") or aweme.get("caption") or ""
        video_obj = aweme.get("video") or {}
        duration = _normalize_duration_seconds(video_obj.get("duration", 0))
        cover_url = _pick_cover_url(aweme, video_obj)
        play_url = _pick_play_url(aweme, video_obj)
        subtitles = _extract_subtitles(aweme)
        author_info = aweme.get("author") or {}
        stats = aweme.get("statistics") or {}

        create_time = aweme.get("create_time")
        publish_time = ""
        if create_time:
            try:
                from datetime import datetime, timezone
                publish_time = datetime.fromtimestamp(int(create_time), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                publish_time = str(create_time)

        item_title = aweme.get("item_title") or ""
        title = item_title or _extract_title_from_desc(desc)

        hashtags = [
            te.get("hashtag_name") or ""
            for te in (aweme.get("text_extra") or [])
            if isinstance(te, dict) and te.get("hashtag_name")
        ]

        video_tags = [
            {"tag_id": t.get("tag_id"), "tag_name": t.get("tag_name"), "level": t.get("level")}
            for t in (aweme.get("video_tag") or [])
            if isinstance(t, dict)
        ]

        suggest_keywords: list[str] = []
        for sg in (aweme.get("suggest_words") or {}).get("suggest_words") or []:
            for w in (sg.get("words") or []):
                word = w.get("word") or ""
                if word:
                    suggest_keywords.append(word)

        logger.info("Douyin API: got video %s, title=%s, desc=%d chars, %d comments, %d subtitles",
                     video_id, title[:40], len(desc), len(comments), len(subtitles))
        return {
            "title": title,
            "content_type": "video",
            "raw_text": desc,
            "caption": aweme.get("caption") or "",
            "video_info": {
                "aweme_id": video_id,
                "duration": duration,
                "cover_url": cover_url,
                "play_url": play_url,
            },
            "subtitles": subtitles,
            "comments": comments,
            "images": [],
            "hashtags": hashtags,
            "video_tags": video_tags,
            "suggest_keywords": suggest_keywords[:10],
            "metadata": {
                "author": author_info.get("nickname") or "",
                "author_id": author_info.get("sec_uid") or "",
                "author_signature": author_info.get("signature") or "",
                "publish_time": publish_time,
                "digg_count": stats.get("digg_count") or 0,
                "comment_count": stats.get("comment_count") or 0,
                "share_count": stats.get("share_count") or 0,
                "play_count": stats.get("play_count") or 0,
                "collected_count": stats.get("collect_count") or 0,
            },
            "_source": "douyin_api",
        }

    @staticmethod
    async def _fetch_comments_from_api(
        client, api_base: str, aweme_id: str, max_pages: int = 3, per_page: int = 20,
    ) -> list[dict]:
        """Fetch comments via /api/douyin/web/fetch_video_comments (paginated)."""
        all_comments: list[dict] = []
        cursor = 0
        for _ in range(max_pages):
            try:
                resp = await client.get(
                    f"{api_base}/api/douyin/web/fetch_video_comments",
                    params={"aweme_id": aweme_id, "cursor": cursor, "count": per_page},
                    timeout=15,
                )
                if resp.status_code != 200:
                    break
                body = resp.json()
                items = body.get("data", {}).get("comments") or []
                if not items:
                    break
                for c in items:
                    if not isinstance(c, dict):
                        continue
                    ct = c.get("create_time", "")
                    if isinstance(ct, (int, float)) and ct > 1_000_000_000:
                        try:
                            from datetime import datetime, timezone
                            ct = datetime.fromtimestamp(int(ct), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            ct = str(ct)
                    all_comments.append({
                        "text": c.get("text") or c.get("content") or "",
                        "digg_count": c.get("digg_count") or 0,
                        "user": (c.get("user") or {}).get("nickname") or "",
                        "create_time": ct,
                        "ip_label": (c.get("ip_label") or ""),
                    })
                has_more = body.get("data", {}).get("has_more")
                next_cursor = body.get("data", {}).get("cursor")
                if not has_more or next_cursor is None:
                    break
                cursor = next_cursor
            except Exception as exc:
                logger.debug("Fetch comments page error: %s", exc)
                break
        # 只保留点赞最高的 20 条（与入库前 postprocess 一致，减少噪声与体积）
        try:
            from integration.raw_content_postprocess import top_comments_by_engagement

            return top_comments_by_engagement(all_comments, max_n=20)
        except Exception:
            all_comments.sort(key=lambda c: int(c.get("digg_count") or 0), reverse=True)
            return all_comments[:20]

    @staticmethod
    async def _resolve_aweme_id(api_base: str, url: str) -> str:
        """Resolve short/redirect URL to aweme_id via API."""
        import httpx
        from urllib.parse import quote
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{api_base}/api/douyin/web/get_aweme_id",
                    params={"url": url},
                )
                if resp.status_code == 200:
                    return resp.json().get("data") or ""
        except Exception:
            pass
        return ""

    def _fallback_from_db(self, url: str) -> dict | None:
        """Read from douyin_videos / douyin_comments tables."""
        video_id = self._extract_video_id(url)
        if not video_id:
            return None

        try:
            from shared.db import fetch_one, fetch_all

            video_row = fetch_one(
                "SELECT * FROM douyin_videos WHERE video_id = %s", (video_id,)
            )
            if not video_row:
                return None

            comments_rows = fetch_all(
                "SELECT username, content, time, location, likes "
                "FROM douyin_comments WHERE video_id = %s ORDER BY likes DESC LIMIT 20",
                (video_id,),
            )

            raw_data = video_row.get("raw_data") or {}
            if isinstance(raw_data, str):
                raw_data = json.loads(raw_data)

            cover_url, play_url, duration = "", "", 0
            video_obj = raw_data.get("video") or {}
            cover_obj = raw_data.get("cover") or video_obj.get("cover") or {}
            if isinstance(cover_obj, dict):
                urls = cover_obj.get("url_list") or []
                cover_url = urls[0] if urls else ""
            elif isinstance(cover_obj, str):
                cover_url = cover_obj
            play_addr = video_obj.get("play_addr") or {}
            play_urls = play_addr.get("url_list") or []
            play_url = play_urls[0] if play_urls else ""
            duration = _normalize_duration_seconds(
                video_obj.get("duration", 0) or raw_data.get("duration", 0)
            )
            subtitles = _extract_subtitles(raw_data)

            comments = [
                {
                    "text": c.get("content") or "",
                    "digg_count": c.get("likes") or 0,
                    "user": c.get("username") or "",
                    "create_time": c.get("time") or "",
                    "location": c.get("location") or "",
                }
                for c in comments_rows
            ]

            logger.info("DB fallback: loaded video %s with %d comments", video_id, len(comments))
            return {
                "title": video_row.get("title") or "",
                "content_type": "video",
                "raw_text": video_row.get("title") or "",
                "video_info": {
                    "aweme_id": video_id,
                    "duration": duration,
                    "cover_url": cover_url,
                    "play_url": play_url,
                },
                "subtitles": subtitles,
                "comments": comments,
                "images": [],
                "metadata": {
                    "author": video_row.get("author") or "",
                    "author_id": "",
                    "publish_time": "",
                    "digg_count": video_row.get("likes") or 0,
                    "comment_count": video_row.get("comments_count") or 0,
                    "share_count": video_row.get("shares") or 0,
                    "play_count": 0,
                    "collected_count": video_row.get("favorites") or 0,
                    "share_link": video_row.get("share_link") or "",
                    "short_link": video_row.get("short_link") or "",
                    "likes_display": video_row.get("likes_display") or "",
                    "comments_display": video_row.get("comments_display") or "",
                    "favorites_display": video_row.get("favorites_display") or "",
                    "shares_display": video_row.get("shares_display") or "",
                },
                "_source": "douyin_videos_db_fallback",
            }
        except Exception as exc:
            logger.warning("DB fallback failed for %s: %s", url[:80], exc)
            return None

    @staticmethod
    def _run_local_scraper(url: str) -> bool:
        """Trigger local node scraper for one URL.

        Returns True if command exits successfully.
        """
        enabled = os.getenv("ENABLE_DOUYIN_LOCAL_SCRAPER", "false").lower() in ("1", "true", "yes")
        if not enabled:
            logger.info(
                "Skip local douyin scraper for silent mode (ENABLE_DOUYIN_LOCAL_SCRAPER=false), url=%s",
                url[:100],
            )
            return False
        if not _DOUYIN_SCRAPER_JS.exists():
            logger.warning("Douyin scraper script not found: %s", _DOUYIN_SCRAPER_JS)
            return False
        cmd = [
            "node",
            str(_DOUYIN_SCRAPER_JS),
            "--url",
            url,
        ]
        try:
            logger.info("Run local douyin crawler for URL: %s", url[:100])
            proc = subprocess.run(
                cmd,
                cwd=str(_PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=240,
                check=False,
            )
            if proc.returncode != 0:
                logger.warning(
                    "Local douyin crawler failed (code=%s): %s",
                    proc.returncode,
                    (proc.stderr or proc.stdout or "")[:300],
                )
                return False
            return True
        except Exception as exc:
            logger.warning("Run local douyin crawler exception for %s: %s", url[:80], exc)
            return False

    @staticmethod
    def _extract_video_id(url: str) -> str:
        m = _VIDEO_ID_RE.search(url or "")
        return m.group(1) if m else ""

    @staticmethod
    def _empty_result() -> dict:
        return {
            "title": "", "content_type": "video", "raw_text": "",
            "video_info": {"aweme_id": "", "duration": 0, "cover_url": "", "play_url": ""},
            "subtitles": [],
            "comments": [], "images": [], "metadata": {},
        }



def _extract_title_from_desc(desc: str) -> str:
    """Extract a clean title from desc, stripping leading #hashtags."""
    if not desc:
        return ""
    text = desc.strip()
    # Remove leading #hashtag segments
    while text.startswith("#"):
        space_idx = text.find(" ", 1)
        if space_idx == -1:
            text = ""
            break
        text = text[space_idx:].strip()
    # Take the first meaningful segment (before any remaining # or newline)
    for sep in ("#", "\n"):
        idx = text.find(sep)
        if idx > 0:
            text = text[:idx]
    return text.strip()


def _normalize_duration_seconds(raw_duration: int | float | str | None) -> int:
    """Normalize duration to seconds (API may return ms)."""
    try:
        val = int(float(raw_duration or 0))
    except (TypeError, ValueError):
        return 0
    if val <= 0:
        return 0
    # Douyin raw duration is often milliseconds.
    if val > 10_000:
        return max(1, val // 1000)
    return val


def _pick_cover_url(video_data: dict, video_obj: dict) -> str:
    cover = video_data.get("cover")
    if isinstance(cover, str) and cover:
        return cover
    if isinstance(cover, dict):
        urls = cover.get("url_list") or []
        if urls:
            return urls[0]
    vcover = video_obj.get("cover") or {}
    if isinstance(vcover, dict):
        urls = vcover.get("url_list") or []
        if urls:
            return urls[0]
    return ""


def _pick_play_url(video_data: dict, video_obj: dict) -> str:
    play = video_data.get("play")
    if isinstance(play, str) and play:
        return play
    if isinstance(play, dict):
        urls = play.get("url_list") or []
        if urls:
            return urls[0]
    play_addr = video_obj.get("play_addr") or {}
    urls = play_addr.get("url_list") or []
    return urls[0] if urls else ""


def _extract_subtitles(payload: dict) -> list[dict]:
    """Extract subtitles into structurer-compatible format."""
    candidates = []
    for key in ("subtitles", "subtitle", "caption_info", "captionInfos", "video_subtitle"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
        elif isinstance(value, dict):
            for sub_key in ("subtitles", "list", "captions"):
                inner = value.get(sub_key)
                if isinstance(inner, list):
                    candidates.extend(inner)
    if not candidates:
        return []

    result: list[dict] = []
    for s in candidates:
        if not isinstance(s, dict):
            continue
        text = (
            s.get("text")
            or s.get("subtitle")
            or s.get("content")
            or s.get("value")
            or ""
        )
        if not text:
            continue
        start = (
            s.get("start_time")
            or s.get("start")
            or s.get("begin")
            or s.get("timestamp")
            or ""
        )
        result.append({"start_time": start, "text": text})
    return result


def _build_candidate_urls(url: str) -> list[str]:
    """Try original url first, then canonical /video/{id} form."""
    u = (url or "").strip()
    candidates: list[str] = []
    if u:
        candidates.append(u)
    vid = _extract_video_id_any(u)
    if vid:
        canonical = f"https://www.douyin.com/video/{vid}"
        if canonical not in candidates:
            candidates.append(canonical)
        share = f"https://www.iesdouyin.com/share/video/{vid}"
        if share not in candidates:
            candidates.append(share)
    return candidates


def _extract_video_id_any(url: str) -> str:
    m = re.search(r"/video/(\d+)", url or "")
    return m.group(1) if m else ""
