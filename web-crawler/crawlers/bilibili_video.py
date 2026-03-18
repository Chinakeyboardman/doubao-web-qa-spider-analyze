"""Bilibili video crawler via the Douyin_TikTok_Download_API (localhost:8080)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.config import CONFIG

from .base import BaseCrawler

logger = logging.getLogger(__name__)

_API_BASE = CONFIG["douyin_api"]["url"]


class BilibiliCrawler(BaseCrawler):
    platform = "B站"
    min_interval = 1.0

    async def crawl(self, url: str) -> dict:
        video_data = await self._fetch_video_data(url)

        return {
            "title": video_data.get("title", ""),
            "content_type": "video",
            "raw_text": video_data.get("desc", ""),
            "video_info": {
                "bvid": video_data.get("bvid", ""),
                "duration": video_data.get("duration", 0),
                "cover_url": video_data.get("cover", ""),
            },
            "comments": [],
            "images": [],
            "metadata": {
                "author": video_data.get("author", {}).get("name", ""),
                "author_id": video_data.get("author", {}).get("mid", ""),
                "publish_time": video_data.get("pubdate", ""),
                "view_count": video_data.get("stat", {}).get("view", 0),
                "like_count": video_data.get("stat", {}).get("like", 0),
                "coin_count": video_data.get("stat", {}).get("coin", 0),
                "share_count": video_data.get("stat", {}).get("share", 0),
                "danmaku_count": video_data.get("stat", {}).get("danmaku", 0),
            },
        }

    async def _fetch_video_data(self, url: str) -> dict:
        """Call the hybrid parsing endpoint for Bilibili video."""
        async with self._get_async_client() as client:
            resp = await client.get(
                f"{_API_BASE}/api/hybrid/video_data",
                params={"url": url, "minimal": "false"},
            )
            resp.raise_for_status()
            data = resp.json()

        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data if isinstance(data, dict) else {}
