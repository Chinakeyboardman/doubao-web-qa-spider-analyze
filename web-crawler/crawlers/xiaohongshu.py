"""Xiaohongshu (小红书) crawler via httpx + BeautifulSoup.

Xiaohongshu note pages are server-rendered, so basic HTTP scraping can extract
the initial data from embedded JSON.
"""

from __future__ import annotations

import json
import logging
import re

from bs4 import BeautifulSoup

from .base import BaseCrawler

logger = logging.getLogger(__name__)

_INITIAL_STATE_RE = re.compile(r"window\.__INITIAL_STATE__\s*=\s*({.+?})\s*;?\s*</script>", re.S)


class XiaohongshuCrawler(BaseCrawler):
    platform = "小红书"
    min_interval = 2.0  # be polite to XHS

    async def crawl(self, url: str) -> dict:
        async with self._get_async_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()

        html = resp.text
        note_data = self._extract_initial_state(html)

        if note_data:
            return self._parse_note(note_data)

        # Fallback: plain HTML extraction
        return self._parse_html(html, url)

    def _extract_initial_state(self, html: str) -> dict | None:
        """Try to extract __INITIAL_STATE__ JSON from SSR HTML."""
        m = _INITIAL_STATE_RE.search(html)
        if not m:
            return None
        try:
            raw = m.group(1).replace("undefined", "null")
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.debug("Failed to parse __INITIAL_STATE__: %s", exc)
            return None

    def _parse_note(self, state: dict) -> dict:
        """Extract structured data from the __INITIAL_STATE__ object."""
        note_detail = state.get("note", {}).get("noteDetailMap", {})
        first_key = next(iter(note_detail), None)
        if not first_key:
            return self._empty_result()

        note = note_detail[first_key].get("note", {})
        user = note.get("user", {})
        interact = note.get("interactInfo", {})
        image_list = note.get("imageList", [])

        images = [
            {
                "url": img.get("urlDefault", ""),
                "alt": img.get("livePhoto", ""),
                "width": img.get("width", 0),
                "height": img.get("height", 0),
            }
            for img in image_list
        ]

        tags = [t.get("name", "") for t in note.get("tagList", [])]

        return {
            "title": note.get("title", ""),
            "content_type": "note",
            "raw_text": note.get("desc", ""),
            "paragraphs": [note.get("desc", "")],
            "images": images,
            "tags": tags,
            "metadata": {
                "author": user.get("nickname", ""),
                "author_id": user.get("userId", ""),
                "liked_count": interact.get("likedCount", 0),
                "collected_count": interact.get("collectedCount", 0),
                "comment_count": interact.get("commentCount", 0),
                "share_count": interact.get("shareCount", 0),
            },
        }

    def _parse_html(self, html: str, url: str) -> dict:
        """Fallback HTML extraction when SSR state is unavailable."""
        soup = BeautifulSoup(html, "lxml")

        title = ""
        title_el = soup.select_one('meta[property="og:title"]')
        if title_el:
            title = title_el.get("content", "")
        elif soup.title:
            title = soup.title.get_text(strip=True)

        desc = ""
        desc_el = soup.select_one('meta[property="og:description"]')
        if desc_el:
            desc = desc_el.get("content", "")

        images: list[dict] = []
        for meta in soup.select('meta[property="og:image"]'):
            src = meta.get("content", "")
            if src:
                images.append({"url": src, "alt": ""})

        return {
            "title": title,
            "content_type": "note",
            "raw_text": desc,
            "paragraphs": [desc] if desc else [],
            "images": images,
            "tags": [],
            "metadata": {},
        }

    @staticmethod
    def _empty_result() -> dict:
        return {
            "title": "",
            "content_type": "note",
            "raw_text": "",
            "paragraphs": [],
            "images": [],
            "tags": [],
            "metadata": {},
        }
