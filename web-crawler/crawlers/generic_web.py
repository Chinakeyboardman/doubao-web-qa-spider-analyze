"""Generic web crawler using httpx + BeautifulSoup.

用于能直接抓取网页摘要和基本信息的网页（如官网、标准文章页）。
也处理 知乎、什么值得买 等未单独实现专用爬虫的站点。
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from .base import BaseCrawler
from .noise_filter import is_noise_paragraph

logger = logging.getLogger(__name__)

_NOISE_TAGS = {
    "script", "style", "nav", "footer", "header", "aside", "iframe", "noscript",
    "button", "select", "option", "textarea", "label", "form",
}
_NOISE_ROLES = {"button", "navigation", "menu", "menubar", "toolbar", "dialog", "alert", "tooltip", "banner", "complementary"}
_NOISE_SELECTORS = [
    '[role="button"]', '[role="navigation"]', '[role="menu"]',
    '[role="menubar"]', '[role="toolbar"]', '[role="dialog"]',
    '[role="alert"]', '[role="tooltip"]', '[role="banner"]',
    '[role="complementary"]',
    '[aria-hidden="true"]',
    '.breadcrumb', '.pagination', '.share-btn', '.share-bar',
    '.comment-form', '.login-form', '.toolbar',
    '.recommend-list', '.related-post', '.sidebar',
]


_CSDN_CONTENT_SELECTORS = ["#article_content", "#content_views", ".article_content"]


class GenericWebCrawler(BaseCrawler):
    platform = "通用"
    min_interval = 1.5

    async def crawl(self, url: str) -> dict:
        async with self._get_async_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        self._remove_noise(soup)

        is_csdn = "csdn.net" in url.lower()
        scope = self._find_content_scope(soup, _CSDN_CONTENT_SELECTORS) if is_csdn else None

        title = self._extract_title(soup)
        paragraphs = self._extract_paragraphs(scope or soup)
        if is_csdn:
            paragraphs = self._dedup_nested_paragraphs(paragraphs)
        images = self._extract_images(scope or soup, url)
        metadata = self._extract_metadata(soup)

        return {
            "title": title,
            "content_type": "article",
            "raw_text": "\n\n".join(paragraphs),
            "paragraphs": paragraphs,
            "images": images,
            "metadata": metadata,
        }

    @staticmethod
    def _find_content_scope(soup: BeautifulSoup, selectors: list[str]):
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                return el
        return None

    @staticmethod
    def _dedup_nested_paragraphs(paragraphs: list[str]) -> list[str]:
        """Remove paragraphs whose text is a substring of a longer paragraph."""
        sorted_by_len = sorted(paragraphs, key=len, reverse=True)
        kept: list[str] = []
        for p in sorted_by_len:
            if not any(p in longer for longer in kept):
                kept.append(p)
        kept.sort(key=lambda p: paragraphs.index(p))
        return kept

    # ------------------------------------------------------------------

    @staticmethod
    def _remove_noise(soup: BeautifulSoup):
        for tag in soup.find_all(_NOISE_TAGS):
            tag.decompose()
        for sel in _NOISE_SELECTORS:
            for el in soup.select(sel):
                el.decompose()
        for el in soup.find_all(attrs={"role": True}):
            if (el.get("role") or "").lower() in _NOISE_ROLES:
                el.decompose()

    @staticmethod
    def _extract_title(soup: BeautifulSoup) -> str:
        for sel in ("h1", "title", 'meta[property="og:title"]'):
            el = soup.select_one(sel)
            if el:
                return el.get_text(strip=True) if el.name != "meta" else (el.get("content") or "")
        return ""

    @staticmethod
    def _extract_paragraphs(soup: BeautifulSoup) -> list[str]:
        candidates = soup.find_all(["p", "div", "article", "section"])
        paragraphs: list[str] = []
        seen: set[str] = set()
        for el in candidates:
            text = el.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) >= 20 and text not in seen and not is_noise_paragraph(text):
                seen.add(text)
                paragraphs.append(text)
        return paragraphs

    @staticmethod
    def _extract_images(soup: BeautifulSoup, base_url: str) -> list[dict]:
        images: list[dict] = []
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if not src or src.startswith("data:"):
                continue
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                from urllib.parse import urljoin
                src = urljoin(base_url, src)
            images.append({
                "url": src,
                "alt": img.get("alt", ""),
            })
        return images

    @staticmethod
    def _extract_metadata(soup: BeautifulSoup) -> dict:
        meta: dict = {}
        for tag in soup.find_all("meta"):
            prop = tag.get("property") or tag.get("name") or ""
            content = tag.get("content", "")
            if "author" in prop.lower():
                meta["author"] = content
            elif "time" in prop.lower() or "date" in prop.lower():
                meta["publish_time"] = content
            elif "description" in prop.lower():
                meta["description"] = content
        return meta
