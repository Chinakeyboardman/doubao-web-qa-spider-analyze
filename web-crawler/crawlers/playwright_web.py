"""Playwright-based web crawler for JS-rendered pages.

Used for platforms that require JavaScript execution to render content
(e.g. 头条/m.toutiao.com, 什么值得买/smzdm.com).
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from .base import BaseCrawler

logger = logging.getLogger(__name__)

_NOISE_TAGS = {"script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"}
_PW_TIMEOUT = 30_000

_CONTENT_SELECTORS = ["article", "main", ".article-content", "#article", ".post-content"]


class PlaywrightWebCrawler(BaseCrawler):
    platform = "通用-JS"
    min_interval = 2.0

    def __init__(self):
        super().__init__()
        self._pw = None
        self._browser = None

    async def _ensure_browser(self):
        if self._browser and self._browser.is_connected():
            return
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

    async def crawl(self, url: str) -> dict:
        await self._ensure_browser()
        context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        try:
            from playwright_stealth import stealth_async
        except ImportError:
            stealth_async = None

        page = await context.new_page()
        if stealth_async:
            await stealth_async(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=_PW_TIMEOUT)
            await self._wait_for_content(page)
            html = await page.content()
        finally:
            await page.close()
            await context.close()

        soup = BeautifulSoup(html, "lxml")
        self._remove_noise(soup)

        title = self._extract_title(soup)
        paragraphs = self._extract_paragraphs(soup)
        paragraphs = self._dedup_nested_paragraphs(paragraphs)
        images = self._extract_images(soup, url)
        metadata = self._extract_metadata(soup)

        return {
            "title": title,
            "content_type": "article",
            "raw_text": "\n\n".join(paragraphs),
            "paragraphs": paragraphs,
            "images": images,
            "metadata": metadata,
        }

    async def _wait_for_content(self, page):
        """Wait for meaningful content to appear on page."""
        for sel in _CONTENT_SELECTORS:
            try:
                await page.wait_for_selector(sel, timeout=5000)
                return
            except Exception:
                continue
        await page.wait_for_timeout(5000)

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

    @staticmethod
    def _remove_noise(soup: BeautifulSoup):
        for tag in soup.find_all(_NOISE_TAGS):
            tag.decompose()

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
            if len(text) >= 20 and text not in seen:
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
