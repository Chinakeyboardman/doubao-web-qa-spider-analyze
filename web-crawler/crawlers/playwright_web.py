"""Playwright-based web crawler for JS-rendered pages.

Used for platforms that require JavaScript execution to render content
(e.g. 头条/m.toutiao.com, 什么值得买/smzdm.com).

什么值得买：www.smzdm.com/p/ 常为 404，规范到 post.smzdm.com。
腾讯 WAF 滑块验证码为风控策略，只在请求频率过高时触发；正常速率 + stealth 不会拦截。
命中 WAF 时等几秒用全新 context 重试，不做复杂的验证码求解。
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from urllib.parse import urlparse, urlunparse

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
_PW_TIMEOUT = 30_000
_SMZDM_WAF_MAX_RETRIES = 3
_SMZDM_WAF_BACKOFF = (4, 8, 15)

_CONTENT_SELECTORS = [
    "article",
    "main",
    ".article-content",
    "#article",
    ".post-content",
    "#article article",
    ".article__content",
]

_SMZDM_WAF_MARKERS = (
    "WafCaptcha",
    "TencentCaptcha",
    "ssl.captcha.qq.com",
    "TCaptcha.js",
    "__captcha",
    "尝试太多了",
)
_SMZDM_BLOCK_MARKERS = (
    "安全验证",
    "请完成验证",
    "人机验证",
    "访问过于频繁",
    "拖动滑块",
    "请点击按钮进行验证",
    "验证失败",
)


class PlaywrightWebCrawler(BaseCrawler):
    platform = "通用-JS"
    min_interval = 2.0
    # 什么值得买专用：更长间隔防风控
    _smzdm_min_interval = 5.0

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

    @staticmethod
    def _normalize_smzdm_url(url: str) -> str:
        """委托 citation_parser.normalize_url（统一规则，入库与爬取一致）。"""
        try:
            from integration.citation_parser import normalize_url
            return normalize_url(url)
        except ImportError:
            return url

    @staticmethod
    def _is_smzdm(url: str) -> bool:
        try:
            return "smzdm.com" in (urlparse(url).netloc or "").lower()
        except Exception:
            return False

    async def crawl(self, url: str) -> dict:
        crawl_url = self._normalize_smzdm_url(url)
        if crawl_url != url:
            logger.info("SMZDM URL normalized: %s -> %s", url[:80], crawl_url[:80])

        is_smzdm = self._is_smzdm(crawl_url)
        max_attempts = _SMZDM_WAF_MAX_RETRIES if is_smzdm else 1

        # 什么值得买单独限速防风控
        if is_smzdm:
            import time as _time
            elapsed = _time.monotonic() - self._last_request_time
            extra = self._smzdm_min_interval - elapsed
            if extra > 0:
                await asyncio.sleep(extra)

        await self._ensure_browser()
        last_block: str | None = None

        for attempt in range(max_attempts):
            if attempt > 0:
                wait = _SMZDM_WAF_BACKOFF[min(attempt - 1, len(_SMZDM_WAF_BACKOFF) - 1)]
                wait += random.random() * 3
                logger.info("[什么值得买] WAF retry %d/%d, backoff %.1fs", attempt + 1, max_attempts, wait)
                await asyncio.sleep(wait)

            result = await self._single_page_fetch(crawl_url, is_smzdm)
            block = result.pop("__block", None)

            if not block:
                return result
            last_block = block
            logger.warning("[什么值得买] blocked (%s) attempt %d/%d: %s", block, attempt + 1, max_attempts, crawl_url[:60])

        return {
            "url": crawl_url,
            "platform": self.platform,
            "error": last_block,
            "title": "",
            "content_type": "article",
            "raw_text": "",
            "paragraphs": [],
            "images": [],
            "metadata": {},
        }

    _DESKTOP_UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    _MOBILE_UA = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    )

    async def _single_page_fetch(self, crawl_url: str, is_smzdm: bool) -> dict:
        """One attempt: fresh context → goto → parse. Returns result dict with optional __block key."""
        try:
            host = (urlparse(crawl_url).netloc or "").lower()
        except Exception:
            host = ""
        # post.m.smzdm.com 数字 ID 文章只在移动端可访问，桌面 UA 会 302→404
        is_mobile_host = "m." in host.split("smzdm")[0] if "smzdm" in host else host.startswith("m.")
        if is_mobile_host:
            viewport = {"width": 390, "height": 844}
            ua = self._MOBILE_UA
        else:
            viewport = {"width": 1280, "height": 800}
            ua = self._DESKTOP_UA
        context = await self._browser.new_context(
            user_agent=ua,
            locale="zh-CN",
            viewport=viewport,
        )
        try:
            from playwright_stealth import Stealth
            stealth = Stealth(
                navigator_platform_override="MacIntel",
                navigator_languages_override=("zh-CN", "zh", "en"),
            )
            await stealth.apply_stealth_async(context)
        except ImportError:
            logger.warning("playwright_stealth not installed; crawling without stealth")

        page = await context.new_page()
        try:
            await page.goto(crawl_url, wait_until="domcontentloaded", timeout=_PW_TIMEOUT)
            # 快速检查 WAF（2 秒），命中就立即退出，不浪费时间等内容选择器
            await page.wait_for_timeout(2000)
            if is_smzdm:
                quick_html = await page.content()
                block = self._detect_smzdm_block(quick_html, await page.title())
                if block:
                    return {"__block": block}
            await self._wait_for_content(page)
            if is_smzdm:
                await self._scroll_lazy_article(page)
            html = await page.content()
            title = await page.title()
        finally:
            await page.close()
            await context.close()

        soup = BeautifulSoup(html, "lxml")
        self._remove_noise(soup)

        title = self._extract_title(soup)
        paragraphs = self._extract_paragraphs(soup)
        paragraphs = self._dedup_nested_paragraphs(paragraphs)
        images = self._extract_images(soup, crawl_url)
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
        """Wait for any content selector (parallel race) or fallback 3s."""
        combined = ", ".join(_CONTENT_SELECTORS)
        try:
            await page.wait_for_selector(combined, timeout=8000)
        except Exception:
            await page.wait_for_timeout(3000)

    @staticmethod
    async def _scroll_lazy_article(page):
        try:
            await page.evaluate(
                """async () => {
                    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
                    const maxY = Math.min(document.body.scrollHeight || 6000, 12000);
                    for (let y = 0; y < maxY; y += 1000) {
                        window.scrollTo(0, y);
                        await sleep(120);
                    }
                    window.scrollTo(0, 0);
                }"""
            )
        except Exception:
            pass
        await page.wait_for_timeout(800)

    @classmethod
    def _detect_smzdm_block(cls, html: str, title: str) -> str | None:
        blob = f"{title or ''}\n{html[:20000]}"
        for m in _SMZDM_WAF_MARKERS:
            if m in blob:
                return "smzdm_blocked:tencent_waf"
        for m in _SMZDM_BLOCK_MARKERS:
            if m in blob:
                return f"smzdm_blocked:{m}"
        low = (html or "").lower()
        if "smzdm.com" in low and ("404" in (title or "") or (title or "").strip() == "404"):
            return "smzdm_blocked:404"
        return None

    @staticmethod
    def _dedup_nested_paragraphs(paragraphs: list[str]) -> list[str]:
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
