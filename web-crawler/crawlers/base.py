"""Base crawler with retry and rate-limiting support."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path

import httpx

# 可选加载配置（跳过 SSL 校验以访问“不安全”网站）
try:
    import sys
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from shared.config import CONFIG
    _verify_ssl = CONFIG.get("crawler", {}).get("verify_ssl", True)
except Exception:
    _verify_ssl = True

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


class BaseCrawler(ABC):
    """Abstract base for platform crawlers."""

    platform: str = "unknown"
    min_interval: float = 1.0  # seconds between requests to same domain

    def __init__(self):
        self._last_request_time: float = 0

    @abstractmethod
    async def crawl(self, url: str) -> dict:
        """Fetch and return raw content dict for the given URL.

        The dict should at minimum contain:
          - url: str
          - title: str
          - content_type: str (e.g. "article", "video")
          - raw_text: str (main textual content)
          - images: list[dict] (each with url, alt, etc.)
          - metadata: dict (author, publish_time, stats, etc.)
        """

    async def crawl_with_retry(self, url: str, max_retries: int = 3) -> dict:
        """Crawl with exponential-backoff retries and rate limiting."""
        await self._rate_limit()

        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                result = await self.crawl(url)
                result.setdefault("url", url)
                result.setdefault("platform", self.platform)
                return result
            except Exception as exc:
                last_exc = exc
                if not _should_retry(exc):
                    logger.warning(
                        "[%s] non-retriable failure for %s: %s",
                        self.platform,
                        url,
                        exc,
                    )
                    break
                wait = 2 ** attempt
                logger.warning(
                    "[%s] attempt %d/%d failed for %s: %s — retrying in %ds",
                    self.platform, attempt, max_retries, url, exc, wait,
                )
                await asyncio.sleep(wait)

        logger.error("[%s] all %d attempts failed for %s", self.platform, max_retries, url)
        return {
            "url": url,
            "platform": self.platform,
            "error": str(last_exc),
            "title": "",
            "content_type": "error",
            "raw_text": "",
            "images": [],
            "metadata": {},
        }

    async def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self._last_request_time = time.monotonic()

    @classmethod
    def _get_async_client(cls, **kwargs) -> httpx.AsyncClient:
        defaults = {
            "headers": DEFAULT_HEADERS,
            "timeout": 30.0,
            "follow_redirects": True,
            "verify": _verify_ssl,
        }
        defaults.update(kwargs)
        return httpx.AsyncClient(**defaults)


def _should_retry(exc: Exception) -> bool:
    """Return whether a crawl exception is worth retrying.

    Heuristics:
    - HTTP 4xx are usually deterministic (except 408/429), do not retry.
    - SSL certificate failures are deterministic, do not retry.
    - Other network/server errors remain retriable.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code if exc.response else None
        if status is not None and 400 <= status < 500 and status not in (408, 429):
            return False
        return True

    text = str(exc).lower()
    if "certificate_verify_failed" in text:
        return False
    if "hostname mismatch" in text:
        return False
    return True
