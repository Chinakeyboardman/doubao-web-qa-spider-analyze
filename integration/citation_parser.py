"""Parse citations from Volcengine API response and classify links by platform.

根据 URL 域名识别平台（抖音/B站/小红书/知乎/什么值得买/淘宝/京东/微博/头条/百度/其他），
后续不同链接走不同解析方式见 integration/parsing_routing.py 与 docs/PARSING_ROUTING.md。

URL 规范化在此处完成（识别阶段），入库即为可爬域名，爬虫不再需要额外处理。
"""

from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse


PLATFORM_RULES: list[tuple[list[str], str, str]] = [
    # (domain keywords, platform_name, default_content_format)
    (["xiaohongshu.com", "xhslink.com"], "小红书", "图文A"),
    (["douyin.com", "iesdouyin.com"], "抖音", "图文A"),
    (["zhihu.com"], "知乎", "图文B"),
    (["smzdm.com", "zdm.cn"], "什么值得买", "图文B"),
    (["bilibili.com", "b23.tv"], "B站", "视频-有字幕"),
    (["taobao.com", "tmall.com", "tb.cn"], "淘宝", "商品页"),
    (["jd.com", "jd.hk"], "京东", "商品页"),
    (["weibo.com"], "微博", "图文A"),
    (["csdn.net"], "CSDN", "图文B"),
    (["toutiao.com", "toutiaoimg.com"], "头条", "图文B"),
    (["baidu.com", "baijiahao.baidu.com"], "百度", "图文B"),
]

_DOUYIN_VIDEO_PATTERNS = re.compile(r"/video/|/share/video/", re.I)

# ---------------------------------------------------------------------------
# URL 规范化规则（移动端 → 桌面端、错误域名修正）
# 规则在 identify_platform / parse_citations 前执行，入库即为可爬域名。
# ---------------------------------------------------------------------------
_URL_NORMALIZE_RULES: list[tuple[str, str, str | None]] = [
    # (原 host, 目标 host, 路径须含（None=任意）)
    # 什么值得买：m.smzdm.com 移动端 → www（移动端晒物页 100% WAF）
    ("m.smzdm.com", "www.smzdm.com", None),
    # 什么值得买：www.smzdm.com/p/… 文章页走 post.smzdm.com（www 常 404）
    ("www.smzdm.com", "post.smzdm.com", "/p/"),
    # 头条移动端 → 桌面端
    ("m.toutiao.com", "www.toutiao.com", None),
]


def normalize_url(url: str) -> str:
    """将已知移动端 / 错误域名规范到可爬的桌面端域名。"""
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        path = p.path or ""
        for src_host, dst_host, path_contains in _URL_NORMALIZE_RULES:
            if host == src_host:
                if path_contains is None or path_contains in path:
                    return urlunparse((p.scheme, dst_host, path, p.params, p.query, p.fragment))
    except Exception:
        pass
    return url


def identify_platform(url: str) -> str:
    """Map a URL to a platform name."""
    host = urlparse(url).netloc.lower()
    for domains, platform, _ in PLATFORM_RULES:
        if any(d in host for d in domains):
            return platform
    return "其他"


def determine_content_format(url: str, platform: str) -> str:
    """Determine content_format from URL + platform."""
    if platform == "抖音":
        return "视频-有字幕" if _DOUYIN_VIDEO_PATTERNS.search(url) else "图文A"
    for _, plat, fmt in PLATFORM_RULES:
        if plat == platform:
            return fmt
    return "图文B"


def parse_citations(api_response) -> list[dict]:
    """Extract citations from a Volcengine chat completion response.

    The API may return web_search_results in different locations depending on
    the model version.  We try several known paths.

    Returns a list of dicts: {url, title, summary, platform, content_format}
    """
    results: list[dict] = []
    seen_urls: set[str] = set()

    raw_refs = _extract_raw_references(api_response)

    for ref in raw_refs:
        url = (ref.get("url") or ref.get("link") or "").strip()
        if not url or url in seen_urls:
            continue
        url = normalize_url(url)
        if url in seen_urls:
            continue
        seen_urls.add(url)

        platform = identify_platform(url)
        content_format = determine_content_format(url, platform)

        results.append({
            "url": url,
            "title": ref.get("title", ""),
            "summary": ref.get("summary") or ref.get("snippet", ""),
            "platform": platform,
            "content_format": content_format,
        })

    return results


def _extract_raw_references(api_response) -> list[dict]:
    """Walk the API response object to find web search references."""
    refs: list[dict] = []

    # Path 1: choice.message.tool_calls containing web_search results
    for choice in getattr(api_response, "choices", []):
        msg = choice.message
        # Check tool_calls
        for tc in (msg.tool_calls or []):
            if tc.type == "web_search" or (tc.function and tc.function.name == "web_search"):
                _try_parse_json_refs(tc, refs)

    # Path 2: model_extra / metadata on the response or message
    for choice in getattr(api_response, "choices", []):
        msg = choice.message
        _collect_from_extra(getattr(msg, "model_extra", None) or {}, refs)

    _collect_from_extra(getattr(api_response, "model_extra", None) or {}, refs)

    # Path 3: look for inline citation URLs in the answer text (fallback)
    if not refs:
        for choice in getattr(api_response, "choices", []):
            text = choice.message.content or ""
            refs.extend(_extract_urls_from_text(text))

    return refs


def _try_parse_json_refs(tool_call, refs: list[dict]):
    """Try to parse JSON from a tool_call's arguments or output."""
    import json

    for attr in ("arguments", "output"):
        raw = None
        if hasattr(tool_call, attr):
            raw = getattr(tool_call, attr)
        elif hasattr(tool_call, "function") and hasattr(tool_call.function, attr):
            raw = getattr(tool_call.function, attr)
        if not raw:
            continue
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data, list):
            refs.extend(data)
        elif isinstance(data, dict):
            for key in ("results", "search_results", "references", "web_search_results"):
                if key in data and isinstance(data[key], list):
                    refs.extend(data[key])
                    return
            if "url" in data or "link" in data:
                refs.append(data)


def _collect_from_extra(extra: dict, refs: list[dict]):
    """Collect references from model_extra metadata dicts."""
    for key in ("web_search", "web_search_results", "search_results", "references", "citations"):
        val = extra.get(key)
        if isinstance(val, list):
            refs.extend(val)
        elif isinstance(val, dict) and "results" in val:
            refs.extend(val["results"])


_URL_RE = re.compile(r"https?://[^\s\]\)\"'>]+")


def _extract_urls_from_text(text: str) -> list[dict]:
    """Fallback: extract raw URLs found in the answer text."""
    results = []
    for url in _URL_RE.findall(text):
        url = url.rstrip(".,;:!?")
        results.append({"url": url, "title": "", "summary": ""})
    return results
