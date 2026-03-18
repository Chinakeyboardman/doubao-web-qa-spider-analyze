"""Common utility functions shared across the QA pipeline."""

from __future__ import annotations

import json
import re

_VIDEO_ID_RE = re.compile(r"/video/(\d+)")


def to_raw_dict(raw_json: dict | str | None) -> dict:
    """Safely convert raw_json (dict, JSON string, or None) to a dict."""
    if raw_json is None:
        return {}
    if isinstance(raw_json, dict):
        return raw_json
    if isinstance(raw_json, str):
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError:
            return {}
    return {}


def has_meaningful_subtitles(raw: dict) -> bool:
    """Check whether raw content contains non-empty subtitle entries."""
    subtitles = raw.get("subtitles") or []
    return any(
        isinstance(item, dict) and (item.get("text") or "").strip()
        for item in subtitles
    )


def extract_video_id_from_url(url: str) -> str:
    """Extract video_id (aweme_id) from a Douyin share URL."""
    m = _VIDEO_ID_RE.search(url or "")
    return m.group(1) if m else ""


def resolve_video_id(content: dict, link_url: str) -> str:
    """Try to get video_id from content dict first, then from the URL."""
    vid = (content.get("video_info") or {}).get("aweme_id", "")
    if vid:
        return str(vid)
    inner = content.get("结构化内容") or {}
    vid = (inner.get("video_info") or {}).get("aweme_id", "")
    if vid:
        return str(vid)
    return extract_video_id_from_url(link_url)
