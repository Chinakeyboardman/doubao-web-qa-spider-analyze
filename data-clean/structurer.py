"""Transform raw crawled content into the standardised JSON formats
defined in docs/大规模qa数据获取.md 第二节表格4（格式A/B/C/D）.

- 格式A：图文A，图文无序（抖音/小红书）
- 格式B：图文B，图文有序（媒体网站/什么值得买），含 标题/内容流/小标题/元数据
- 格式C：视频-有字幕，含 字幕内容/视频章节/评论(含UP主回复)/元数据
- 格式D：视频-无字幕，含 语音转文本/STT处理信息/评论，元数据可为空
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ContentStructurer:
    """Converts a raw_content dict (from a crawler) into the canonical JSON schema."""

    def structure(self, raw_content: dict, content_format: str, link_id: str = "") -> dict:
        """Dispatch to the appropriate formatter."""
        formatters = {
            "图文A": self._format_a_image_text_unordered,
            "图文B": self._format_b_image_text_ordered,
            "视频-有字幕": self._format_c_video_with_subtitle,
            "视频-无字幕": self._format_d_video_without_subtitle,
        }
        fn = formatters.get(content_format)
        if fn is None:
            logger.warning("Unknown content_format %r for %s, using 图文B fallback", content_format, link_id)
            fn = self._format_b_image_text_ordered

        result = fn(raw_content, link_id)
        result["链接ID"] = link_id
        result["内容格式"] = content_format
        return result

    # ------------------------------------------------------------------
    # Format A: 图文无序 (小红书/抖音图文)
    # ------------------------------------------------------------------
    @staticmethod
    def _format_a_image_text_unordered(raw: dict, link_id: str) -> dict:
        title_text = raw.get("title", "")
        body = raw.get("raw_text", "")
        paragraphs = raw.get("paragraphs", [body] if body else [])

        char_pos = 0
        formatted_paragraphs: list[dict] = []
        for idx, p in enumerate(paragraphs, 1):
            end = char_pos + len(p)
            formatted_paragraphs.append({
                "段落序号": idx,
                "内容": p,
                "字符位置": f"{char_pos}-{end}",
                "字符数": len(p),
            })
            char_pos = end

        images: list[dict] = []
        for idx, img in enumerate(raw.get("images", []), 1):
            images.append({
                "序号": idx,
                "图片URL": img.get("url", ""),
                "图片描述": img.get("alt") or img.get("description", ""),
                "图片主题": img.get("topic", ""),
                "位置关系": "独立（与正文无顺序）",
            })

        comments: list[dict] = []
        for idx, c in enumerate(raw.get("comments", []), 1):
            comments.append({
                "序号": idx,
                "内容": c.get("text", ""),
                "点赞数": c.get("digg_count") or c.get("liked_count", 0),
                "发布时间": c.get("create_time", ""),
            })

        meta = raw.get("metadata", {})
        return {
            "结构化内容": {
                "标题": {"文本": title_text, "字符位置": f"0-{len(title_text)}"},
                "正文": formatted_paragraphs,
                "图片": images,
                "标签": raw.get("tags", []),
                "评论": comments,
            },
            "元数据": {
                "作者": meta.get("author", ""),
                "作者类型": meta.get("author_type", ""),
                "发布时间": meta.get("publish_time", ""),
                "点赞数": meta.get("liked_count") or meta.get("digg_count", 0),
                "评论数": meta.get("comment_count", 0),
                "收藏数": meta.get("collected_count", 0),
            },
        }

    # ------------------------------------------------------------------
    # Format B: 图文有序 (知乎/什么值得买/通用网页)
    # ------------------------------------------------------------------
    @staticmethod
    def _format_b_image_text_ordered(raw: dict, link_id: str) -> dict:
        title_text = raw.get("title", "")
        paragraphs = raw.get("paragraphs", [])
        images = raw.get("images", [])

        content_flow: list[dict] = []
        seq = 0
        char_pos = 0

        for p in paragraphs:
            seq += 1
            end = char_pos + len(p)
            content_flow.append({
                "序号": seq,
                "类型": "文本",
                "内容": p,
                "字符位置": f"{char_pos}-{end}",
            })
            char_pos = end

        for img in images:
            seq += 1
            content_flow.append({
                "序号": seq,
                "类型": "图片",
                "图片URL": img.get("url", ""),
                "图片描述": img.get("alt") or img.get("description", ""),
                "图片主题": img.get("topic", ""),
            })

        meta = raw.get("metadata", {})
        return {
            "结构化内容": {
                "标题": {"主标题": title_text, "副标题": ""},
                "内容流": content_flow,
                "小标题": [],
            },
            "元数据": {
                "来源网站": raw.get("platform", ""),
                "作者": meta.get("author", ""),
                "发布时间": meta.get("publish_time", ""),
                "阅读量": meta.get("view_count", 0),
                "点赞数": meta.get("like_count", 0),
            },
        }

    # ------------------------------------------------------------------
    # Format C: 视频-有字幕
    # ------------------------------------------------------------------
    @staticmethod
    def _format_c_video_with_subtitle(raw: dict, link_id: str) -> dict:
        video_info = raw.get("video_info", {})
        meta = raw.get("metadata", {})
        stt_text = (raw.get("stt_text") or "").strip()
        video_desc = _video_desc_with_fallback(raw, stt_text)

        subtitle_entries: list[dict] = []
        for idx, s in enumerate(raw.get("subtitles", []), 1):
            subtitle_entries.append({
                "序号": idx,
                "时间轴": s.get("start_time", ""),
                "字幕文本": s.get("text", ""),
                "字符数": len(s.get("text", "")),
            })
        if not subtitle_entries and stt_text:
            subtitle_entries = _stt_to_subtitle_entries(stt_text)

        comments: list[dict] = []
        for idx, c in enumerate(raw.get("comments", []), 1):
            comments.append({
                "序号": idx,
                "内容": c.get("text", ""),
                "点赞数": c.get("digg_count", 0),
                "UP主回复": c.get("reply", ""),
            })

        video_chapters: list[dict] = []
        for ch in raw.get("chapters", []):
            video_chapters.append({
                "标题": ch.get("title", ""),
                "时间范围": ch.get("time_range", ""),
            })

        return {
            "结构化内容": {
                "视频标题": raw.get("title", ""),
                "视频简介": video_desc,
                "视频时长": _format_duration(video_info.get("duration", 0)),
                "字幕内容": subtitle_entries,
                "视频章节": video_chapters,
                "评论": comments,
            },
            "元数据": {
                "UP主": meta.get("author", ""),
                "发布时间": meta.get("publish_time", ""),
                "播放量": meta.get("play_count") or meta.get("view_count", 0),
                "点赞数": meta.get("digg_count") or meta.get("like_count", 0),
                "投币数": meta.get("coin_count", 0),
            },
        }

    # ------------------------------------------------------------------
    # Format D: 视频-无字幕 (needs STT later)
    # ------------------------------------------------------------------
    @staticmethod
    def _format_d_video_without_subtitle(raw: dict, link_id: str) -> dict:
        video_info = raw.get("video_info", {})
        meta = raw.get("metadata", {})
        stt_text = (raw.get("stt_text") or "").strip()
        audio_info = raw.get("audio_info", {}) or {}
        video_desc = _video_desc_with_fallback(raw, stt_text)
        stt_lines = _split_stt_text(stt_text)

        return {
            "结构化内容": {
                "视频标题": raw.get("title", ""),
                "视频简介": video_desc,
                "视频时长": _format_duration(video_info.get("duration", 0)),
                "语音转文本": [
                    {
                        "序号": idx + 1,
                        "时间轴": "",
                        "文本": line,
                    }
                    for idx, line in enumerate(stt_lines)
                ],
                "STT处理信息": {
                    "处理时间": audio_info.get("processed_at", ""),
                    "使用模型": audio_info.get("transcript_model", ""),
                    "总置信度": 0.8 if stt_lines else 0,
                },
                "评论": [],
            },
            "元数据": {},
        }


def _format_duration(seconds: int | float) -> str:
    """Convert seconds to MM:SS or HH:MM:SS string."""
    seconds = int(seconds)
    if seconds <= 0:
        return "00:00"
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _video_desc_with_fallback(raw: dict, stt_text: str) -> str:
    """整合 raw_text 兜底：视频简介取 raw_text/caption/stt_text 中长度最长的。"""
    candidates = [
        (raw.get("raw_text") or "").strip(),
        (raw.get("caption") or "").strip(),
        (stt_text or "").strip(),
    ]
    return max(candidates, key=len)


def _split_stt_text(text: str, max_len: int = 80) -> list[str]:
    """Split long STT text into readable chunks for structured output."""
    t = (text or "").strip()
    if not t:
        return []
    chunks: list[str] = []
    for block in t.replace("。", "。\n").replace("！", "！\n").replace("？", "？\n").splitlines():
        block = block.strip()
        if not block:
            continue
        if len(block) <= max_len:
            chunks.append(block)
            continue
        for i in range(0, len(block), max_len):
            part = block[i:i + max_len].strip()
            if part:
                chunks.append(part)
    return chunks


def _stt_to_subtitle_entries(stt_text: str) -> list[dict]:
    """Convert plain STT text to Format C subtitle entries."""
    lines = _split_stt_text(stt_text)
    return [
        {
            "序号": idx + 1,
            "时间轴": "",
            "字幕文本": line,
            "字符数": len(line),
        }
        for idx, line in enumerate(lines)
    ]


def structured_to_raw(structured: dict, content_format: str) -> dict:
    """将已结构化的 content_json 转回 structurer 所需的 raw 形态，便于重新按文档规范生成。"""
    inner = structured.get("结构化内容") or {}
    meta = structured.get("元数据") or {}

    if content_format == "图文A":
        title = (inner.get("标题") or {}).get("文本", "")
        body_list = inner.get("正文") or []
        paragraphs = [b.get("内容", "") for b in body_list]
        images = []
        for img in inner.get("图片") or []:
            images.append({
                "url": img.get("图片URL", ""),
                "alt": img.get("图片描述", ""),
                "description": img.get("图片描述", ""),
                "topic": img.get("图片主题", ""),
            })
        comments = []
        for c in inner.get("评论") or []:
            comments.append({
                "text": c.get("内容", ""),
                "digg_count": c.get("点赞数", 0),
                "create_time": c.get("发布时间", ""),
            })
        return {
            "title": title,
            "raw_text": " ".join(paragraphs),
            "paragraphs": paragraphs,
            "images": images,
            "tags": inner.get("标签") or [],
            "comments": comments,
            "metadata": {
                "author": meta.get("作者", ""),
                "author_type": meta.get("作者类型", ""),
                "publish_time": meta.get("发布时间", ""),
                "liked_count": meta.get("点赞数", 0),
                "comment_count": meta.get("评论数", 0),
                "collected_count": meta.get("收藏数", 0),
            },
        }

    if content_format == "图文B":
        title = (inner.get("标题") or {}).get("主标题", "")
        flow = inner.get("内容流") or []
        paragraphs = [x.get("内容", "") for x in flow if x.get("类型") == "文本"]
        images = []
        for x in flow:
            if x.get("类型") == "图片":
                images.append({
                    "url": x.get("图片URL", ""),
                    "alt": x.get("图片描述", ""),
                    "topic": x.get("图片主题", ""),
                })
        return {
            "title": title,
            "paragraphs": paragraphs,
            "images": images,
            "platform": meta.get("来源网站", ""),
            "metadata": {
                "author": meta.get("作者", ""),
                "publish_time": meta.get("发布时间", ""),
                "view_count": meta.get("阅读量", 0),
                "like_count": meta.get("点赞数", 0),
            },
        }

    if content_format == "视频-有字幕":
        subtitles = []
        for s in inner.get("字幕内容") or []:
            subtitles.append({"start_time": s.get("时间轴", ""), "text": s.get("字幕文本", "")})
        comments = []
        for c in inner.get("评论") or []:
            comments.append({
                "text": c.get("内容", ""),
                "digg_count": c.get("点赞数", 0),
                "reply": c.get("UP主回复", ""),
            })
        chapters = []
        for ch in inner.get("视频章节") or []:
            chapters.append({"title": ch.get("标题", ""), "time_range": ch.get("时间范围", "")})
        return {
            "title": inner.get("视频标题", ""),
            "raw_text": inner.get("视频简介", ""),
            "video_info": {},
            "subtitles": subtitles,
            "comments": comments,
            "chapters": chapters,
            "metadata": {
                "author": meta.get("UP主", ""),
                "publish_time": meta.get("发布时间", ""),
                "play_count": meta.get("播放量", 0),
                "digg_count": meta.get("点赞数", 0),
                "coin_count": meta.get("投币数", 0),
            },
        }

    if content_format == "视频-无字幕":
        stt_items = inner.get("语音转文本") or []
        stt_text = "\n".join(
            (item.get("文本") or "").strip()
            for item in stt_items
            if isinstance(item, dict) and (item.get("文本") or "").strip()
        ).strip()
        stt_info = inner.get("STT处理信息") or {}
        return {
            "title": inner.get("视频标题", ""),
            "raw_text": inner.get("视频简介", ""),
            "video_info": {},
            "subtitles": [],
            "comments": [],
            "stt_text": stt_text,
            "audio_info": {
                "transcript_model": stt_info.get("使用模型", ""),
                "processed_at": stt_info.get("处理时间", ""),
            },
            "metadata": {},
        }

    return {"title": "", "paragraphs": [], "images": [], "metadata": meta}
