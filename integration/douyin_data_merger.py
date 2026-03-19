"""Merge data from douyin_videos / douyin_comments into qa_link_content.

douyin-crawler (Node.js/Puppeteer) 已在同一 PG 实例中采集了抖音视频和评论，
但 qa_link_content 中的抖音数据来自 8080 API，可能不完整甚至为空。
本模块通过 video_id 关联两套数据，将 douyin_videos + douyin_comments 的字段
补全到 qa_link_content.content_json 中。
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import execute, fetch_all, fetch_one
from shared.sql_builder import sb
from shared.utils import extract_video_id_from_url, resolve_video_id

logger = logging.getLogger(__name__)

_LAST_ZERO_ENRICH_LOG: list[float] = [0.0]
_ZERO_ENRICH_INTERVAL = 10.0


class DouyinDataMerger:
    """Enrich qa_link_content with data from douyin_videos / douyin_comments."""

    def merge_all(self, *, link_ids: list[str] | None = None, force: bool = False) -> int:
        """Scan Douyin links in qa_link and enrich their qa_link_content rows.

        Only processes links that haven't been enriched yet (no raw_json),
        unless force=True which re-processes all.

        Returns number of rows updated.
        """
        not_enriched_filter = "" if force else " AND (lc.link_id IS NULL OR lc.raw_json IS NULL)"
        if link_ids:
            any_frag, any_params = sb.expand_any("l.link_id", link_ids)
            rows = fetch_all(
                "SELECT l.link_id, l.link_url, l.updated_at AS link_updated_at, lc.raw_json, lc.content_json "
                "FROM qa_link l "
                "LEFT JOIN qa_link_content lc ON l.link_id = lc.link_id "
                f"WHERE l.platform = '抖音' AND {any_frag}{not_enriched_filter}",
                any_params,
            )
        else:
            rows = fetch_all(
                "SELECT l.link_id, l.link_url, l.updated_at AS link_updated_at, lc.raw_json, lc.content_json "
                "FROM qa_link l "
                "LEFT JOIN qa_link_content lc ON l.link_id = lc.link_id "
                f"WHERE l.platform = '抖音'{not_enriched_filter}"
            )
        if not rows:
            logger.info("No Douyin links found in qa_link.")
            return 0

        updated = 0
        skipped_downgrade = 0
        for row in rows:
            link_id = row["link_id"]
            link_url = row["link_url"] or ""
            content = row.get("raw_json") or row.get("content_json")

            if isinstance(content, str):
                content = json.loads(content)
            if content is None:
                content = {}

            video_id = self._resolve_video_id(content, link_url)
            if not video_id:
                logger.warning("Cannot resolve video_id for %s (%s)", link_id, link_url[:80])
                continue

            video_row = fetch_one(
                "SELECT * FROM douyin_videos WHERE video_id = %s", (video_id,)
            )
            if not video_row:
                logger.debug("video_id %s not found in douyin_videos (link %s)", video_id, link_id)
                continue

            comments_rows = fetch_all(
                "SELECT username, content, time, location, likes "
                "FROM douyin_comments WHERE video_id = %s ORDER BY likes DESC",
                (video_id,),
            )

            merged = self._merge_content(content, video_row, comments_rows, link_id)
            if not _is_meaningful_content(merged):
                logger.warning("Skip %s: merged content looks empty", link_id)
                continue

            old_score = _content_quality_score(content)
            new_score = _content_quality_score(merged)
            has_existing = (row.get("raw_json") or row.get("content_json")) is not None
            if has_existing and new_score < old_score:
                skipped_downgrade += 1
                logger.info(
                    "Skip overwrite for %s: new score %d < old score %d",
                    link_id,
                    new_score,
                    old_score,
                )
                continue
            merged_json = json.dumps(merged, ensure_ascii=False)

            if row.get("raw_json") is None and row.get("content_json") is None:
                _upsert = sb.upsert_suffix(
                    ["link_id"], ["raw_json"],
                )
                execute(
                    "INSERT INTO qa_link_content (link_id, raw_json, video_parse_status, status) "
                    "VALUES (%s, %s, 'pending', 'done') "
                    + _upsert + ", video_parse_status = 'pending'",
                    (link_id, merged_json),
                )
            else:
                execute(
                    "UPDATE qa_link_content SET raw_json = %s, video_parse_status = 'pending' WHERE link_id = %s",
                    (merged_json, link_id),
                )

            _sync_link_video_metadata(link_id, merged, video_id)

            link_updated_at = row.get("link_updated_at")
            ol = " AND updated_at = %s" if link_updated_at else ""
            params = (link_id, link_updated_at) if link_updated_at else (link_id,)
            n = execute(
                "UPDATE qa_link SET status = 'done', fetched_at = CURRENT_TIMESTAMP "
                "WHERE link_id = %s AND status != 'done'" + ol,
                params,
            )
            if link_updated_at and n == 0:
                logger.warning("DouyinDataMerger: link %s optimistic lock failed", link_id)
            publish_time = _extract_publish_time(video_row, merged)
            popularity = _build_popularity(video_row, merged)
            if publish_time or popularity:
                execute(
                    "UPDATE qa_link "
                    "SET publish_time = COALESCE(NULLIF(%s, ''), publish_time), "
                    "    popularity = COALESCE(NULLIF(%s, ''), popularity) "
                    "WHERE link_id = %s",
                    (publish_time, popularity, link_id),
                )

            updated += 1
            logger.info("Enriched %s with douyin_videos data (video_id=%s)", link_id, video_id)

        now = time.monotonic()
        if updated > 0 or skipped_downgrade > 0:
            logger.info(
                "DouyinDataMerger: enriched %d / %d Douyin links, skipped_downgrade=%d",
                updated,
                len(rows),
                skipped_downgrade,
            )
        elif now - _LAST_ZERO_ENRICH_LOG[0] >= _ZERO_ENRICH_INTERVAL:
            _LAST_ZERO_ENRICH_LOG[0] = now
            logger.info(
                "DouyinDataMerger: enriched 0 / %d Douyin links (throttled)",
                len(rows),
            )
        return updated

    @staticmethod
    def _resolve_video_id(content: dict, link_url: str) -> str:
        """Try to get video_id from content_json first, then from the URL."""
        return resolve_video_id(content, link_url)

    @staticmethod
    def _merge_content(
        existing: dict,
        video_row: dict,
        comments_rows: list[dict],
        link_id: str,
    ) -> dict:
        """Merge douyin_videos + douyin_comments data into existing content_json.

        Produces a raw-format dict suitable for the structurer pipeline,
        overwriting empty/zero fields with richer data from the DB tables.
        """
        raw_data = video_row.get("raw_data") or {}
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        title = video_row.get("title") or existing.get("title") or ""
        author = _sanitize_author(video_row.get("author") or "")
        if not author:
            existing_meta = existing.get("metadata") or {}
            author = _sanitize_author(existing_meta.get("author") or "")

        cover_url = ""
        play_url = ""
        duration = 0
        if raw_data:
            video_obj = raw_data.get("video") or {}
            stats_obj = raw_data.get("stats") or {}
            cover_obj = raw_data.get("cover") or video_obj.get("cover") or {}
            if isinstance(cover_obj, dict):
                urls = cover_obj.get("url_list") or []
                cover_url = urls[0] if urls else cover_obj.get("uri", "")
            elif isinstance(cover_obj, str):
                cover_url = cover_obj
            play_addr = video_obj.get("play_addr") or {}
            play_urls = play_addr.get("url_list") or []
            play_url = play_urls[0] if play_urls else ""
            duration = _normalize_duration_seconds(
                video_obj.get("duration", 0)
                or raw_data.get("duration", 0)
                or stats_obj.get("durationSeconds", 0)
            )

        existing_vi = existing.get("video_info") or {}
        if not cover_url:
            cover_url = existing_vi.get("cover_url", "")
        if not play_url:
            play_url = existing_vi.get("play_url", "")
        if not duration:
            duration = _normalize_duration_seconds(existing_vi.get("duration", 0))

        comments = []
        if comments_rows:
            for c in comments_rows:
                comments.append({
                    "text": c.get("content") or "",
                    "digg_count": c.get("likes") or 0,
                    "user": c.get("username") or "",
                    "create_time": c.get("time") or "",
                    "location": c.get("location") or "",
                })
        if not comments:
            comments = existing.get("comments") or []

        existing_meta = existing.get("metadata") or {}
        digg = video_row.get("likes") or existing_meta.get("digg_count", 0)
        comment_count = video_row.get("comments_count") or existing_meta.get("comment_count", 0)
        share_count = video_row.get("shares") or existing_meta.get("share_count", 0)
        favorites = video_row.get("favorites") or existing_meta.get("collected_count", 0)
        play_count = existing_meta.get("play_count", 0)

        return {
            "title": title,
            "content_type": "video",
            "raw_text": title,
            "video_info": {
                "aweme_id": video_row.get("video_id", ""),
                "duration": duration,
                "cover_url": cover_url,
                "play_url": play_url,
            },
            "subtitles": existing.get("subtitles") or [],
            "comments": comments,
            "images": existing.get("images") or [],
            "metadata": {
                "author": author,
                "author_id": existing_meta.get("author_id", ""),
                "publish_time": existing_meta.get("publish_time", ""),
                "digg_count": digg,
                "comment_count": comment_count,
                "share_count": share_count,
                "play_count": play_count,
                "collected_count": favorites,
                "share_link": video_row.get("share_link") or "",
                "short_link": video_row.get("short_link") or "",
                "likes_display": video_row.get("likes_display") or "",
                "comments_display": video_row.get("comments_display") or "",
                "favorites_display": video_row.get("favorites_display") or "",
                "shares_display": video_row.get("shares_display") or "",
            },
            "_enriched_from": "douyin_videos+douyin_comments",
        }


def _normalize_duration_seconds(raw_duration: int | float | str | None) -> int:
    try:
        val = int(float(raw_duration or 0))
    except (TypeError, ValueError):
        return 0
    if val <= 0:
        return 0
    # Source often provides milliseconds.
    if val > 10_000:
        return max(1, val // 1000)
    return val


def _extract_publish_time(video_row: dict, merged: dict) -> str:
    raw_data = video_row.get("raw_data") or {}
    if isinstance(raw_data, str):
        try:
            raw_data = json.loads(raw_data)
        except json.JSONDecodeError:
            raw_data = {}
    stats = raw_data.get("stats") if isinstance(raw_data, dict) else {}
    if isinstance(stats, dict):
        val = (stats.get("publishTime") or "").strip()
        if val:
            return val
    meta = merged.get("metadata") or {}
    return (meta.get("publish_time") or "").strip()


def _build_popularity(video_row: dict, merged: dict) -> str:
    meta = merged.get("metadata") or {}
    likes = meta.get("digg_count", 0)
    comments = meta.get("comment_count", 0)
    favorites = meta.get("collected_count", 0)
    shares = meta.get("share_count", 0)
    likes_d = _prefer_numeric_display(meta.get("likes_display") or video_row.get("likes_display"), likes)
    comments_d = _prefer_numeric_display(meta.get("comments_display") or video_row.get("comments_display"), comments)
    favorites_d = _prefer_numeric_display(meta.get("favorites_display") or video_row.get("favorites_display"), favorites)
    shares_d = _prefer_numeric_display(meta.get("shares_display") or video_row.get("shares_display"), shares)
    return f"点赞:{likes_d}; 评论:{comments_d}; 收藏:{favorites_d}; 分享:{shares_d}"


def _prefer_numeric_display(display: str | None, numeric_value: int | float | str) -> str:
    """Use display text only when it actually looks numeric (e.g. 319, 1.2万)."""
    raw = str(display or "").strip()
    if raw and any(ch.isdigit() for ch in raw):
        return raw
    return str(int(float(numeric_value or 0)))


def _sanitize_author(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    blocked = {
        "我的", "推荐", "关注", "朋友", "直播", "放映厅", "短剧", "搜索", "客户端", "通知", "私信", "投稿"
    }
    if text in blocked:
        return ""
    return text.lstrip("@").strip()


def _is_meaningful_content(content: dict) -> bool:
    """Reject obvious empty payloads before writeback."""
    title = (content.get("title") or "").strip()
    comments = content.get("comments") or []
    duration = int((content.get("video_info") or {}).get("duration") or 0)
    has_comment_text = any((c.get("text") or "").strip() for c in comments if isinstance(c, dict))
    return bool(title or has_comment_text or duration > 0)


def _content_quality_score(content: dict) -> int:
    """Simple quality score to prevent downgrade overwrites."""
    if not isinstance(content, dict):
        return 0

    # If structured, convert to raw-like view for scoring.
    if "链接ID" in content:
        inner = content.get("结构化内容") or {}
        meta = content.get("元数据") or {}
        title = (inner.get("视频标题") or "").strip()
        comments = inner.get("评论") or []
        duration_text = (inner.get("视频时长") or "").strip()
        duration = 0 if duration_text in ("", "00:00") else 1
        publish = (meta.get("发布时间") or "").strip()
        numeric_signal = int(bool(meta.get("点赞数") or meta.get("评论数") or meta.get("播放量")))
        non_empty_comments = sum(1 for c in comments if isinstance(c, dict) and (c.get("内容") or "").strip())
        return int(bool(title)) + non_empty_comments + duration + int(bool(publish)) + numeric_signal

    title = (content.get("title") or "").strip()
    comments = content.get("comments") or []
    video_info = content.get("video_info") or {}
    meta = content.get("metadata") or {}
    duration = int((video_info.get("duration") or 0) > 0)
    publish = (meta.get("publish_time") or "").strip()
    numeric_signal = int(bool(meta.get("digg_count") or meta.get("comment_count") or meta.get("play_count")))
    non_empty_comments = sum(1 for c in comments if isinstance(c, dict) and (c.get("text") or "").strip())
    return int(bool(title)) + non_empty_comments + duration + int(bool(publish)) + numeric_signal


def _sync_link_video_metadata(link_id: str, merged: dict, video_id: str) -> None:
    """Update qa_link_video with enriched video metadata from douyin_videos."""
    vi = merged.get("video_info") or {}
    subtitles = merged.get("subtitles") or []
    has_subtitles = any(
        isinstance(s, dict) and (s.get("text") or "").strip()
        for s in subtitles
    )
    initial_status = "skip" if has_subtitles else "pending"
    subtitles_json = json.dumps(subtitles, ensure_ascii=False) if subtitles else None

    if sb.is_pg:
        execute(
            "INSERT INTO qa_link_video "
            "(link_id, model_api_input_type, video_id, play_url, cover_url, duration, "
            " subtitles, raw_api_response, status, fetched_at) "
            "VALUES (%s, 'input_audio', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) "
            "ON CONFLICT (link_id, model_api_input_type) DO UPDATE SET "
            "video_id  = COALESCE(NULLIF(EXCLUDED.video_id, ''), qa_link_video.video_id), "
            "play_url  = COALESCE(NULLIF(EXCLUDED.play_url, ''), qa_link_video.play_url), "
            "cover_url = COALESCE(NULLIF(EXCLUDED.cover_url, ''), qa_link_video.cover_url), "
            "duration  = GREATEST(EXCLUDED.duration, qa_link_video.duration), "
            "subtitles = COALESCE(EXCLUDED.subtitles, qa_link_video.subtitles), "
            "raw_api_response = COALESCE(qa_link_video.raw_api_response, EXCLUDED.raw_api_response), "
            "fetched_at = COALESCE(qa_link_video.fetched_at, EXCLUDED.fetched_at), "
            "status    = CASE WHEN qa_link_video.status IN ('done','skip') "
            "           THEN qa_link_video.status ELSE EXCLUDED.status END",
            (link_id, video_id, vi.get("play_url") or "", vi.get("cover_url") or "",
             int(vi.get("duration") or 0), subtitles_json,
             json.dumps(merged, ensure_ascii=False), initial_status),
        )
    else:
        execute(
            "INSERT INTO qa_link_video "
            "(link_id, model_api_input_type, video_id, play_url, cover_url, duration, "
            " subtitles, raw_api_response, status, fetched_at) "
            "VALUES (%s, 'input_audio', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) "
            "ON DUPLICATE KEY UPDATE "
            "video_id  = COALESCE(NULLIF(VALUES(video_id), ''), video_id), "
            "play_url  = COALESCE(NULLIF(VALUES(play_url), ''), play_url), "
            "cover_url = COALESCE(NULLIF(VALUES(cover_url), ''), cover_url), "
            "duration  = GREATEST(VALUES(duration), duration), "
            "subtitles = COALESCE(VALUES(subtitles), subtitles), "
            "raw_api_response = COALESCE(raw_api_response, VALUES(raw_api_response)), "
            "fetched_at = COALESCE(fetched_at, VALUES(fetched_at)), "
            "status    = CASE WHEN status IN ('done','skip') "
            "           THEN status ELSE VALUES(status) END",
            (link_id, video_id, vi.get("play_url") or "", vi.get("cover_url") or "",
             int(vi.get("duration") or 0), subtitles_json,
             json.dumps(merged, ensure_ascii=False), initial_status),
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    merger = DouyinDataMerger()
    count = merger.merge_all()
    print(f"Enriched {count} Douyin link(s)")
