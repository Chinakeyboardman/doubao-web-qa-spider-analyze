"""抖音视频音频转写流水线：下载 → ffmpeg 抽音频 → OSS 上传 → SeedASR 2.0 转写。"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import httpx
import requests as _requests

from shared.config import CONFIG
from shared.db import execute, fetch_all
from shared.oss import upload_file as oss_upload, get_public_url as oss_url
from shared.utils import to_raw_dict, has_meaningful_subtitles, extract_video_id_from_url, resolve_video_id

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EXPORT_ROOT = _PROJECT_ROOT / "export"
_MEDIA_DIR = _EXPORT_ROOT / "media"
_DEFAULT_API_BASE = CONFIG.get("douyin_api", {}).get("url", "http://localhost:8081")

_MAX_PROMPT_AUDIO_CHARS = int(os.getenv("DOUYIN_AUDIO_MAX_TRANSCRIPT_CHARS", "6000"))
_ASR_TIMEOUT_SECONDS = int(os.getenv("VOLCENGINE_ASR_TIMEOUT_SECONDS", "300"))

_ASR_APP_ID = (CONFIG.get("asr", {}).get("app_id") or "").strip()
_ASR_ACCESS_TOKEN = (CONFIG.get("asr", {}).get("access_token") or "").strip()
_ASR_RESOURCE_ID = (CONFIG.get("asr", {}).get("resource_id") or "volc.seedasr.auc").strip()
_ASR_SUBMIT_URL = CONFIG.get("asr", {}).get(
    "submit_url",
    "https://openspeech.bytedance.com/api/v3/auc/bigmodel/submit",
)
_ASR_QUERY_URL = CONFIG.get("asr", {}).get(
    "query_url",
    "https://openspeech.bytedance.com/api/v3/auc/bigmodel/query",
)

_to_raw_dict = to_raw_dict
_has_meaningful_subtitles = has_meaningful_subtitles


# ---------------------------------------------------------------------------
# 视频下载 & 音频提取（保留原有逻辑）
# ---------------------------------------------------------------------------

def download_video(link_url: str, api_base: str, output_path: Path) -> Path:
    """通过本地下载 API 下载抖音视频，连接失败重试一次。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    download_url = f"{api_base}/api/download?url={quote(link_url)}&with_watermark=false"
    last_err: Exception | None = None
    for attempt in range(2):
        try:
            with httpx.Client(timeout=180, follow_redirects=True) as client:
                resp = client.get(download_url)
                resp.raise_for_status()
                data = resp.content
            last_err = None
            break
        except (httpx.RemoteProtocolError, httpx.ConnectError, OSError) as e:
            last_err = e
            if attempt == 0:
                logger.warning("[download] %s attempt %s failed: %s", output_path.name, attempt + 1, str(e)[:120])
                continue
            raise RuntimeError(f"download failed after retry: {e}") from e
    if last_err is not None:
        raise RuntimeError(f"download failed: {last_err}") from last_err

    if len(data) < 10 * 1024 and data.strip().startswith(b"{"):
        try:
            payload = json.loads(data.decode("utf-8", errors="ignore"))
            detail = payload.get("detail")
            if isinstance(detail, dict):
                msg = detail.get("message", str(detail))
            else:
                msg = payload.get("message") or str(payload)
            raise RuntimeError(f"Douyin download API returned error: {str(msg)[:200]}")
        except json.JSONDecodeError:
            pass

    output_path.write_bytes(data)
    return output_path


def extract_compress_audio(video_path: Path, audio_path: Path) -> Path:
    """用 ffmpeg 从视频中提取音频并压缩为 mp3（16kHz 单声道 64kbps）。"""
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-i", str(video_path),
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k",
        str(audio_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg audio extract failed: {(proc.stderr or proc.stdout)[:500]}")
    return audio_path


# ---------------------------------------------------------------------------
# 大模型录音文件识别标准版 API 2.0（SeedASR，提交+查询两步模式）
# ---------------------------------------------------------------------------

def _build_asr_headers(request_id: str) -> dict[str, str]:
    return {
        "X-Api-App-Key": _ASR_APP_ID,
        "X-Api-Access-Key": _ASR_ACCESS_TOKEN,
        "X-Api-Resource-Id": _ASR_RESOURCE_ID,
        "X-Api-Request-Id": request_id,
        "X-Api-Sequence": "-1",
    }


def transcribe_with_seedasr_v2(
    audio_url: str,
    audio_format: str = "mp3",
) -> tuple[str, list[dict], int]:
    """使用火山引擎 SeedASR 2.0 标准版进行录音文件识别。

    两步模式：先提交任务，再轮询查询结果。
    返回 (transcript_text, subtitle_entries, duration_ms)。
    """
    if not _ASR_APP_ID or not _ASR_ACCESS_TOKEN:
        raise RuntimeError("VOLCENGINE_ASR_APP_ID / VOLCENGINE_ASR_ACCESS_TOKEN 未配置")

    request_id = str(uuid.uuid4())
    headers = _build_asr_headers(request_id)

    # ---- 第 1 步：提交任务 ----
    submit_body = {
        "user": {"uid": _ASR_APP_ID},
        "audio": {
            "format": audio_format,
            "url": audio_url,
        },
        "request": {
            "model_name": "bigmodel",
            "enable_itn": True,
            "enable_punc": True,
            "show_utterances": True,
        },
    }

    resp = _requests.post(
        _ASR_SUBMIT_URL, json=submit_body, headers=headers, timeout=60,
    )
    status_code = (resp.headers.get("X-Api-Status-Code") or "").strip()
    if status_code != "20000000":
        api_msg = (resp.headers.get("X-Api-Message") or "").strip()
        logid = (resp.headers.get("X-Tt-Logid") or "").strip()
        raise RuntimeError(
            f"SeedASR submit failed: status={status_code} message={api_msg} logid={logid}"
        )
    logger.info("[asr] 任务已提交: request_id=%s", request_id)

    # ---- 第 2 步：轮询查询结果 ----
    query_headers = _build_asr_headers(request_id)
    poll_interval = 3.0
    max_poll_interval = 15.0
    start_time = time.time()

    while True:
        if time.time() - start_time > _ASR_TIMEOUT_SECONDS:
            raise RuntimeError(
                f"SeedASR 查询超时: 已等待 {_ASR_TIMEOUT_SECONDS}s, request_id={request_id}"
            )

        time.sleep(poll_interval)
        poll_interval = min(max_poll_interval, poll_interval * 1.5)

        query_resp = _requests.post(
            _ASR_QUERY_URL, json={}, headers=query_headers, timeout=60,
        )
        q_status = (query_resp.headers.get("X-Api-Status-Code") or "").strip()

        if q_status in ("20000001", "20000002"):
            logger.debug("[asr] 任务处理中: status=%s, request_id=%s", q_status, request_id)
            continue

        if q_status == "20000003":
            raise RuntimeError(f"SeedASR 返回静音音频: request_id={request_id}")

        if q_status != "20000000":
            api_msg = (query_resp.headers.get("X-Api-Message") or "").strip()
            logid = (query_resp.headers.get("X-Tt-Logid") or "").strip()
            raise RuntimeError(
                f"SeedASR query failed: status={q_status} message={api_msg} logid={logid}"
            )

        break

    # ---- 解析结果 ----
    body = query_resp.json()
    result = body.get("result") or {}
    text = (result.get("text") or "").strip()

    utterances = result.get("utterances") or []
    subtitles: list[dict] = []
    if isinstance(utterances, list):
        for item in utterances:
            if not isinstance(item, dict):
                continue
            line_text = (item.get("text") or "").strip()
            if not line_text:
                continue
            subtitles.append({
                "start_time": item.get("start_time", ""),
                "end_time": item.get("end_time", ""),
                "text": line_text,
            })

    duration = body.get("audio_info", {}).get("duration")
    if not duration:
        duration = (result.get("additions") or {}).get("duration")
    try:
        duration_ms = int(duration or 0)
    except Exception:
        duration_ms = 0

    if not text and subtitles:
        text = " ".join([s["text"] for s in subtitles]).strip()
    text = text[:_MAX_PROMPT_AUDIO_CHARS].strip()
    if not text:
        raise RuntimeError(f"SeedASR 返回空文本: request_id={request_id}")

    logger.info("[asr] 转写完成: request_id=%s, 文本长度=%d", request_id, len(text))
    return text, subtitles, duration_ms


# ---------------------------------------------------------------------------
# 单条处理：下载 → 抽音频 → OSS 上传 → ASR 转写
# ---------------------------------------------------------------------------

def _oss_key_for(query_id: str, file_basename: str, ext: str) -> str:
    """生成 OSS 对象 key，与本地 export/media/ 目录结构一致。"""
    return f"export/media/{query_id}/{file_basename}.{ext}"


def _file_basename(link_id: str, link_url: str, raw: dict) -> str:
    """生成音视频文件 basename：link_id + 抖音视频 ID，用于区分同名链接。"""
    video_id = resolve_video_id(raw, link_url) or extract_video_id_from_url(link_url)
    if video_id:
        return f"{link_id}_{video_id}"
    return link_id


def process_one(
    link_id: str,
    query_id: str,
    link_url: str,
    raw_json: dict | str | None,
    api_base: str = _DEFAULT_API_BASE,
) -> dict:
    """处理单条抖音链接：下载视频 → 抽取音频 → 上传 OSS → ASR 转写。"""
    raw = _to_raw_dict(raw_json)
    if _has_meaningful_subtitles(raw):
        return {"skipped": True, "reason": "already_has_subtitles"}
    if (raw.get("stt_text") or "").strip():
        return {"skipped": True, "reason": "already_has_stt_text"}
    if not link_url:
        return {"skipped": True, "reason": "missing_link_url"}

    file_basename = _file_basename(link_id, link_url, raw)
    out_dir = _MEDIA_DIR / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    video_path = out_dir / f"{file_basename}.mp4"
    audio_path = out_dir / f"{file_basename}.mp3"

    # 1) 下载视频
    if not video_path.exists() or video_path.stat().st_size == 0:
        download_video(link_url, api_base, video_path)

    # 2) 用 ffmpeg 抽取 mp3
    if not audio_path.exists() or audio_path.stat().st_size == 0:
        extract_compress_audio(video_path, audio_path)

    # 3) 上传到 OSS
    video_oss_url = oss_upload(video_path, _oss_key_for(query_id, file_basename, "mp4"))
    audio_oss_url = oss_upload(audio_path, _oss_key_for(query_id, file_basename, "mp3"))

    # 4) SeedASR 2.0 转写
    transcript = ""
    subtitles: list[dict] = []
    duration_ms = 0
    transcript_source = "seedasr_v2"
    transcript_model = "seedasr_v2"

    try:
        transcript, subtitles, duration_ms = transcribe_with_seedasr_v2(
            audio_oss_url, audio_format="mp3",
        )
    except Exception as asr_exc:
        logger.error("[audio] %s SeedASR 转写失败: %s", link_id, str(asr_exc)[:500], exc_info=True)
        fallback_text = (raw.get("raw_text") or raw.get("caption") or "").strip()
        if not fallback_text:
            raise RuntimeError(f"transcribe failed: {asr_exc}") from asr_exc
        transcript = fallback_text
        transcript_source = "raw_text_fallback"
        transcript_model = "raw_text_fallback"
        logger.warning("[audio] %s 使用 raw_text 回退", link_id)

    if not transcript:
        return {
            "skipped": True,
            "reason": "empty_transcript",
            "audio_path": audio_oss_url,
            "video_path": video_oss_url,
        }

    return {
        "stt_text": transcript,
        "audio_path": audio_oss_url,
        "video_path": video_oss_url,
        "model_api_file_id": "",
        "model_api_input_type": "input_audio",
        "transcript_source": transcript_source,
        "transcript_model": transcript_model,
        "subtitles": subtitles,
        "duration_ms": duration_ms,
        "skipped": False,
    }


# ---------------------------------------------------------------------------
# DB 状态同步（保留原有逻辑）
# ---------------------------------------------------------------------------

def _set_video_status(
    vid: int, link_id: str, status: str, error_message: str = "",
    video_updated_at=None, content_updated_at=None,
) -> None:
    """更新 qa_link_video 和 qa_link_content.video_parse_status 的状态。"""
    extra = ""
    params: list = [status, vid]
    if error_message:
        extra = ", error_message = %s, retry_count = retry_count + 1"
        params = [status, error_message, vid]
    ol_v = " AND updated_at = %s" if video_updated_at else ""
    params_v = tuple(params) + (video_updated_at,) if video_updated_at else tuple(params)
    n = execute(
        f"UPDATE qa_link_video SET status = %s{extra} WHERE id = %s{ol_v}",
        params_v,
    )
    if video_updated_at and n == 0:
        logger.warning("[audio] qa_link_video id=%s optimistic lock failed", vid)
    ol_c = " AND updated_at = %s" if content_updated_at else ""
    params_c = (status, link_id, content_updated_at) if content_updated_at else (status, link_id)
    n = execute(
        f"UPDATE qa_link_content SET video_parse_status = %s WHERE link_id = %s{ol_c}",
        params_c,
    )
    if content_updated_at and n == 0:
        logger.warning("[audio] qa_link_content %s optimistic lock failed", link_id)


def _sync_to_content(
    vid: int, link_id: str, result: dict, raw: dict,
    video_updated_at=None, content_updated_at=None,
) -> None:
    """将 STT 结果写回 qa_link_content.raw_json 和 qa_link_video，并清空 content_json 以便重新结构化。"""
    stt_text = (result.get("stt_text") or "").strip()
    model_name = (result.get("transcript_model") or "seedasr_v2").strip()
    subtitles = result.get("subtitles")
    if not isinstance(subtitles, list):
        subtitles = []

    raw["stt_text"] = stt_text
    raw.setdefault("audio_info", {})
    raw["audio_info"]["audio_path"] = result.get("audio_path", "")
    raw["audio_info"]["video_path"] = result.get("video_path", "")
    raw["audio_info"]["transcript_model"] = model_name
    raw["audio_info"]["transcript_source"] = result.get("transcript_source", "seedasr_v2")
    raw["audio_info"]["model_api_file_id"] = result.get("model_api_file_id", "")
    raw["audio_info"]["model_api_input_type"] = result.get("model_api_input_type", "")
    if result.get("duration_ms"):
        raw["audio_info"]["duration_ms"] = int(result["duration_ms"])

    if subtitles:
        raw["subtitles"] = subtitles
    elif not _has_meaningful_subtitles(raw):
        raw["subtitles"] = [{"start_time": "", "text": stt_text}]

    ol_c = " AND updated_at = %s" if content_updated_at else ""
    params_c = (json.dumps(raw, ensure_ascii=False), link_id, content_updated_at) if content_updated_at else (json.dumps(raw, ensure_ascii=False), link_id)
    n = execute(
        "UPDATE qa_link_content "
        "SET raw_json = %s, content_json = NULL, status = 'done', video_parse_status = 'done' "
        f"WHERE link_id = %s{ol_c}",
        params_c,
    )
    if content_updated_at and n == 0:
        logger.warning("[audio] qa_link_content %s optimistic lock failed (sync)", link_id)

    ol_v = " AND updated_at = %s" if video_updated_at else ""
    base = (
        stt_text, result.get("video_path", ""), result.get("audio_path", ""),
        model_name, result.get("transcript_source", ""), result.get("model_api_file_id", ""),
        json.dumps(raw.get("subtitles") or [], ensure_ascii=False), vid,
    )
    params_v = base + (video_updated_at,) if video_updated_at else base
    n = execute(
        "UPDATE qa_link_video SET "
        "stt_text = %s, video_path = %s, audio_path = %s, "
        "transcript_model = %s, transcript_source = %s, "
        "model_api_file_id = %s, subtitles = %s, "
        "status = 'done', transcribed_at = CURRENT_TIMESTAMP "
        f"WHERE id = %s{ol_v}",
        params_v,
    )
    if video_updated_at and n == 0:
        logger.warning("[audio] qa_link_video id=%s optimistic lock failed (sync)", vid)


# ---------------------------------------------------------------------------
# 批量处理
# ---------------------------------------------------------------------------

def batch_process(
    *,
    query_ids: list[str] | None = None,
    api_base: str = _DEFAULT_API_BASE,
    concurrency: int = 2,
    batch_size: int = 1000,
) -> int:
    """从 qa_link_video 领取一批待处理行，执行完整的转写流程并同步到 qa_link_content。"""
    from shared.claim_functions import claim_pending_video_parse_v2
    rows = claim_pending_video_parse_v2(max(1, int(batch_size)), query_ids=query_ids or None)

    if not rows:
        logger.info("[audio] no eligible douyin links for transcription")
        return 0

    success_count = 0

    def _run(row: dict) -> tuple[int, str, dict, dict, dict]:
        vid = row["vid"]
        link_id = row["link_id"]
        try:
            result = process_one(
                link_id,
                row["query_id"],
                row.get("link_url") or "",
                row.get("raw_json"),
                api_base,
            )
        except Exception as exc:
            logger.error("[audio] %s process failed: %s", link_id, exc, exc_info=True)
            result = {
                "skipped": True,
                "reason": f"error:{exc}",
            }
        return vid, link_id, _to_raw_dict(row.get("raw_json")), result, row

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        futures = [ex.submit(_run, row) for row in rows]
        for fut in as_completed(futures):
            vid, link_id, raw, result, row = fut.result()
            vo, co = row.get("video_updated_at"), row.get("content_updated_at")
            if result.get("skipped"):
                reason = result.get("reason", "")
                if reason == "already_has_subtitles":
                    _set_video_status(vid, link_id, "skip", video_updated_at=vo, content_updated_at=co)
                    execute(
                        "UPDATE qa_link_video SET subtitles = %s WHERE id = %s",
                        (json.dumps(raw.get("subtitles") or [], ensure_ascii=False), vid),
                    )
                elif reason == "already_has_stt_text":
                    _set_video_status(vid, link_id, "done", video_updated_at=vo, content_updated_at=co)
                    execute(
                        "UPDATE qa_link_video SET stt_text = %s WHERE id = %s",
                        (raw.get("stt_text", ""), vid),
                    )
                else:
                    err_msg = str(reason)[:500]
                    _set_video_status(vid, link_id, "error", err_msg, video_updated_at=vo, content_updated_at=co)
                logger.info("[audio] skip %s: %s", link_id, reason)
                continue

            stt_text = (result.get("stt_text") or "").strip()
            if not stt_text:
                _set_video_status(vid, link_id, "error", "empty_transcript", video_updated_at=vo, content_updated_at=co)
                logger.info("[audio] skip %s: empty stt_text", link_id)
                continue

            _sync_to_content(vid, link_id, result, raw, video_updated_at=vo, content_updated_at=co)

            execute(
                "UPDATE qa_link_video SET fetched_at = CURRENT_TIMESTAMP "
                "WHERE id = %s AND fetched_at IS NULL",
                (vid,),
            )
            success_count += 1
            logger.info("[audio] transcribed %s", link_id)

    return success_count
