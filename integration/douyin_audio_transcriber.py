"""Douyin audio transcription pipeline (ASR-first with Seed2 fallback)."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import httpx

from shared.config import CONFIG
from shared.db import execute, fetch_all
from shared.utils import to_raw_dict, has_meaningful_subtitles
from shared.volcengine_llm import get_seed2_client

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EXPORT_ROOT = _PROJECT_ROOT / "export"
_MEDIA_DIR = _EXPORT_ROOT / "media"
_DEFAULT_API_BASE = CONFIG.get("douyin_api", {}).get("url", "http://localhost:8081")
_SEED2_MODEL = (
    os.getenv("VOLCENGINE_AUDIO_MODEL", "").strip()
    or CONFIG.get("volcengine", {}).get("seedance_model")
    or CONFIG.get("volcengine", {}).get("seed_model", "")
)
_MAX_AUDIO_MB = int(os.getenv("DOUYIN_AUDIO_MAX_MB", "10"))
_MAX_VIDEO_MB = int(os.getenv("DOUYIN_VIDEO_MAX_MB", "80"))
_MAX_PROMPT_AUDIO_CHARS = int(os.getenv("DOUYIN_AUDIO_MAX_TRANSCRIPT_CHARS", "6000"))
_FILE_PROCESS_MAX_WAIT_SECONDS = int(os.getenv("DOUYIN_FILE_PROCESS_MAX_WAIT_SECONDS", "900"))
_FILE_PROCESS_POLL_INTERVAL_SECONDS = int(os.getenv("DOUYIN_FILE_PROCESS_POLL_INTERVAL_SECONDS", "20"))
_FILE_PROCESS_MAX_POLL_INTERVAL_SECONDS = int(os.getenv("DOUYIN_FILE_PROCESS_MAX_POLL_INTERVAL_SECONDS", "60"))
_ASR_TIMEOUT_SECONDS = int(os.getenv("VOLCENGINE_ASR_TIMEOUT_SECONDS", "180"))
_ASR_MODEL_NAME = (os.getenv("VOLCENGINE_ASR_MODEL_NAME", "bigmodel") or "bigmodel").strip()
_FILE_ID_RE = re.compile(r"file-[A-Za-z0-9-]+")
_ASR_ENDPOINT = CONFIG.get("asr", {}).get(
    "endpoint",
    "https://openspeech.bytedance.com/api/v3/auc/bigmodel/recognize/flash",
)
_ASR_APP_ID = (CONFIG.get("asr", {}).get("app_id") or "").strip()
_ASR_ACCESS_TOKEN = (CONFIG.get("asr", {}).get("access_token") or "").strip()
_ASR_RESOURCE_ID = (CONFIG.get("asr", {}).get("resource_id") or "volc.bigasr.auc_turbo").strip()


_to_raw_dict = to_raw_dict
_has_meaningful_subtitles = has_meaningful_subtitles


def _extract_text_from_response(resp) -> str:
    """Best-effort extraction for Responses API object."""
    output_text = getattr(resp, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    payload = None
    if hasattr(resp, "model_dump"):
        try:
            payload = resp.model_dump()
        except Exception:
            payload = None
    if payload is None:
        payload = getattr(resp, "__dict__", {})

    collected: list[str] = []

    def _walk(node):
        if isinstance(node, dict):
            txt = node.get("text")
            if isinstance(txt, str) and txt.strip():
                collected.append(txt.strip())
            for v in node.values():
                _walk(v)
            return
        if isinstance(node, list):
            for it in node:
                _walk(it)
            return
        if isinstance(node, str):
            # avoid collecting every random field string by requiring non-trivial text
            if len(node.strip()) > 8:
                collected.append(node.strip())

    _walk(payload)
    if not collected:
        return ""
    return "\n".join(collected).strip()


def download_video(link_url: str, api_base: str, output_path: Path) -> Path:
    """Download Douyin video using local download API. Retry once on connection/partial body errors."""
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
    """Extract audio directly to compressed mp3 (16k mono 64kbps)."""
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "64k",
        str(audio_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg audio extract failed: {(proc.stderr or proc.stdout)[:500]}")
    return audio_path


def _looks_audio_not_supported_error(err_text: str) -> bool:
    text = (err_text or "").lower()
    return (
        "input_audio" in text
        or "audio input is not supported" in text
        or ("audio" in text and "not supported" in text)
    )


def _extract_file_id_from_error(err_text: str) -> str:
    m = _FILE_ID_RE.search(err_text or "")
    return m.group(0) if m else ""


def _safe_transcript_model_name(model: str) -> str:
    """Avoid exposing endpoint id like ep-xxxx in stored metadata."""
    m = (model or "").strip()
    if not m:
        return "seed2"
    if m.startswith("ep-"):
        return "seed2"
    return m


def transcribe_with_volcengine_asr(audio_path: Path) -> tuple[str, list[dict], int]:
    """Use Volcengine Flash ASR with base64 audio payload.

    Returns (transcript_text, subtitle_entries, duration_ms).
    """
    if not _ASR_APP_ID or not _ASR_ACCESS_TOKEN:
        raise RuntimeError("VOLCENGINE_ASR_APP_ID / VOLCENGINE_ASR_ACCESS_TOKEN is empty")
    if not audio_path.exists():
        raise RuntimeError(f"audio file not found: {audio_path}")

    payload_audio = base64.b64encode(audio_path.read_bytes()).decode("utf-8")
    headers = {
        "X-Api-App-Key": _ASR_APP_ID,
        "X-Api-Access-Key": _ASR_ACCESS_TOKEN,
        "X-Api-Resource-Id": _ASR_RESOURCE_ID,
        "X-Api-Request-Id": str(uuid.uuid4()),
        "X-Api-Sequence": "-1",
    }
    request = {
        "user": {"uid": _ASR_APP_ID},
        "audio": {"data": payload_audio},
        "request": {"model_name": _ASR_MODEL_NAME},
    }
    import requests as _requests

    resp = _requests.post(
        _ASR_ENDPOINT, json=request, headers=headers, timeout=_ASR_TIMEOUT_SECONDS,
    )
    if "X-Api-Status-Code" not in resp.headers:
        raise RuntimeError(
            f"flash asr: missing status header, http={resp.status_code}, headers={dict(resp.headers)}"
        )
    api_status = (resp.headers.get("X-Api-Status-Code") or "").strip()
    if api_status != "20000000":
        api_msg = (resp.headers.get("X-Api-Message") or "").strip()
        logid = (resp.headers.get("X-Tt-Logid") or "").strip()
        raise RuntimeError(
            f"flash asr failed: status={api_status} message={api_msg} logid={logid} body={resp.text[:500]}"
        )

    body = resp.json()
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
            subtitles.append(
                {
                    "start_time": item.get("start_time", ""),
                    "end_time": item.get("end_time", ""),
                    "text": line_text,
                }
            )

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
        raise RuntimeError("flash asr returned empty text")
    return text, subtitles, duration_ms


def _wait_file_active(client, file_id: str) -> None:
    """Wait until uploaded file becomes active with backoff polling."""
    max_wait = max(30, int(_FILE_PROCESS_MAX_WAIT_SECONDS))
    poll = max(5.0, float(_FILE_PROCESS_POLL_INTERVAL_SECONDS))
    poll_max = max(poll, float(_FILE_PROCESS_MAX_POLL_INTERVAL_SECONDS))
    start = time.time()

    while True:
        file_obj = client.files.retrieve(file_id)
        status = str(getattr(file_obj, "status", "") or "").lower()
        if status == "active":
            return
        if status in ("failed", "error", "cancelled"):
            raise RuntimeError(f"file {file_id} processing failed: status={status}")
        if time.time() - start >= max_wait:
            raise RuntimeError(
                f"Giving up on waiting for file {file_id} to finish processing after {max_wait} seconds."
            )
        time.sleep(poll)
        poll = min(poll_max, poll * 1.5)


def transcribe_audio_with_seed2(
    media_path: Path,
    *,
    model: str | None = None,
    media_kind: str = "audio",
) -> tuple[str, str, str]:
    """Upload file via Files API and parse with Responses API.

    Returns (transcript_text, model_api_file_id, input_type).
    """
    chosen_model = model or _SEED2_MODEL
    if not chosen_model:
        raise RuntimeError("VOLCENGINE_SEEDANCE_MODEL/VOLCENGINE_SEED_MODEL is empty")

    size_mb = media_path.stat().st_size / (1024 * 1024)
    limit_mb = _MAX_AUDIO_MB if media_kind == "audio" else _MAX_VIDEO_MB
    if size_mb > limit_mb:
        raise RuntimeError(
            f"media too large for single request: {size_mb:.1f}MB > {limit_mb}MB ({media_kind})"
        )

    input_type = "input_audio" if media_kind == "audio" else "input_video"
    prompt = (
        "请完整转录文件中的中文口述内容，输出纯文本，不要解释。如果有口语停顿请自然断句。"
        if media_kind == "audio"
        else "请完整转录视频中的中文口述/字幕内容，输出纯文本，不要解释。"
    )

    client = get_seed2_client(timeout=300)
    create_kwargs = {"purpose": "user_data"}

    with media_path.open("rb") as f:
        uploaded = client.files.create(file=f, **create_kwargs)
    file_id = getattr(uploaded, "id", "")
    if not file_id:
        raise RuntimeError("Files API upload succeeded but file.id is empty")

    _wait_file_active(client, file_id)

    req = {
        "model": chosen_model,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": input_type,
                        "file_id": file_id,
                    },
                    {"type": "input_text", "text": prompt},
                ],
            }
        ],
        "caching": {"type": "enabled"},
        "store": True,
    }
    try:
        resp = client.responses.create(**req)
    except TypeError:
        # SDK compatibility: older clients may not accept caching/store args.
        req.pop("caching", None)
        req.pop("store", None)
        resp = client.responses.create(**req)
    text = _extract_text_from_response(resp)
    return text[:_MAX_PROMPT_AUDIO_CHARS].strip(), file_id, input_type


def process_one(
    link_id: str,
    query_id: str,
    link_url: str,
    raw_json: dict | str | None,
    api_base: str = _DEFAULT_API_BASE,
) -> dict:
    """Process one douyin row and return result metadata."""
    raw = _to_raw_dict(raw_json)
    if _has_meaningful_subtitles(raw):
        return {"skipped": True, "reason": "already_has_subtitles"}
    if (raw.get("stt_text") or "").strip():
        return {"skipped": True, "reason": "already_has_stt_text"}
    if not link_url:
        return {"skipped": True, "reason": "missing_link_url"}

    out_dir = _MEDIA_DIR / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    video_path = out_dir / f"{link_id}.mp4"
    audio_path = out_dir / f"{link_id}.mp3"

    # 先下载视频，再抽取 mp3；默认走 ASR 音频转写。
    if not video_path.exists() or video_path.stat().st_size == 0:
        download_video(link_url, api_base, video_path)

    audio_ok = False
    if audio_path.exists() and audio_path.stat().st_size > 0:
        audio_ok = True
    else:
        try:
            extract_compress_audio(video_path, audio_path)
            audio_ok = True
        except Exception as ext_exc:
            logger.warning("[audio] %s ffmpeg extract failed, will try Seed2 video: %s", link_id, str(ext_exc)[:200])

    transcript_source = "asr_audio"
    transcript = ""
    model_api_file_id = ""
    model_api_input_type = "input_audio"
    subtitles: list[dict] = []
    duration_ms = 0
    transcript_model = "volcengine_asr_bigmodel"
    asr_exc: Exception | None = None

    if audio_ok:
        try:
            transcript, subtitles, duration_ms = transcribe_with_volcengine_asr(audio_path)
        except Exception as exc:
            asr_exc = exc
            logger.error(
                "[audio] %s flash asr failed, fallback to seed2 video: %s",
                link_id,
                str(exc)[:500],
                exc_info=True,
            )
            audio_ok = False

    if not audio_ok or not transcript:
        try:
            transcript, model_api_file_id, model_api_input_type = transcribe_audio_with_seed2(
                video_path,
                media_kind="video",
            )
            transcript_source = "seed2_video"
            transcript_model = _safe_transcript_model_name(_SEED2_MODEL)
        except Exception as seed_exc:
            err_text = str(seed_exc)
            logger.error("[audio] %s seed2 video fallback failed: %s", link_id, err_text[:500], exc_info=True)
            fallback_text = (raw.get("raw_text") or raw.get("caption") or "").strip()
            if not fallback_text:
                raise RuntimeError(
                    f"transcribe failed: asr={asr_exc}; seed2={seed_exc}"
                ) from seed_exc
            transcript = fallback_text
            model_api_file_id = _extract_file_id_from_error(err_text)
            model_api_input_type = ""
            transcript_source = "raw_text_fallback"
            transcript_model = "raw_text_fallback"
            logger.warning("[audio] %s using raw_text fallback", link_id)

    if not transcript:
        return {
            "skipped": True,
            "reason": "empty_transcript",
            "audio_path": str(audio_path),
            "video_path": str(video_path),
        }

    return {
        "stt_text": transcript,
        "audio_path": str(audio_path),
        "video_path": str(video_path),
        "model_api_file_id": model_api_file_id,
        "model_api_input_type": model_api_input_type,
        "transcript_source": transcript_source,
        "transcript_model": transcript_model,
        "subtitles": subtitles,
        "duration_ms": duration_ms,
        "skipped": False,
    }


def _set_video_status(vid: int, link_id: str, status: str, error_message: str = "") -> None:
    """Update status on qa_link_video (by PK) and qa_link_content.video_parse_status."""
    extra = ""
    params: list = [status, vid]
    if error_message:
        extra = ", error_message = %s, retry_count = retry_count + 1"
        params = [status, error_message, vid]
    execute(
        f"UPDATE qa_link_video SET status = %s{extra} WHERE id = %s",
        tuple(params),
    )
    execute(
        "UPDATE qa_link_content SET video_parse_status = %s WHERE link_id = %s",
        (status, link_id),
    )


def _sync_to_content(vid: int, link_id: str, result: dict, raw: dict) -> None:
    """Write STT results back into qa_link_content.raw_json (by link_id)
    and qa_link_video (by PK vid), then clear content_json for re-structuring.
    """
    stt_text = (result.get("stt_text") or "").strip()
    model_name = (result.get("transcript_model") or _safe_transcript_model_name(_SEED2_MODEL)).strip()
    subtitles = result.get("subtitles")
    if not isinstance(subtitles, list):
        subtitles = []

    raw["stt_text"] = stt_text
    raw.setdefault("audio_info", {})
    raw["audio_info"]["audio_path"] = result.get("audio_path", "")
    raw["audio_info"]["video_path"] = result.get("video_path", "")
    raw["audio_info"]["transcript_model"] = model_name
    raw["audio_info"]["transcript_source"] = result.get("transcript_source", "audio_input")
    raw["audio_info"]["model_api_file_id"] = result.get("model_api_file_id", "")
    raw["audio_info"]["model_api_input_type"] = result.get("model_api_input_type", "")
    if result.get("duration_ms"):
        raw["audio_info"]["duration_ms"] = int(result["duration_ms"])

    if subtitles:
        raw["subtitles"] = subtitles
    elif not _has_meaningful_subtitles(raw):
        raw["subtitles"] = [{"start_time": "", "text": stt_text}]

    execute(
        "UPDATE qa_link_content "
        "SET raw_json = %s, content_json = NULL, status = 'done', video_parse_status = 'done' "
        "WHERE link_id = %s",
        (json.dumps(raw, ensure_ascii=False), link_id),
    )

    execute(
        "UPDATE qa_link_video SET "
        "stt_text = %s, video_path = %s, audio_path = %s, "
        "transcript_model = %s, transcript_source = %s, "
        "model_api_file_id = %s, subtitles = %s, "
        "status = 'done', transcribed_at = CURRENT_TIMESTAMP "
        "WHERE id = %s",
        (
            stt_text,
            result.get("video_path", ""),
            result.get("audio_path", ""),
            model_name,
            result.get("transcript_source", ""),
            result.get("model_api_file_id", ""),
            json.dumps(raw.get("subtitles") or [], ensure_ascii=False),
            vid,
        ),
    )


def batch_process(
    *,
    query_ids: list[str] | None = None,
    api_base: str = _DEFAULT_API_BASE,
    concurrency: int = 2,
    batch_size: int = 1000,
) -> int:
    """Claim one batch from qa_link_video, process, and sync results to qa_link_content."""
    rows = fetch_all(
        "SELECT * FROM claim_pending_video_parse_v2(%s, %s)",
        (max(1, int(batch_size)), query_ids or None),
    )

    if not rows:
        logger.info("[audio] no eligible douyin links for transcription")
        return 0

    success_count = 0

    def _run(row: dict) -> tuple[int, str, dict, dict]:
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
        return vid, link_id, _to_raw_dict(row.get("raw_json")), result

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        futures = [ex.submit(_run, row) for row in rows]
        for fut in as_completed(futures):
            vid, link_id, raw, result = fut.result()
            if result.get("skipped"):
                reason = result.get("reason", "")
                if reason == "already_has_subtitles":
                    _set_video_status(vid, link_id, "skip")
                    execute(
                        "UPDATE qa_link_video SET subtitles = %s WHERE id = %s",
                        (json.dumps(raw.get("subtitles") or [], ensure_ascii=False), vid),
                    )
                elif reason == "already_has_stt_text":
                    _set_video_status(vid, link_id, "done")
                    execute(
                        "UPDATE qa_link_video SET stt_text = %s WHERE id = %s",
                        (raw.get("stt_text", ""), vid),
                    )
                else:
                    err_msg = str(reason)[:500]
                    _set_video_status(vid, link_id, "error", err_msg)
                logger.info("[audio] skip %s: %s", link_id, reason)
                continue

            stt_text = (result.get("stt_text") or "").strip()
            if not stt_text:
                _set_video_status(vid, link_id, "error", "empty_transcript")
                logger.info("[audio] skip %s: empty stt_text", link_id)
                continue

            _sync_to_content(vid, link_id, result, raw)

            execute(
                "UPDATE qa_link_video SET fetched_at = CURRENT_TIMESTAMP "
                "WHERE id = %s AND fetched_at IS NULL",
                (vid,),
            )
            success_count += 1
            logger.info("[audio] transcribed %s", link_id)

    return success_count

