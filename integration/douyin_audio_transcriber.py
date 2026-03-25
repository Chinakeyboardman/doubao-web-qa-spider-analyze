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
from integration.raw_content_postprocess import shrink_json_object_for_storage

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EXPORT_ROOT = _PROJECT_ROOT / "export"
_DEFAULT_API_BASE = CONFIG.get("douyin_api", {}).get("url", "http://localhost:8081")


def _media_dir() -> Path:
    """与 export/media 一致；随 _EXPORT_ROOT 补丁变化（单测隔离本地目录）。"""
    return _EXPORT_ROOT / "media"

_MAX_PROMPT_AUDIO_CHARS = int(os.getenv("DOUYIN_AUDIO_MAX_TRANSCRIPT_CHARS", "6000"))
_ASR_TIMEOUT_SECONDS = int(os.getenv("VOLCENGINE_ASR_TIMEOUT_SECONDS", "300"))

# 抖音本地下载 API：大文件易中途断连，需流式写入 + 多轮重试 + 读超时放宽
_DOUYIN_DOWNLOAD_MAX_RETRIES = max(1, int(os.getenv("DOUYIN_DOWNLOAD_MAX_RETRIES", "5")))
_DOUYIN_DOWNLOAD_READ_TIMEOUT = float(os.getenv("DOUYIN_DOWNLOAD_READ_TIMEOUT_SECONDS", "900"))
_DOUYIN_DOWNLOAD_CHUNK_BYTES = max(65536, int(os.getenv("DOUYIN_DOWNLOAD_CHUNK_BYTES", str(256 * 1024))))

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

# 音轨为 BGM/歌曲、ASR 无有效口播，但页面仍有标题/简介等文字时：标记 done，正文走文本+后续 LLM（非音轨转写）
_DEFAULT_BGM_PARSE_NOTE = (
    "音轨主要为背景音乐或歌曲，ASR 无有效口播；stt_text 采用页面标题/简介等文本。"
    "后续结构化由 LLM 基于文本与元数据理解（非音轨转写）。"
)
_BGM_FALLBACK_MIN_CHARS = int(os.getenv("DOUYIN_BGM_TEXT_MIN_CHARS", "15"))


def _crawl_combined_text(raw: dict) -> str:
    """合并标题、简介、caption，供 BGM/ASR 失败时的正文兜底。"""
    title = (raw.get("title") or "").strip()
    raw_text = (raw.get("raw_text") or "").strip()
    cap = (raw.get("caption") or "").strip()
    parts = [p for p in (title, raw_text, cap) if p]
    return "\n".join(parts).strip()


def _is_asr_no_speech_like(exc: BaseException) -> bool:
    """SeedASR 静音、空文本等：不适合继续要求音轨转写。"""
    s = str(exc)
    markers = (
        "静音",
        "20000003",
        "空文本",
        "SeedASR 返回空文本",
        "返回静音",
    )
    return any(m in s for m in markers)


# ---------------------------------------------------------------------------
# 视频下载 & 音频提取（保留原有逻辑）
# ---------------------------------------------------------------------------

def _truncate_error_for_db(message: str, *, max_len: int = 2000) -> str:
    """入库错误信息：保留末尾（ffmpeg/网络真实原因多在尾部），避免 [:500] 只剩版本横幅。"""
    s = (message or "").strip()
    if len(s) <= max_len:
        return s
    return f"...[trunc head {len(s) - max_len + 40} chars]...\n" + s[-(max_len - 40) :]


def _parse_download_api_json_error(data: bytes) -> str | None:
    """若响应体为 JSON 错误，返回可读信息。"""
    if not data or len(data) > 64 * 1024 or not data.strip().startswith(b"{"):
        return None
    try:
        payload = json.loads(data.decode("utf-8", errors="ignore"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return str(payload)[:500] if payload else None
    detail = payload.get("detail")
    if isinstance(detail, dict):
        msg = detail.get("message", str(detail))
    else:
        msg = payload.get("message") or str(payload)
    return str(msg)[:500]


def download_video(link_url: str, api_base: str, output_path: Path) -> Path:
    """通过本地下载 API 下载抖音视频。

    使用 **流式写入** + **Content-Length 校验** + **多轮重试**，缓解大文件中途断连
    （如 peer closed connection without complete body）。
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    download_url = f"{api_base}/api/download?url={quote(link_url)}&with_watermark=false"
    part_path = output_path.parent / f"{output_path.name}.part"

    timeout = httpx.Timeout(
        connect=30.0,
        read=_DOUYIN_DOWNLOAD_READ_TIMEOUT,
        write=30.0,
        pool=30.0,
    )
    last_err: Exception | None = None

    for attempt in range(_DOUYIN_DOWNLOAD_MAX_RETRIES):
        wait = min(60.0, 2.0 ** min(attempt, 5))
        try:
            part_path.unlink(missing_ok=True)
            expected_len: int | None = None
            received = 0

            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                with client.stream("GET", download_url) as resp:
                    resp.raise_for_status()
                    cl = resp.headers.get("content-length")
                    if cl and str(cl).isdigit():
                        expected_len = int(cl)
                    ct = (resp.headers.get("content-type") or "").lower()
                    if "application/json" in ct or "text/json" in ct:
                        data = resp.read()
                        if _parse_download_api_json_error(data):
                            raise RuntimeError(
                                f"Douyin download API returned error: {_parse_download_api_json_error(data)}"
                            )
                        raise RuntimeError("Douyin download API returned JSON instead of video")

                    with open(part_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=_DOUYIN_DOWNLOAD_CHUNK_BYTES):
                            received += len(chunk)
                            f.write(chunk)

            if expected_len is not None and received != expected_len:
                part_path.unlink(missing_ok=True)
                last_err = RuntimeError(
                    f"incomplete download: received {received} bytes, expected {expected_len}"
                )
                logger.warning(
                    "[download] %s attempt %s/%s: %s — retry in %.1fs",
                    output_path.name,
                    attempt + 1,
                    _DOUYIN_DOWNLOAD_MAX_RETRIES,
                    last_err,
                    wait,
                )
                time.sleep(wait)
                continue

            raw = part_path.read_bytes()
            if len(raw) < 10 * 1024 and raw.strip().startswith(b"{"):
                err_txt = _parse_download_api_json_error(raw)
                part_path.unlink(missing_ok=True)
                if err_txt:
                    raise RuntimeError(f"Douyin download API returned error: {err_txt}")
                raise RuntimeError("Douyin download API returned JSON error body")

            part_path.replace(output_path)
            return output_path

        except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError, httpx.HTTPStatusError, OSError) as e:
            last_err = e
            part_path.unlink(missing_ok=True)
            logger.warning(
                "[download] %s attempt %s/%s failed: %s — retry in %.1fs",
                output_path.name,
                attempt + 1,
                _DOUYIN_DOWNLOAD_MAX_RETRIES,
                str(e)[:240],
                wait,
            )
            time.sleep(wait)
        except RuntimeError:
            part_path.unlink(missing_ok=True)
            raise

    raise RuntimeError(
        f"Douyin download failed after {_DOUYIN_DOWNLOAD_MAX_RETRIES} attempts: {last_err}"
    ) from last_err


def _ffprobe_audio_stream_count(video_path: Path) -> int:
    """返回视频中音轨数量；ffprobe 不可用时返回 -1（调用方继续尝试 ffmpeg）。"""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index",
        "-of",
        "csv=p=0",
        str(video_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return -1
    if proc.returncode != 0:
        return -1
    lines = [ln.strip() for ln in (proc.stdout or "").splitlines() if ln.strip()]
    return len(lines)


def _ffprobe_container_valid(video_path: Path) -> tuple[bool, str]:
    """检查文件是否为可读媒体（能解析 format duration）。

    用于尽早发现「moov atom not found」、截断下载、HTML 误存为 mp4 等，避免 ffmpeg 反复失败刷屏。
    若系统无 ffprobe，返回 (True, '') 不阻拦后续逻辑。
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "ffprobe timeout (120s)"
    err = (proc.stderr or "").strip()
    out = (proc.stdout or "").strip()
    if proc.returncode != 0:
        tail = err[-2000:] if len(err) > 2000 else err
        return False, tail or f"ffprobe exit {proc.returncode}"
    try:
        duration = float(out.split()[0])
    except (ValueError, IndexError):
        return False, f"bad duration output: {out!r}"
    if duration <= 0:
        return False, f"zero or negative duration: {duration}"
    return True, ""


def _ffmpeg_error_snippet(proc: subprocess.CompletedProcess, *, max_chars: int = 3000) -> str:
    """ffmpeg 失败时真正有用的信息往往在 stderr 末尾（前几行多为版本横幅）。"""
    combined = ((proc.stderr or "") + "\n" + (proc.stdout or "")).strip()
    if not combined:
        return f"(ffmpeg produced no stderr/stdout; exit code {proc.returncode})"
    if len(combined) <= max_chars:
        return combined
    return "...[truncated head]...\n" + combined[-max_chars:]


def _run_ffmpeg_extract_to_mp3(video_path: Path, audio_path: Path, *, extra_args: list[str]) -> subprocess.CompletedProcess:
    """执行一次 ffmpeg：抽取第一条音轨 → mp3（16kHz 单声道 64kbps）。"""
    cmd = [
        "ffmpeg",
        "-y",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        *extra_args,
        "-i",
        str(video_path),
        "-vn",
        "-map",
        "0:a:0",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "libmp3lame",
        "-b:a",
        "64k",
        str(audio_path),
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def extract_compress_audio(video_path: Path, audio_path: Path) -> Path:
    """用 ffmpeg 从视频中提取音频并压缩为 mp3（16kHz 单声道 64kbps）。

    部分抖音下载的 mp4 时间戳/封装异常，单次默认参数会失败；依次尝试更宽容的解码选项。
    """
    if not video_path.exists() or video_path.stat().st_size < 512:
        raise RuntimeError(f"video file missing or too small: {video_path}")

    n_audio = _ffprobe_audio_stream_count(video_path)
    if n_audio == 0:
        raise RuntimeError(f"no audio stream in video (ffprobe): {video_path}")

    audio_path.parent.mkdir(parents=True, exist_ok=True)
    if audio_path.exists():
        try:
            audio_path.unlink()
        except OSError:
            pass

    # 尝试顺序：默认 → 容错时间戳/损坏包 → 忽略部分解码错误
    attempts: list[tuple[str, list[str]]] = [
        ("default", []),
        (
            "genpts_discardcorrupt",
            ["-fflags", "+genpts+discardcorrupt", "-probesize", "50M", "-analyzeduration", "10M"],
        ),
        (
            "ignore_err",
            [
                "-fflags",
                "+genpts+discardcorrupt",
                "-err_detect",
                "ignore_err",
                "-probesize",
                "50M",
                "-analyzeduration",
                "10M",
            ],
        ),
    ]

    last_proc: subprocess.CompletedProcess | None = None
    for name, extra in attempts:
        proc = _run_ffmpeg_extract_to_mp3(video_path, audio_path, extra_args=extra)
        last_proc = proc
        if proc.returncode == 0 and audio_path.exists() and audio_path.stat().st_size > 0:
            if name != "default":
                logger.info(
                    "[audio] ffmpeg extract ok with %s for %s", name, video_path.name
                )
            return audio_path

    # 最后一招：先抽 16k 单声道 wav（部分流 libmp3lame 直接封装失败，pcm 可过）
    wav_path = audio_path.with_suffix(".tmp.wav")
    try:
        if wav_path.exists():
            wav_path.unlink()
        wav_cmd = [
            "ffmpeg",
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-fflags",
            "+genpts+discardcorrupt",
            "-err_detect",
            "ignore_err",
            "-i",
            str(video_path),
            "-vn",
            "-map",
            "0:a:0",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "wav",
            str(wav_path),
        ]
        wproc = subprocess.run(wav_cmd, capture_output=True, text=True)
        if wproc.returncode == 0 and wav_path.exists() and wav_path.stat().st_size > 0:
            mp3_cmd = [
                "ffmpeg",
                "-y",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(wav_path),
                "-c:a",
                "libmp3lame",
                "-b:a",
                "64k",
                str(audio_path),
            ]
            mproc = subprocess.run(mp3_cmd, capture_output=True, text=True)
            if mproc.returncode == 0 and audio_path.exists() and audio_path.stat().st_size > 0:
                logger.info("[audio] ffmpeg extract ok via wav-pipeline for %s", video_path.name)
                return audio_path
            last_proc = mproc
        else:
            last_proc = wproc
    finally:
        try:
            if wav_path.exists():
                wav_path.unlink()
        except OSError:
            pass

    err = _ffmpeg_error_snippet(last_proc) if last_proc else "no subprocess"
    raise RuntimeError(f"ffmpeg audio extract failed after retries: {err}")


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


def _unlink_local_media_after_oss(video_path: Path, audio_path: Path, link_id: str) -> None:
    """OSS 已成功上传后删除本地 mp4/mp3，节省磁盘；后续 SeedASR 仅使用 OSS URL。"""
    for p in (video_path, audio_path):
        try:
            if p.is_file():
                p.unlink()
                logger.debug("[audio] %s removed local after OSS upload: %s", link_id, p)
        except OSError as e:
            logger.warning("[audio] %s could not remove local file %s: %s", link_id, p, e)


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
    out_dir = _media_dir() / query_id
    out_dir.mkdir(parents=True, exist_ok=True)
    video_path = out_dir / f"{file_basename}.mp4"
    audio_path = out_dir / f"{file_basename}.mp3"

    # 1) 下载视频（缓存 mp4 若 ffprobe 不可读则删后重下）
    if not video_path.exists() or video_path.stat().st_size == 0:
        download_video(link_url, api_base, video_path)
    else:
        ok_cached, reason_cached = _ffprobe_container_valid(video_path)
        if not ok_cached:
            logger.warning(
                "[audio] %s cached mp4 failed ffprobe, re-downloading: %s",
                link_id,
                reason_cached[:500],
            )
            try:
                video_path.unlink()
            except OSError:
                pass
            download_video(link_url, api_base, video_path)

    ok_v, reason_v = _ffprobe_container_valid(video_path)
    if not ok_v:
        raise RuntimeError(
            "downloaded video is not a valid media file (ffprobe). "
            "Common causes: incomplete download, moov atom missing, or non-video body saved as .mp4. "
            f"Details: {reason_v[:1500]}"
        )

    # 2) 用 ffmpeg 抽取 mp3
    if not audio_path.exists() or audio_path.stat().st_size == 0:
        extract_compress_audio(video_path, audio_path)

    # 3) 上传到 OSS（成功后删本地，转写只依赖 OSS URL）
    video_oss_url = oss_upload(video_path, _oss_key_for(query_id, file_basename, "mp4"))
    audio_oss_url = oss_upload(audio_path, _oss_key_for(query_id, file_basename, "mp3"))
    _unlink_local_media_after_oss(video_path, audio_path, link_id)

    # 4) SeedASR 2.0 转写
    transcript = ""
    subtitles: list[dict] = []
    duration_ms = 0
    transcript_source = "seedasr_v2"
    transcript_model = "seedasr_v2"

    parse_note = ""
    try:
        transcript, subtitles, duration_ms = transcribe_with_seedasr_v2(
            audio_oss_url, audio_format="mp3",
        )
    except Exception as asr_exc:
        logger.error("[audio] %s SeedASR 转写失败: %s", link_id, str(asr_exc)[:500], exc_info=True)
        combined = _crawl_combined_text(raw)
        single_fb = (raw.get("raw_text") or raw.get("caption") or "").strip()
        # 合并正文优先于单字段，便于标题+简介都有内容时兜底
        text_candidate = combined if len(combined) >= len(single_fb) else single_fb
        if not text_candidate.strip():
            text_candidate = combined or single_fb

        if _is_asr_no_speech_like(asr_exc) and len(text_candidate.strip()) >= _BGM_FALLBACK_MIN_CHARS:
            transcript = text_candidate.strip()
            transcript_source = "text_content_bgm_no_asr"
            transcript_model = "text_content_bgm_no_asr"
            parse_note = _DEFAULT_BGM_PARSE_NOTE
            subtitles = []
            duration_ms = 0
            logger.warning(
                "[audio] %s ASR 无口播/静音类失败，采用页面文本兜底并标记 done（text_content_bgm_no_asr）",
                link_id,
            )
        elif single_fb:
            transcript = single_fb
            transcript_source = "raw_text_fallback"
            transcript_model = "raw_text_fallback"
            logger.warning("[audio] %s 使用 raw_text 回退", link_id)
        elif combined:
            transcript = combined
            transcript_source = "raw_text_fallback"
            transcript_model = "raw_text_fallback"
            logger.warning("[audio] %s 使用 title+简介合并 raw_text 回退", link_id)
        else:
            raise RuntimeError(f"transcribe failed: {asr_exc}") from asr_exc

    if not transcript:
        return {
            "skipped": True,
            "reason": "empty_transcript",
            "audio_path": audio_oss_url,
            "video_path": video_oss_url,
        }

    out: dict = {
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
    if parse_note:
        out["parse_note"] = parse_note
    return out


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
        logger.warning("[audio] qa_link_video id=%s optimistic lock failed, fallback update", vid)
        execute(
            f"UPDATE qa_link_video SET status = %s{extra} WHERE id = %s",
            tuple(params),
        )
    ol_c = " AND updated_at = %s" if content_updated_at else ""
    params_c = (status, link_id, content_updated_at) if content_updated_at else (status, link_id)
    n = execute(
        f"UPDATE qa_link_content SET video_parse_status = %s WHERE link_id = %s{ol_c}",
        params_c,
    )
    if content_updated_at and n == 0:
        logger.warning("[audio] qa_link_content %s optimistic lock failed, fallback update", link_id)
        execute(
            "UPDATE qa_link_content SET video_parse_status = %s WHERE link_id = %s",
            (status, link_id),
        )


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
    if (result.get("parse_note") or "").strip():
        raw["audio_info"]["parse_note"] = (result.get("parse_note") or "").strip()
    elif "parse_note" in raw.get("audio_info", {}):
        # 新结果无说明时不清除旧字段（按需可改为 pop）
        pass

    if subtitles:
        raw["subtitles"] = subtitles
    elif not _has_meaningful_subtitles(raw):
        raw["subtitles"] = [{"start_time": "", "text": stt_text}]

    shrink_json_object_for_storage(raw, link_id=link_id, label="raw_json_audio_sync")

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
        "status = 'done', transcribed_at = CURRENT_TIMESTAMP, error_message = NULL "
        f"WHERE id = %s{ol_v}",
        params_v,
    )
    if video_updated_at and n == 0:
        logger.warning("[audio] qa_link_video id=%s optimistic lock failed (sync)", vid)


def mark_link_video_text_bgm_done(
    link_id: str,
    *,
    parse_note: str | None = None,
    min_chars: int | None = None,
) -> bool:
    """人工确认：音轨无口播但页面有足量文本时，将 qa_link_video 标为 done。

    - ``stt_text`` 使用标题+简介+caption 合并文本
    - ``transcript_source`` = ``text_content_bgm_no_asr``
    - ``raw_json.audio_info.parse_note`` 记录说明（默认 ``_DEFAULT_BGM_PARSE_NOTE``）
    - 清空 ``qa_link_video.error_message``
    """
    from shared.db import fetch_one

    row = fetch_one(
        "SELECT v.id AS vid, l.query_id, lc.raw_json, v.video_path, v.audio_path "
        "FROM qa_link_video v "
        "JOIN qa_link l ON l.link_id = v.link_id "
        "JOIN qa_link_content lc ON lc.link_id = v.link_id "
        "WHERE v.link_id = %s",
        (link_id,),
    )
    if not row:
        logger.warning("[audio] mark text_bgm_done: link_id=%s not found", link_id)
        return False

    raw = _to_raw_dict(row.get("raw_json"))
    combined = _crawl_combined_text(raw)
    mc = int(min_chars if min_chars is not None else _BGM_FALLBACK_MIN_CHARS)
    if len(combined.strip()) < mc:
        logger.warning(
            "[audio] mark text_bgm_done: combined text too short (%d chars, need >=%d) for %s",
            len(combined.strip()),
            mc,
            link_id,
        )
        return False

    note = (parse_note or _DEFAULT_BGM_PARSE_NOTE).strip()
    vid = int(row["vid"])
    vp = (row.get("video_path") or "").strip() or (raw.get("audio_info") or {}).get("video_path") or ""
    ap = (row.get("audio_path") or "").strip() or (raw.get("audio_info") or {}).get("audio_path") or ""

    result = {
        "stt_text": combined.strip(),
        "transcript_source": "text_content_bgm_no_asr",
        "transcript_model": "text_content_bgm_no_asr",
        "parse_note": note,
        "audio_path": ap,
        "video_path": vp,
        "model_api_file_id": "",
        "model_api_input_type": "input_audio",
        "subtitles": [],
        "duration_ms": 0,
        "skipped": False,
    }
    _sync_to_content(vid, link_id, result, raw, video_updated_at=None, content_updated_at=None)
    logger.info("[audio] marked text_bgm_done for %s", link_id)
    return True


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
                    err_msg = _truncate_error_for_db(str(reason), max_len=2000)
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
