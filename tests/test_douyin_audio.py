from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import integration.douyin_audio_transcriber as transcriber  # noqa: E402

from integration.douyin_audio_transcriber import (  # noqa: E402
    _file_basename,
    _oss_key_for,
    _to_raw_dict,
    batch_process,
    download_video,
    extract_compress_audio,
    mark_link_video_text_bgm_done,
    process_one,
    transcribe_with_seedasr_v2,
)


class _DummyAsrResponse:
    def __init__(self, body: dict, status_header: str = "20000000"):
        self._body = body
        self.headers = {
            "X-Api-Status-Code": status_header,
            "X-Api-Message": "OK",
            "X-Tt-Logid": "test-logid",
        }
        self.text = json.dumps(body, ensure_ascii=False)

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


class DouyinAudioUnitTests(unittest.TestCase):
    def test_to_raw_dict(self):
        self.assertEqual(_to_raw_dict(None), {})
        self.assertEqual(_to_raw_dict({"a": 1}), {"a": 1})
        self.assertEqual(_to_raw_dict('{"x":2}')["x"], 2)
        self.assertEqual(_to_raw_dict("not_json"), {})

    def test_process_one_skip_if_subtitles_exist(self):
        result = process_one(
            link_id="Q0001_L001",
            query_id="Q0001",
            link_url="https://www.iesdouyin.com/share/video/1",
            raw_json={"subtitles": [{"text": "已有字幕"}]},
        )
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "already_has_subtitles")

    def test_process_one_skip_if_already_transcribed(self):
        result = process_one(
            link_id="Q0001_L001",
            query_id="Q0001",
            link_url="https://www.iesdouyin.com/share/video/1",
            raw_json={"stt_text": "已有转写"},
        )
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "already_has_stt_text")

    def test_extract_compress_audio(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            video = td_path / "sample.mp4"
            audio = td_path / "sample.mp3"
            gen_cmd = [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
                "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=16000",
                "-shortest", "-t", "1",
                str(video),
            ]
            proc = subprocess.run(gen_cmd, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or proc.stdout))

            out = extract_compress_audio(video, audio)
            self.assertEqual(out, audio)
            self.assertTrue(audio.exists())
            self.assertGreater(audio.stat().st_size, 0)

    @patch("integration.douyin_audio_transcriber._requests.post")
    def test_transcribe_with_seedasr_v2_mocked(self, mock_post):
        """测试 SeedASR 2.0 submit + query 两步流程。"""
        submit_resp = MagicMock()
        submit_resp.headers = {"X-Api-Status-Code": "20000000", "X-Api-Message": "OK", "X-Tt-Logid": "log1"}

        query_body = {
            "audio_info": {"duration": 3696},
            "result": {
                "text": "这是字节跳动，今日头条母公司。",
                "utterances": [
                    {"start_time": 0, "end_time": 1705, "text": "这是字节跳动，"},
                    {"start_time": 2110, "end_time": 3696, "text": "今日头条母公司。"},
                ],
            },
        }
        query_resp = _DummyAsrResponse(query_body)

        mock_post.side_effect = [submit_resp, query_resp]

        with patch.object(transcriber, "_ASR_APP_ID", "8082927118"), \
             patch.object(transcriber, "_ASR_ACCESS_TOKEN", "token-demo"), \
             patch("time.sleep"):
            text, subtitles, duration_ms = transcribe_with_seedasr_v2(
                "https://oss.example.com/test.mp3", audio_format="mp3"
            )

        self.assertEqual(text, "这是字节跳动，今日头条母公司。")
        self.assertEqual(duration_ms, 3696)
        self.assertEqual(len(subtitles), 2)
        self.assertEqual(mock_post.call_count, 2)

    @patch("integration.douyin_audio_transcriber._requests.post")
    def test_transcribe_with_seedasr_v2_polling_until_done(self, mock_post):
        """测试 SeedASR 轮询：先返回处理中，再返回成功。"""
        submit_resp = MagicMock()
        submit_resp.headers = {"X-Api-Status-Code": "20000000", "X-Api-Message": "OK"}

        processing_resp = MagicMock()
        processing_resp.headers = {"X-Api-Status-Code": "20000001", "X-Api-Message": "Processing"}

        query_body = {
            "audio_info": {"duration": 1000},
            "result": {"text": "轮询成功文本", "utterances": []},
        }
        success_resp = _DummyAsrResponse(query_body)

        mock_post.side_effect = [submit_resp, processing_resp, processing_resp, success_resp]

        with patch.object(transcriber, "_ASR_APP_ID", "8082927118"), \
             patch.object(transcriber, "_ASR_ACCESS_TOKEN", "token-demo"), \
             patch("time.sleep"):
            text, subtitles, duration_ms = transcribe_with_seedasr_v2(
                "https://oss.example.com/test.mp3", audio_format="mp3"
            )

        self.assertEqual(text, "轮询成功文本")
        self.assertEqual(duration_ms, 1000)
        self.assertEqual(mock_post.call_count, 4)

    @patch("httpx.Client")
    def test_download_video_success(self, mock_client_class):
        """测试 download_video 在 API 流式返回视频数据时正确写入。"""
        content = b"fake video content from api"
        mock_stream_resp = MagicMock()
        mock_stream_resp.status_code = 200
        mock_stream_resp.headers = {
            "content-type": "video/mp4",
            "content-length": str(len(content)),
        }
        mock_stream_resp.raise_for_status = MagicMock()
        mock_stream_resp.iter_bytes = lambda chunk_size=None: iter([content])

        mock_stream_cm = MagicMock()
        mock_stream_cm.__enter__ = MagicMock(return_value=mock_stream_resp)
        mock_stream_cm.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.stream.return_value = mock_stream_cm
        mock_client_class.return_value = mock_client

        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "video.mp4"
            from integration.douyin_audio_transcriber import download_video
            result = download_video(
                "https://www.iesdouyin.com/share/video/1",
                "http://localhost:80",
                out_path,
            )
            self.assertEqual(result, out_path)
            self.assertTrue(out_path.exists())
            self.assertEqual(out_path.read_bytes(), content)

    def test_file_basename_with_video_id(self):
        """测试 _file_basename 在 URL 含 video_id 时追加后缀。"""
        self.assertEqual(
            _file_basename("Q0080_L032", "https://www.iesdouyin.com/share/video/7537676583709068584", {}),
            "Q0080_L032_7537676583709068584",
        )
        self.assertEqual(
            _file_basename("Q0001_L001", "https://www.douyin.com/video/7123456789", {}),
            "Q0001_L001_7123456789",
        )

    def test_file_basename_without_video_id(self):
        """测试 _file_basename 在无 video_id 时使用 link_id。"""
        self.assertEqual(_file_basename("Q0001_L001", "https://example.com", {}), "Q0001_L001")

    def test_oss_key_for_format(self):
        """测试 OSS key 格式与 export/media 目录一致。"""
        self.assertEqual(
            _oss_key_for("Q0001", "Q0001_L001_7537676583709068584", "mp4"),
            "export/media/Q0001/Q0001_L001_7537676583709068584.mp4",
        )
        self.assertEqual(
            _oss_key_for("Q0001", "Q0001_L001_7537676583709068584", "mp3"),
            "export/media/Q0001/Q0001_L001_7537676583709068584.mp3",
        )

    @patch(
        "integration.douyin_audio_transcriber._ffprobe_container_valid",
        return_value=(True, ""),
    )
    @patch("integration.douyin_audio_transcriber.oss_upload")
    @patch("integration.douyin_audio_transcriber.transcribe_with_seedasr_v2")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_full_flow(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe,
        mock_oss_upload,
        _mock_ffprobe,
    ):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(video_path, audio_path):
                    self.assertTrue(video_path.exists())
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_oss_upload.side_effect = lambda local, key: f"https://oss.example.com/{key}"
                mock_transcribe.return_value = (
                    "ASR转写文本",
                    [{"start_time": 0, "end_time": 1000, "text": "ASR转写文本"}],
                    1000,
                )

                result = process_one(
                    link_id="Q0001_L001",
                    query_id="Q0001",
                    link_url="https://www.iesdouyin.com/share/video/1",
                    raw_json={"title": "x"},
                )

                self.assertFalse(result["skipped"])
                self.assertEqual(result["transcript_source"], "seedasr_v2")
                self.assertEqual(result["transcript_model"], "seedasr_v2")
                self.assertEqual(result["model_api_input_type"], "input_audio")
                self.assertEqual(len(result["subtitles"]), 1)
                self.assertIn("oss.example.com", result["audio_path"])
                self.assertIn("oss.example.com", result["video_path"])

    @patch(
        "integration.douyin_audio_transcriber._ffprobe_container_valid",
        return_value=(True, ""),
    )
    @patch("integration.douyin_audio_transcriber.oss_upload")
    @patch("integration.douyin_audio_transcriber.transcribe_with_seedasr_v2")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_fallback_to_raw_text(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe,
        mock_oss_upload,
        _mock_ffprobe,
    ):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(_video_path, audio_path):
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_oss_upload.side_effect = lambda local, key: f"https://oss.example.com/{key}"
                mock_transcribe.side_effect = RuntimeError("asr failed")

                result = process_one(
                    link_id="Q0001_L001",
                    query_id="Q0001",
                    link_url="https://www.iesdouyin.com/share/video/1",
                    raw_json={"caption": "兜底文案"},
                )

                self.assertFalse(result["skipped"])
                self.assertEqual(result["transcript_source"], "raw_text_fallback")
                self.assertEqual(result["stt_text"], "兜底文案")
                self.assertEqual(result["transcript_model"], "raw_text_fallback")

    @patch(
        "integration.douyin_audio_transcriber._ffprobe_container_valid",
        return_value=(True, ""),
    )
    @patch("integration.douyin_audio_transcriber.oss_upload")
    @patch("integration.douyin_audio_transcriber.transcribe_with_seedasr_v2")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_bgm_no_speech_uses_combined_text(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe,
        mock_oss_upload,
        _mock_ffprobe,
    ):
        """ASR 报静音/无口播且页面有足够文本时：done + text_content_bgm_no_asr + parse_note。"""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(_video_path, audio_path):
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_oss_upload.side_effect = lambda local, key: f"https://oss.example.com/{key}"
                mock_transcribe.side_effect = RuntimeError("SeedASR 返回静音音频: request_id=x")

                result = process_one(
                    link_id="Q0001_L001",
                    query_id="Q0001",
                    link_url="https://www.iesdouyin.com/share/video/1",
                    raw_json={
                        "title": "标题",
                        "raw_text": "这是一段足够长的简介内容用于测试兜底",
                    },
                )

                self.assertFalse(result["skipped"])
                self.assertEqual(result["transcript_source"], "text_content_bgm_no_asr")
                self.assertEqual(result["transcript_model"], "text_content_bgm_no_asr")
                self.assertIn("parse_note", result)
                self.assertIn("背景音乐", result["parse_note"])
                self.assertIn("标题", result["stt_text"])

    @patch("integration.douyin_audio_transcriber.execute")
    @patch("shared.claim_functions.claim_pending_video_parse_v2")
    @patch("integration.douyin_audio_transcriber.process_one")
    def test_batch_process_updates_raw_json(self, mock_process_one, mock_claim, mock_execute):
        mock_claim.return_value = [
            {
                "vid": 1,
                "query_id": "Q0001",
                "link_id": "Q0001_L001",
                "link_url": "https://www.iesdouyin.com/share/video/1",
                "raw_json": {"title": "a", "video_info": {"play_url": "p"}},
                "model_api_input_type": "input_audio",
                "video_updated_at": None,
                "content_updated_at": None,
            }
        ]
        mock_process_one.return_value = {
            "skipped": False,
            "stt_text": "转写文本",
            "audio_path": "https://oss.example.com/export/media/Q0001/Q0001_L001.mp3",
            "video_path": "https://oss.example.com/export/media/Q0001/Q0001_L001.mp4",
        }

        count = batch_process(query_ids=["Q0001"], concurrency=1)
        self.assertEqual(count, 1)
        self.assertTrue(mock_execute.called)
        self.assertTrue(mock_claim.called)
        sql_calls = [c.args[0] for c in mock_execute.call_args_list]
        self.assertTrue(any("UPDATE qa_link_content" in sql for sql in sql_calls))
        self.assertTrue(any("video_parse_status" in sql for sql in sql_calls))


class MarkBgmTextDoneUnitTests(unittest.TestCase):
    """模拟 Q0233_L014 类数据：仅测逻辑，不访问数据库（fetch_one / _sync_to_content 全部 mock）。"""

    @patch("integration.douyin_audio_transcriber._sync_to_content")
    @patch("shared.db.fetch_one")
    def test_mark_link_video_text_bgm_done_q0233_like_no_db_write(
        self, mock_fetch_one, mock_sync,
    ):
        """与线上一致：音轨无口播、页面有足量文本 → text_content_bgm_no_asr + parse_note；不落库。"""
        raw_like_q0233 = {
            "title": "小象超市冰淇淋🍦亲测封神！平价天花板",
            "raw_text": (
                "小象超市冰淇淋🍦亲测封神！平价天花板\n"
                "我自己作为冰淇淋脑袋\n小象超市这款我直接无限回购！\n"
                "🍨 口感用料\n鲜奶基底超绵密，无添加口感真实。"
            ),
        }
        mock_fetch_one.return_value = {
            "vid": 424242,
            "query_id": "Q0233",
            "raw_json": raw_like_q0233,
            "video_path": "https://oss.example.com/export/media/Q0233/Q0233_L014_7618861518550407094.mp4",
            "audio_path": "https://oss.example.com/export/media/Q0233/Q0233_L014_7618861518550407094.mp3",
        }

        ok = mark_link_video_text_bgm_done("Q0233_L014")

        self.assertTrue(ok)
        mock_fetch_one.assert_called_once()
        mock_sync.assert_called_once()
        _vid, link_id, result, raw = mock_sync.call_args[0]
        self.assertEqual(link_id, "Q0233_L014")
        self.assertEqual(_vid, 424242)
        self.assertEqual(result["transcript_source"], "text_content_bgm_no_asr")
        self.assertEqual(result["transcript_model"], "text_content_bgm_no_asr")
        self.assertIn("背景音乐", result["parse_note"])
        self.assertIn("小象超市", result["stt_text"])
        self.assertIn("parse_note", result)
        # 合并正文应包含标题与正文
        self.assertIn("小象超市冰淇淋", result["stt_text"])
        self.assertEqual(
            result["video_path"],
            "https://oss.example.com/export/media/Q0233/Q0233_L014_7618861518550407094.mp4",
        )

    @patch("shared.db.fetch_one")
    def test_mark_link_video_text_bgm_done_short_text_returns_false(self, mock_fetch_one):
        mock_fetch_one.return_value = {
            "vid": 1,
            "query_id": "Q0233",
            "raw_json": {"title": "短", "raw_text": ""},
            "video_path": "",
            "audio_path": "",
        }
        ok = mark_link_video_text_bgm_done("Q0233_L099", min_chars=15)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
