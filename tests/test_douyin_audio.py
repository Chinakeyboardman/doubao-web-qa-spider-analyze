from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import integration.douyin_audio_transcriber as transcriber  # noqa: E402

from integration.douyin_audio_transcriber import (  # noqa: E402
    _to_raw_dict,
    batch_process,
    extract_compress_audio,
    process_one,
    transcribe_with_volcengine_asr,
    transcribe_audio_with_seed2,
)


class _DummyChoice:
    def __init__(self, text: str):
        self.message = type("Msg", (), {"content": text})()


class _DummyResp:
    def __init__(self, text: str):
        self.output_text = text


class _DummyFiles:
    def create(self, **kwargs):
        return type("Uploaded", (), {"id": "file-test-123"})()

    def wait_for_processing(self, file_id: str, **kwargs):
        return None

    def retrieve(self, file_id: str):
        return type("FileObj", (), {"status": "active"})()


class _DummyResponses:
    def __init__(self, text: str):
        self._text = text

    def create(self, **kwargs):
        return _DummyResp(self._text)


class _DummyClient:
    def __init__(self, text: str):
        self.files = _DummyFiles()
        self.responses = _DummyResponses(text)


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
                "ffmpeg",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x240:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=16000",
                "-shortest",
                "-t",
                "1",
                str(video),
            ]
            proc = subprocess.run(gen_cmd, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or proc.stdout))

            out = extract_compress_audio(video, audio)
            self.assertEqual(out, audio)
            self.assertTrue(audio.exists())
            self.assertGreater(audio.stat().st_size, 0)

    @patch("integration.douyin_audio_transcriber.get_seed2_client")
    def test_transcribe_audio_seed2_mocked(self, mock_get_client):
        with tempfile.TemporaryDirectory() as td:
            mp3_path = Path(td) / "a.mp3"
            mp3_path.write_bytes(b"\x00\x01\x02\x03")
            mock_get_client.return_value = _DummyClient("这是转写结果")
            text, file_id, input_type = transcribe_audio_with_seed2(
                mp3_path,
                model="ep-demo",
                media_kind="audio",
            )
            self.assertEqual(text, "这是转写结果")
            self.assertEqual(file_id, "file-test-123")
            self.assertEqual(input_type, "input_audio")

    @patch("requests.post")
    def test_transcribe_with_volcengine_asr_mocked(self, mock_post):
        with tempfile.TemporaryDirectory() as td:
            mp3_path = Path(td) / "a.mp3"
            mp3_path.write_bytes(b"\x01\x02\x03")
            asr_body = {
                "audio_info": {"duration": 3696},
                "result": {
                    "text": "这是字节跳动，今日头条母公司。",
                    "utterances": [
                        {"start_time": 0, "end_time": 1705, "text": "这是字节跳动，"},
                        {"start_time": 2110, "end_time": 3696, "text": "今日头条母公司。"},
                    ],
                },
            }
            mock_post.return_value = _DummyAsrResponse(asr_body)

            with patch.object(transcriber, "_ASR_APP_ID", "8082927118"), patch.object(
                transcriber, "_ASR_ACCESS_TOKEN", "token-demo"
            ):
                text, subtitles, duration_ms = transcribe_with_volcengine_asr(mp3_path)

            self.assertEqual(text, "这是字节跳动，今日头条母公司。")
            self.assertEqual(duration_ms, 3696)
            self.assertEqual(len(subtitles), 2)

    @patch("integration.douyin_audio_transcriber.transcribe_audio_with_seed2")
    @patch("integration.douyin_audio_transcriber.transcribe_with_volcengine_asr")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_prefers_asr_audio(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe_asr,
        mock_transcribe_seed2,
    ):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(video_path, audio_path):
                    self.assertTrue(video_path.exists())
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_transcribe_asr.return_value = (
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
                self.assertEqual(result["transcript_source"], "asr_audio")
                self.assertEqual(result["model_api_input_type"], "input_audio")
                self.assertEqual(result["transcript_model"], "volcengine_asr_bigmodel")
                self.assertEqual(len(result["subtitles"]), 1)
                self.assertTrue(result["audio_path"].endswith("Q0001_L001.mp3"))
                self.assertTrue(result["video_path"].endswith("Q0001_L001.mp4"))
                mock_transcribe_seed2.assert_not_called()

    @patch("integration.douyin_audio_transcriber.transcribe_audio_with_seed2")
    @patch("integration.douyin_audio_transcriber.transcribe_with_volcengine_asr")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_fallback_to_seed2_video(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe_asr,
        mock_transcribe,
    ):
        """ASR 失败后回退到 seed2 视频理解。"""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(_video_path, audio_path):
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_transcribe_asr.side_effect = RuntimeError("asr failed")
                mock_transcribe.return_value = ("seed2转写文本", "file-test-keep", "input_video")

                result = process_one(
                    link_id="Q0001_L001",
                    query_id="Q0001",
                    link_url="https://www.iesdouyin.com/share/video/1",
                    raw_json={"title": "x"},
                )

                self.assertFalse(result["skipped"])
                self.assertEqual(result["transcript_source"], "seed2_video")
                self.assertEqual(result["model_api_input_type"], "input_video")
                self.assertEqual(result["transcript_model"], transcriber._safe_transcript_model_name(transcriber._SEED2_MODEL))
                self.assertEqual(result["stt_text"], "seed2转写文本")
                self.assertEqual(result["subtitles"], [])

    @patch("integration.douyin_audio_transcriber.transcribe_audio_with_seed2")
    @patch("integration.douyin_audio_transcriber.transcribe_with_volcengine_asr")
    @patch("integration.douyin_audio_transcriber.extract_compress_audio")
    @patch("integration.douyin_audio_transcriber.download_video")
    def test_process_one_fallback_to_raw_text_when_all_models_fail(
        self,
        mock_download_video,
        mock_extract_audio,
        mock_transcribe_asr,
        mock_transcribe_seed2,
    ):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            with patch.object(transcriber, "_EXPORT_ROOT", td_path):
                def _download(_url, _api_base, output_path):
                    output_path.write_bytes(b"fake video")
                    return output_path

                def _extract(_video_path, audio_path):
                    audio_path.write_bytes(b"fake mp3")
                    return audio_path

                mock_download_video.side_effect = _download
                mock_extract_audio.side_effect = _extract
                mock_transcribe_asr.side_effect = RuntimeError("asr failed")
                mock_transcribe_seed2.side_effect = RuntimeError("seed2 failed with file-demo")

                result = process_one(
                    link_id="Q0001_L001",
                    query_id="Q0001",
                    link_url="https://www.iesdouyin.com/share/video/1",
                    raw_json={"caption": "兜底文案"},
                )

                self.assertFalse(result["skipped"])
                self.assertEqual(result["transcript_source"], "raw_text_fallback")
                self.assertEqual(result["model_api_input_type"], "")
                self.assertEqual(result["stt_text"], "兜底文案")
                self.assertEqual(result["transcript_model"], "raw_text_fallback")

    @patch("integration.douyin_audio_transcriber.execute")
    @patch("integration.douyin_audio_transcriber.fetch_all")
    @patch("integration.douyin_audio_transcriber.process_one")
    def test_batch_process_updates_raw_json(self, mock_process_one, mock_fetch_all, mock_execute):
        mock_fetch_all.return_value = [
            {
                "vid": 1,
                "query_id": "Q0001",
                "link_id": "Q0001_L001",
                "link_url": "https://www.iesdouyin.com/share/video/1",
                "raw_json": {"title": "a", "video_info": {"play_url": "p"}},
                "model_api_input_type": "input_audio",
            }
        ]
        mock_process_one.return_value = {
            "skipped": False,
            "stt_text": "转写文本",
            "audio_path": "export/media/Q0001/Q0001_L001.mp3",
            "video_path": "export/media/Q0001/Q0001_L001.mp4",
        }

        count = batch_process(query_ids=["Q0001"], concurrency=1)
        self.assertEqual(count, 1)
        self.assertTrue(mock_execute.called)
        self.assertTrue(mock_fetch_all.called)
        self.assertIn("claim_pending_video_parse", mock_fetch_all.call_args[0][0])
        sql_calls = [c.args[0] for c in mock_execute.call_args_list]
        self.assertTrue(any("UPDATE qa_link_content" in sql for sql in sql_calls))
        self.assertTrue(any("video_parse_status" in sql for sql in sql_calls))


@unittest.skipUnless(
    os.getenv("RUN_LIVE_DOUYIN_AUDIO_TESTS") == "1",
    "Set RUN_LIVE_DOUYIN_AUDIO_TESTS=1 to run live network/model tests.",
)
class DouyinAudioLiveTests(unittest.TestCase):
    def test_live_transcribe_one_audio(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            video = td_path / "sample.mp4"
            audio = td_path / "sample.mp3"
            gen_cmd = [
                "ffmpeg",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x240:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=600:sample_rate=16000",
                "-shortest",
                "-t",
                "1",
                str(video),
            ]
            proc = subprocess.run(gen_cmd, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, msg=(proc.stderr or proc.stdout))
            extract_compress_audio(video, audio)
            text, _, _ = transcribe_audio_with_seed2(audio)
            self.assertIsInstance(text, str)


if __name__ == "__main__":
    unittest.main()
