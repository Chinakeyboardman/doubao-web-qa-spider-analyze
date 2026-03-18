"""Unit tests for qa_link_video integration: shared/utils, _sync_to_content, _upsert_link_video,
batch_process (v2), and structuring resilience to video parse failure."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "web-crawler"))
sys.path.insert(0, str(PROJECT_ROOT / "data-clean"))

import integration.douyin_audio_transcriber as transcriber  # noqa: E402

from shared.utils import (  # noqa: E402
    to_raw_dict,
    has_meaningful_subtitles,
    extract_video_id_from_url,
    resolve_video_id,
)
from integration.douyin_audio_transcriber import (  # noqa: E402
    _set_video_status,
    _sync_to_content,
    batch_process,
    process_one,
)


class TestSharedUtils(unittest.TestCase):
    """Tests for shared/utils.py common helpers."""

    def test_to_raw_dict_none(self):
        self.assertEqual(to_raw_dict(None), {})

    def test_to_raw_dict_dict(self):
        self.assertEqual(to_raw_dict({"a": 1}), {"a": 1})

    def test_to_raw_dict_json_string(self):
        self.assertEqual(to_raw_dict('{"x": 2}'), {"x": 2})

    def test_to_raw_dict_invalid_string(self):
        self.assertEqual(to_raw_dict("not_json"), {})

    def test_has_meaningful_subtitles_true(self):
        self.assertTrue(has_meaningful_subtitles({"subtitles": [{"text": "hello"}]}))

    def test_has_meaningful_subtitles_empty(self):
        self.assertFalse(has_meaningful_subtitles({"subtitles": []}))
        self.assertFalse(has_meaningful_subtitles({}))
        self.assertFalse(has_meaningful_subtitles({"subtitles": [{"text": ""}]}))

    def test_extract_video_id_from_url(self):
        self.assertEqual(
            extract_video_id_from_url("https://www.douyin.com/video/7123456789"),
            "7123456789",
        )
        self.assertEqual(extract_video_id_from_url("https://example.com"), "")

    def test_resolve_video_id_from_content(self):
        content = {"video_info": {"aweme_id": "111"}}
        self.assertEqual(resolve_video_id(content, ""), "111")

    def test_resolve_video_id_from_structured(self):
        content = {"结构化内容": {"video_info": {"aweme_id": "222"}}}
        self.assertEqual(resolve_video_id(content, ""), "222")

    def test_resolve_video_id_from_url(self):
        self.assertEqual(
            resolve_video_id({}, "https://www.douyin.com/video/333"),
            "333",
        )


class TestUpsertLinkVideo(unittest.TestCase):
    """Tests for crawler_manager._upsert_link_video."""

    def setUp(self):
        import crawler_manager as cm  # pyright: ignore[reportMissingImports]
        self.cm = cm

    def test_skips_non_douyin(self):
        with patch.object(self.cm, "fetch_all", return_value=[{"platform": "B站"}]) as mfa, \
             patch.object(self.cm, "execute") as mexe:
            self.cm._upsert_link_video("L001", {"video_info": {"aweme_id": "123"}})
            mexe.assert_not_called()

    def test_creates_for_douyin(self):
        with patch.object(self.cm, "fetch_all", return_value=[{"platform": "抖音"}]) as mfa, \
             patch.object(self.cm, "execute") as mexe:
            raw = {
                "video_info": {
                    "aweme_id": "99",
                    "play_url": "http://play.mp4",
                    "cover_url": "http://cover.jpg",
                    "duration": 60,
                },
                "subtitles": [],
            }
            self.cm._upsert_link_video("L001", raw)
            self.assertTrue(mexe.called)
            sql = mexe.call_args[0][0]
            self.assertIn("INSERT INTO qa_link_video", sql)

    def test_skip_status_when_subtitles(self):
        with patch.object(self.cm, "fetch_all", return_value=[{"platform": "抖音"}]) as mfa, \
             patch.object(self.cm, "execute") as mexe:
            raw = {
                "video_info": {"aweme_id": "99"},
                "subtitles": [{"text": "已有字幕"}],
            }
            self.cm._upsert_link_video("L001", raw)
            params = mexe.call_args[0][1]
            self.assertEqual(params[-1], "skip")


class TestSyncToContent(unittest.TestCase):
    """Tests for douyin_audio_transcriber._sync_to_content."""

    @patch.object(transcriber, "execute")
    def test_sync_writes_content_and_video(self, mock_execute):
        result = {
            "stt_text": "转写文本",
            "audio_path": "/tmp/a.mp3",
            "video_path": "/tmp/v.mp4",
            "transcript_source": "audio_file_id",
            "model_api_file_id": "file-123",
            "model_api_input_type": "input_audio",
        }
        raw = {"title": "测试"}
        _sync_to_content(1, "L001", result, raw)

        self.assertEqual(mock_execute.call_count, 2)

        content_sql = mock_execute.call_args_list[0][0][0]
        self.assertIn("UPDATE qa_link_content", content_sql)
        self.assertIn("content_json = NULL", content_sql)

        video_sql = mock_execute.call_args_list[1][0][0]
        self.assertIn("UPDATE qa_link_video", video_sql)
        self.assertIn("transcribed_at", video_sql)

    @patch.object(transcriber, "execute")
    def test_sync_merges_stt_into_raw(self, mock_execute):
        result = {"stt_text": "hello", "audio_path": "", "video_path": ""}
        raw = {"title": "x", "subtitles": []}
        _sync_to_content(1, "L001", result, raw)

        self.assertEqual(raw["stt_text"], "hello")
        self.assertIn("audio_info", raw)
        self.assertEqual(raw["subtitles"], [{"start_time": "", "text": "hello"}])


class TestSetVideoStatus(unittest.TestCase):
    """Tests for douyin_audio_transcriber._set_video_status."""

    @patch.object(transcriber, "execute")
    def test_updates_both_tables(self, mock_execute):
        _set_video_status(1, "L001", "done")
        self.assertEqual(mock_execute.call_count, 2)
        video_sql = mock_execute.call_args_list[0][0][0]
        self.assertIn("qa_link_video", video_sql)
        content_sql = mock_execute.call_args_list[1][0][0]
        self.assertIn("qa_link_content", content_sql)

    @patch.object(transcriber, "execute")
    def test_includes_error_message(self, mock_execute):
        _set_video_status(1, "L001", "error", "something broke")
        video_args = mock_execute.call_args_list[0][0][1]
        self.assertIn("something broke", video_args)


class TestBatchProcessV2(unittest.TestCase):
    """Tests for batch_process using qa_link_video (claim_pending_video_parse_v2)."""

    @patch.object(transcriber, "execute")
    @patch.object(transcriber, "fetch_all")
    @patch.object(transcriber, "process_one")
    def test_batch_process_success(self, mock_process_one, mock_fetch_all, mock_execute):
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
            "audio_path": "",
            "video_path": "export/media/Q0001/Q0001_L001.mp4",
            "transcript_source": "video_file_id",
            "model_api_file_id": "file-1",
            "model_api_input_type": "input_video",
        }

        count = batch_process(query_ids=["Q0001"], concurrency=1)
        self.assertEqual(count, 1)
        self.assertIn("claim_pending_video_parse_v2", mock_fetch_all.call_args[0][0])

        sql_calls = [c.args[0] for c in mock_execute.call_args_list]
        self.assertTrue(any("UPDATE qa_link_content" in s for s in sql_calls))
        self.assertTrue(any("UPDATE qa_link_video" in s for s in sql_calls))

    @patch.object(transcriber, "execute")
    @patch.object(transcriber, "fetch_all")
    @patch.object(transcriber, "process_one")
    def test_batch_process_skip_subtitles(self, mock_process_one, mock_fetch_all, mock_execute):
        mock_fetch_all.return_value = [
            {
                "vid": 2,
                "query_id": "Q0001",
                "link_id": "Q0001_L001",
                "link_url": "https://www.douyin.com/video/1",
                "raw_json": {"subtitles": [{"text": "已有字幕"}]},
                "model_api_input_type": "input_audio",
            }
        ]
        mock_process_one.return_value = {"skipped": True, "reason": "already_has_subtitles"}

        count = batch_process(query_ids=["Q0001"], concurrency=1)
        self.assertEqual(count, 0)
        sql_calls = [c.args[0] for c in mock_execute.call_args_list]
        self.assertTrue(any("qa_link_video" in s for s in sql_calls))

    @patch.object(transcriber, "execute")
    @patch.object(transcriber, "fetch_all")
    @patch.object(transcriber, "process_one")
    def test_old_orders_missing_douyin_data_mark_error(self, mock_process_one, mock_fetch_all, mock_execute):
        """Old-order cases where Douyin media is unavailable should become error, not block flow."""
        mock_fetch_all.return_value = [
            {
                "vid": 10,
                "query_id": "Q0002",
                "link_id": "Q0002_L016",
                "link_url": "https://www.iesdouyin.com/share/video/invalid-1",
                "raw_json": {"title": "old case 1", "video_info": {}},
                "model_api_input_type": "input_audio",
            },
            {
                "vid": 11,
                "query_id": "Q0001",
                "link_id": "Q0001_L007",
                "link_url": "https://www.iesdouyin.com/share/video/invalid-2",
                "raw_json": {"title": "old case 2", "video_info": {}},
                "model_api_input_type": "input_audio",
            },
            {
                "vid": 12,
                "query_id": "Q0001",
                "link_id": "Q0001_L009",
                "link_url": "",
                "raw_json": {"title": "old case 3", "video_info": {}},
                "model_api_input_type": "input_audio",
            },
        ]
        mock_process_one.side_effect = [
            {"skipped": True, "reason": "error:download_failed"},
            {"skipped": True, "reason": "error:resolve_aweme_failed"},
            {"skipped": True, "reason": "missing_link_url"},
        ]

        count = batch_process(query_ids=["Q0001", "Q0002"], concurrency=1, batch_size=3)
        self.assertEqual(count, 0)

        execute_calls = mock_execute.call_args_list
        error_updates = [
            c for c in execute_calls
            if "UPDATE qa_link_video SET status = %s" in c.args[0]
            and c.args[1][0] == "error"
        ]
        self.assertGreaterEqual(len(error_updates), 3)


class TestStructureResilience(unittest.TestCase):
    """Verify that the structurer works with or without video data."""

    def test_format_d_without_stt(self):
        from structurer import ContentStructurer  # pyright: ignore[reportMissingImports]

        structurer = ContentStructurer()
        raw = {
            "title": "测试视频",
            "raw_text": "这是一个测试",
            "video_info": {"duration": 120},
        }
        result = structurer.structure(raw, "视频-无字幕", "L001")
        self.assertEqual(result["内容格式"], "视频-无字幕")
        self.assertIn("结构化内容", result)
        self.assertEqual(result["结构化内容"]["视频标题"], "测试视频")

    def test_format_c_with_subtitles(self):
        from structurer import ContentStructurer  # pyright: ignore[reportMissingImports]

        structurer = ContentStructurer()
        raw = {
            "title": "有字幕视频",
            "subtitles": [{"start_time": "00:00:05", "text": "大家好"}],
            "video_info": {"duration": 60},
            "comments": [],
        }
        result = structurer.structure(raw, "视频-有字幕", "L002")
        self.assertEqual(result["内容格式"], "视频-有字幕")
        subtitles = result["结构化内容"].get("字幕内容", [])
        self.assertTrue(len(subtitles) > 0)

    def test_format_d_with_stt(self):
        from structurer import ContentStructurer  # pyright: ignore[reportMissingImports]

        structurer = ContentStructurer()
        raw = {
            "title": "STT视频",
            "stt_text": "这是语音转写的文本内容",
            "video_info": {"duration": 90},
            "audio_info": {
                "transcript_model": "seed2",
                "processed_at": "2026-01-01 12:00",
            },
        }
        result = structurer.structure(raw, "视频-无字幕", "L003")
        stt = result["结构化内容"].get("语音转文本", [])
        self.assertTrue(len(stt) > 0)


if __name__ == "__main__":
    unittest.main()
