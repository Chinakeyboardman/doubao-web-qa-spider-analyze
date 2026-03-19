"""阿里云 OSS 模块单元测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import oss2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from shared.oss import get_public_url, upload_file  # noqa: E402


class TestOssGetPublicUrl(unittest.TestCase):
    """测试 get_public_url。"""

    def test_get_public_url_format(self):
        with patch("shared.oss._BUCKET_NAME", "dev-vedio-file"), \
             patch("shared.oss._ENDPOINT", "oss-cn-shenzhen.aliyuncs.com"):
            url = get_public_url("export/media/Q0001/Q0001_L001.mp3")
            self.assertEqual(
                url,
                "https://dev-vedio-file.oss-cn-shenzhen.aliyuncs.com/export/media/Q0001/Q0001_L001.mp3",
            )

    def test_get_public_url_preserves_key(self):
        with patch("shared.oss._BUCKET_NAME", "my-bucket"), \
             patch("shared.oss._ENDPOINT", "oss-cn-hangzhou.aliyuncs.com"):
            url = get_public_url("path/to/file.mp4")
            self.assertIn("path/to/file.mp4", url)
            self.assertTrue(url.startswith("https://"))


class TestOssUploadFile(unittest.TestCase):
    """测试 upload_file（mock oss2）。"""

    def test_upload_file_raises_if_file_not_exists(self):
        with self.assertRaises(FileNotFoundError) as ctx:
            upload_file("/nonexistent/path.mp3", "export/media/test.mp3")
        self.assertIn("不存在", str(ctx.exception))

    @patch("shared.oss._get_bucket")
    def test_upload_file_returns_public_url(self, mock_get_bucket):
        mock_bucket = MagicMock()
        mock_bucket.head_object.side_effect = oss2.exceptions.NoSuchKey(404, {}, "", {})
        mock_get_bucket.return_value = mock_bucket

        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(b"fake audio content")
            tmp_path = f.name

        try:
            with patch("shared.oss._BUCKET_NAME", "dev-vedio-file"), \
                 patch("shared.oss._ENDPOINT", "oss-cn-shenzhen.aliyuncs.com"):
                url = upload_file(tmp_path, "export/media/Q0001/test.mp3")
                self.assertIn("https://", url)
                self.assertIn("dev-vedio-file", url)
                self.assertIn("export/media/Q0001/test.mp3", url)
                mock_bucket.put_object_from_file.assert_called_once()
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    @patch("shared.oss._get_bucket")
    def test_upload_file_skips_if_same_size_exists(self, mock_get_bucket):
        mock_bucket = MagicMock()
        mock_head = MagicMock()
        mock_head.content_length = 17
        mock_bucket.head_object.return_value = mock_head

        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
            f.write(b"fake audio content")
            tmp_path = f.name

        try:
            with patch("shared.oss._BUCKET_NAME", "dev-vedio-file"), \
                 patch("shared.oss._ENDPOINT", "oss-cn-shenzhen.aliyuncs.com"):
                url = upload_file(tmp_path, "export/media/Q0001/test.mp3")
                self.assertIn("https://", url)
                mock_bucket.put_object_from_file.assert_not_called()
        finally:
            Path(tmp_path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
