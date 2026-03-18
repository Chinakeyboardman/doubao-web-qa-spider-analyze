from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from integration.run import _range_status_snapshot  # noqa: E402


class TestRunSyncVideoStatus(unittest.TestCase):
    @patch("shared.db.fetch_one")
    def test_range_status_snapshot_includes_video(self, mock_fetch_one):
        mock_fetch_one.side_effect = [
            {"pending": 1, "processing": 0, "done": 2, "error": 0},  # qa_query
            {"pending": 3, "processing": 1, "done": 4, "error": 0},  # qa_link
            {"pending": 2, "processing": 1, "done": 5, "error": 1},  # qa_link_video
        ]

        snap = _range_status_snapshot("Q0001", "Q0001")
        self.assertIn("video", snap)
        self.assertEqual(snap["video"]["pending"], 2)
        self.assertEqual(snap["video"]["processing"], 1)
        self.assertEqual(snap["video"]["done"], 5)
        self.assertEqual(snap["video"]["error"], 1)


if __name__ == "__main__":
    unittest.main()
