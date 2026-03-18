#!/usr/bin/env python3
"""批量修正 DB 中的 video_path/audio_path：export/Qxxx/ → export/media/Qxxx/"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.db import fetch_all, execute

# 匹配 export/Q0001/ 形式，替换为 export/media/Q0001/
_PATTERN = re.compile(r"export/Q(\d+)/")
_REPLACEMENT = r"export/media/Q\1/"


def _fix_path(path: str | None) -> str | None:
    if not path or not path.strip():
        return path
    if "export/media/" in path:
        return path  # 已是新格式，不重复替换
    new = _PATTERN.sub(_REPLACEMENT, path)
    return new if new != path else path


def migrate_qa_link_video() -> int:
    """更新 qa_link_video 的 video_path、audio_path。"""
    rows = fetch_all(
        "SELECT id, video_path, audio_path FROM qa_link_video "
        "WHERE (video_path IS NOT NULL AND video_path != '' AND video_path LIKE '%%export/Q%%/%%' AND video_path NOT LIKE '%%export/media/%%') "
        "   OR (audio_path IS NOT NULL AND audio_path != '' AND audio_path LIKE '%%export/Q%%/%%' AND audio_path NOT LIKE '%%export/media/%%')"
    )
    count = 0
    for r in rows:
        vp = _fix_path(r.get("video_path"))
        ap = _fix_path(r.get("audio_path"))
        if vp == r.get("video_path") and ap == r.get("audio_path"):
            continue
        execute(
            "UPDATE qa_link_video SET video_path = %s, audio_path = %s WHERE id = %s",
            (vp or r.get("video_path"), ap or r.get("audio_path"), r["id"]),
        )
        count += 1
    return count


def migrate_qa_link_content() -> int:
    """更新 qa_link_content.raw_json 中的 audio_info.video_path、audio_info.audio_path。"""
    rows = fetch_all(
        "SELECT link_id, raw_json FROM qa_link_content WHERE raw_json ? 'audio_info'"
    )
    count = 0
    for r in rows:
        raw = dict(r["raw_json"])
        ai = raw.get("audio_info") or {}
        vp_old, ap_old = ai.get("video_path"), ai.get("audio_path")
        vp = _fix_path(vp_old)
        ap = _fix_path(ap_old)
        if vp == vp_old and ap == ap_old:
            continue
        ai["video_path"] = vp or vp_old or ""
        ai["audio_path"] = ap or ap_old or ""
        raw["audio_info"] = ai
        execute(
            "UPDATE qa_link_content SET raw_json = %s WHERE link_id = %s",
            (json.dumps(raw, ensure_ascii=False), r["link_id"]),
        )
        count += 1
    return count


def main():
    print("开始迁移 media 路径...")
    n1 = migrate_qa_link_video()
    print(f"  qa_link_video: {n1} 条")
    n2 = migrate_qa_link_content()
    print(f"  qa_link_content: {n2} 条")
    print("完成。")


if __name__ == "__main__":
    main()
