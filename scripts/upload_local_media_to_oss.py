#!/usr/bin/env python3
"""将本地音视频上传到 OSS，并同步更新数据库中的 video_path/audio_path。

用法：
    ./venv/bin/python scripts/upload_local_media_to_oss.py
    ./venv/bin/python scripts/upload_local_media_to_oss.py --limit 5   # 仅处理前 5 条（测试用）
    ./venv/bin/python scripts/upload_local_media_to_oss.py --dry-run   # 仅打印不写入

流程：
    1. 查询 qa_link_video 中 video_path/audio_path 为本地路径（非 http）的记录
    2. 按目录结构（export/media/Qxxx/xxx.mp4）上传到 OSS
    3. 更新 qa_link_video 的 video_path、audio_path 为 OSS URL
    4. 若 qa_link_content.raw_json.audio_info 中存在对应路径，一并更新
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(_PROJECT_ROOT / ".env")

from shared.db import execute, fetch_all
from shared.oss import upload_file as oss_upload, get_public_url


def _is_local_path(p: str | None) -> bool:
    """判断是否为本地路径（非 http URL）。"""
    if not p or not str(p).strip():
        return False
    s = str(p).strip()
    return not (s.startswith("http://") or s.startswith("https://"))


def _local_to_oss_key(local_path: str, project_root: Path) -> str | None:
    """将本地路径转为 OSS key（与 export/media 目录结构一致）。

    支持绝对路径和相对路径。
    例：/path/to/project/export/media/Q0001/Q0001_L002.mp4 -> export/media/Q0001/Q0001_L002.mp4
    """
    p = Path(local_path)
    try:
        resolved = p.resolve()
        root_resolved = project_root.resolve()
        if resolved.is_relative_to(root_resolved):
            rel = resolved.relative_to(root_resolved)
            return str(rel).replace("\\", "/")
        # 绝对路径但不在项目下，尝试提取 export/media/... 部分
        parts = str(resolved).replace("\\", "/").split("/")
        if "export" in parts and "media" in parts:
            idx = parts.index("export")
            return "/".join(parts[idx:])
    except (ValueError, TypeError):
        pass
    # 相对路径
    s = str(local_path).replace("\\", "/").lstrip("/")
    if s.startswith("export/media/"):
        return s
    return None


def process_one(
    vid: int,
    link_id: str,
    video_path: str | None,
    audio_path: str | None,
    project_root: Path,
) -> tuple[str | None, str | None]:
    """处理单条记录：上传本地文件到 OSS，返回新的 video_url, audio_url。"""
    new_video_url = None
    new_audio_url = None

    for local_path, ext in [(video_path, "mp4"), (audio_path, "mp3")]:
        if not _is_local_path(local_path):
            continue
        fp = Path(local_path)
        if not fp.is_absolute():
            fp = project_root / fp
        if not fp.exists() or fp.stat().st_size == 0:
            print(f"  [SKIP] 文件不存在或为空: {fp}")
            continue
        oss_key = _local_to_oss_key(str(fp), project_root)
        if not oss_key:
            print(f"  [SKIP] 无法解析 OSS key: {local_path}")
            continue
        try:
            url = oss_upload(fp, oss_key)
            if ext == "mp4":
                new_video_url = url
            else:
                new_audio_url = url
        except Exception as e:
            print(f"  [ERR] 上传失败 {fp.name}: {e}")

    return new_video_url, new_audio_url


def main():
    import argparse

    parser = argparse.ArgumentParser(description="上传本地音视频到 OSS 并更新数据库")
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 条（测试用）")
    parser.add_argument("--dry-run", action="store_true", help="仅打印不写入")
    args = parser.parse_args()

    project_root = _PROJECT_ROOT
    print("1. 分批查询需要上传的 qa_link_video 记录...")

    batch_size = 200
    last_id = 0
    total_rows = 0
    updated_video = 0
    updated_content = 0
    dry_run = args.dry_run

    def fetch_batch():
        nonlocal last_id
        limit = batch_size
        if args.limit:
            limit = min(batch_size, args.limit - total_rows)
            if limit <= 0:
                return []
        rows = fetch_all(
            """
            SELECT v.id, v.link_id, v.video_path, v.audio_path, l.query_id
            FROM qa_link_video v
            JOIN qa_link l ON v.link_id = l.link_id
            WHERE ((v.video_path IS NOT NULL AND v.video_path != '' AND v.video_path NOT LIKE 'http%%')
               OR (v.audio_path IS NOT NULL AND v.audio_path != '' AND v.audio_path NOT LIKE 'http%%'))
              AND v.id > %s
            ORDER BY v.id
            LIMIT %s
            """,
            (last_id, limit),
        )
        if rows:
            last_id = rows[-1]["id"]
        return rows

    while True:
        rows = fetch_batch()
        if not rows:
            break
        if args.limit and total_rows >= args.limit:
            break
        total_rows += len(rows)
        if total_rows == len(rows):
            print(f"   共约 {len(rows)}+ 条待处理（分批 {batch_size}）")

        for i, r in enumerate(rows):
            vid = r["id"]
            link_id = r["link_id"]
            vp = r.get("video_path")
            ap = r.get("audio_path")
            if (i + 1) % 100 == 0 or i == 0:
                print(f"  [{total_rows - len(rows) + i + 1}] link_id={link_id}")

            new_vp, new_ap = process_one(vid, link_id, vp, ap, project_root)
            if not new_vp and not new_ap:
                continue

            final_vp = new_vp if new_vp else (vp or "")
            final_ap = new_ap if new_ap else (ap or "")

            if not dry_run:
                execute(
                    "UPDATE qa_link_video SET video_path = %s, audio_path = %s WHERE id = %s",
                    (final_vp, final_ap, vid),
                )
            updated_video += 1

            # 更新 qa_link_content.raw_json.audio_info
            content_rows = fetch_all(
                "SELECT link_id, raw_json FROM qa_link_content WHERE link_id = %s",
                (link_id,),
            )
            for cr in content_rows:
                raw = cr["raw_json"]
                if isinstance(raw, str):
                    raw = json.loads(raw) if raw else {}
                elif raw is None:
                    raw = {}
                ai = raw.get("audio_info")
                if not isinstance(ai, dict):
                    continue
                changed = False
                if new_vp and (ai.get("video_path") == vp or _is_local_path(ai.get("video_path"))):
                    ai["video_path"] = new_vp
                    changed = True
                if new_ap and (ai.get("audio_path") == ap or _is_local_path(ai.get("audio_path"))):
                    ai["audio_path"] = new_ap
                    changed = True
                if changed:
                    raw["audio_info"] = ai
                    if not dry_run:
                        execute(
                            "UPDATE qa_link_content SET raw_json = %s WHERE link_id = %s",
                            (json.dumps(raw, ensure_ascii=False), link_id),
                        )
                    updated_content += 1

    print(f"\n完成。qa_link_video 更新 {updated_video} 条，qa_link_content 更新 {updated_content} 条。")
    if dry_run:
        print("(dry-run 模式，未实际写入数据库)")


if __name__ == "__main__":
    main()
