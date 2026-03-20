#!/usr/bin/env python3
"""导出 QA 数据：从数据库生成完整 JSON 与 Markdown 报告，写入 export/ 目录。"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from shared.db import fetch_all, fetch_one
from shared.sql_builder import sb

_EXPORT_DIR = Path(__file__).resolve().parent.parent / "export"


def _ensure_export_dir():
    _EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def _query_stats():
    """返回五张 QA 主表统计。"""
    total = fetch_one("SELECT COUNT(*) AS c FROM qa_query", ())["c"]
    done_q = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_query WHERE status = 'done'", ()
    )["c"]
    pending_q = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_query WHERE status = 'pending'", ()
    )["c"]
    answers = fetch_one("SELECT COUNT(*) AS c FROM qa_answer", ())["c"]
    avg_len = fetch_one(
        f"SELECT {sb.cast_int('COALESCE(AVG(answer_length), 0)')} AS c FROM qa_answer", ()
    )["c"]
    links_total = fetch_one("SELECT COUNT(*) AS c FROM qa_link", ())["c"]
    links_done = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link WHERE status = 'done'", ()
    )["c"]
    links_pending = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link WHERE status = 'pending'", ()
    )["c"]
    links_error = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link WHERE status = 'error'", ()
    )["c"]
    contents = fetch_one("SELECT COUNT(*) AS c FROM qa_link_content", ())["c"]
    video_total = fetch_one("SELECT COUNT(*) AS c FROM qa_link_video", ())["c"]
    video_done = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link_video WHERE status = 'done'", ()
    )["c"]
    video_pending = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link_video WHERE status = 'pending'", ()
    )["c"]
    video_error = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link_video WHERE status = 'error'", ()
    )["c"]
    video_skip = fetch_one(
        "SELECT COUNT(*) AS c FROM qa_link_video WHERE status = 'skip'", ()
    )["c"]
    return {
        "query_total": total,
        "query_done": done_q,
        "query_pending": pending_q,
        "answers_count": answers,
        "answer_avg_length": avg_len,
        "links_count": links_total,
        "links_done": links_done,
        "links_pending": links_pending,
        "links_error": links_error,
        "contents_count": contents,
        "video_count": video_total,
        "video_done": video_done,
        "video_pending": video_pending,
        "video_error": video_error,
        "video_skip": video_skip,
    }


def _serialize_row(row: dict) -> dict:
    """把 psycopg2 返回的 dict 转成可 JSON 序列化的 dict（日期/Decimal 等）。"""
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat() if v else None
        elif hasattr(v, "__float__") and type(v).__name__ == "Decimal":
            out[k] = int(v) if v == int(v) else float(v)
        else:
            out[k] = v
    return out


def build_full_data() -> dict:
    """从数据库组装完整导出结构（用于 JSON）。"""
    stats = _query_stats()
    # 去掉 ORDER BY 避免 MySQL sort buffer；在应用层按 id 排序
    queries = fetch_all(
        "SELECT id, query_id, query_text, category, intent_type, status, created_at, updated_at FROM qa_query"
    )
    queries.sort(key=lambda r: r.get("id") or 0)
    qid_to_answer = {}
    for row in fetch_all(
        "SELECT query_id, answer_text, answer_length, has_citation, citation_count, raw_data, created_at FROM qa_answer"
    ):
        qid_to_answer[row["query_id"]] = _serialize_row(dict(row))
    qid_to_links = {}
    link_rows = fetch_all(
        "SELECT id, query_id, link_id, link_url, platform, content_format, status, publish_time, popularity, fetched_at, created_at FROM qa_link"
    )
    link_rows.sort(key=lambda r: (r.get("query_id", ""), r.get("id") or 0))
    for row in link_rows:
        qid = row["query_id"]
        if qid not in qid_to_links:
            qid_to_links[qid] = []
        qid_to_links[qid].append(_serialize_row(dict(row)))
    link_id_to_content = {}
    for row in fetch_all(
        "SELECT link_id, raw_json, content_json, video_parse_status, status, created_at, updated_at "
        "FROM qa_link_content"
    ):
        c_at = row.get("created_at")
        # 导出用 content_json（结构化）；无则用 raw_json（旧数据兼容）
        cj = row.get("content_json") or row.get("raw_json")
        link_id_to_content[row["link_id"]] = {
            "content_json": cj if isinstance(cj, (dict, list)) else (cj or {}),
            "status": row.get("status"),
            "video_parse_status": row.get("video_parse_status"),
            "created_at": c_at.isoformat() if c_at and hasattr(c_at, "isoformat") else None,
            "updated_at": (
                row.get("updated_at").isoformat()
                if row.get("updated_at") and hasattr(row.get("updated_at"), "isoformat")
                else None
            ),
        }
    link_id_to_video = {}
    for row in fetch_all(
        "SELECT link_id, video_id, play_url, cover_url, duration, video_path, audio_path, "
        "stt_text, subtitles, transcript_model, transcript_source, model_api_file_id, "
        "model_api_input_type, status, error_message, retry_count, fetched_at, transcribed_at, "
        "created_at, updated_at "
        "FROM qa_link_video"
    ):
        link_id_to_video[row["link_id"]] = _serialize_row(dict(row))
    qa_list = []
    for q in queries:
        q_ser = _serialize_row(dict(q))
        ans = qid_to_answer.get(q["query_id"])
        links = qid_to_links.get(q["query_id"], [])
        answer_payload = None
        if ans:
            answer_payload = {
                "text": ans.get("answer_text") or "",
                "length": ans.get("answer_length") or 0,
                "has_citation": ans.get("has_citation") or False,
                "citation_count": ans.get("citation_count") or 0,
            }
        links_payload = []
        for ln in links:
            lk = {
                "link_id": ln["link_id"],
                "link_url": ln["link_url"],
                "platform": ln.get("platform"),
                "content_format": ln.get("content_format"),
                "status": ln.get("status"),
            }
            content_row = link_id_to_content.get(ln["link_id"])
            if content_row and content_row.get("content_json"):
                lk["structured_content"] = content_row["content_json"]
                lk["content_status"] = content_row.get("status")
                lk["video_parse_status"] = content_row.get("video_parse_status")
            video_row = link_id_to_video.get(ln["link_id"])
            if video_row:
                lk["video_resource"] = video_row
            links_payload.append(lk)
        qa_list.append({
            "query_id": q["query_id"],
            "query_text": q["query_text"],
            "category": q.get("category"),
            "intent_type": q.get("intent_type"),
            "status": q.get("status"),
            "answer": answer_payload,
            "links": links_payload,
        })
    return {
        "导出时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "数据概览": {
            "Query总数": stats["query_total"],
            "已采集回答数": stats["answers_count"],
            "平均回答长度": stats["answer_avg_length"],
            "引用链接数": stats["links_count"],
            "链接状态": {"done": stats["links_done"], "pending": stats["links_pending"], "error": stats["links_error"]},
            "已结构化内容数": stats["contents_count"],
            "视频资源数": stats["video_count"],
            "视频状态": {
                "done": stats["video_done"],
                "pending": stats["video_pending"],
                "error": stats["video_error"],
                "skip": stats["video_skip"],
            },
            "待处理Query数": stats["query_pending"],
        },
        "QA数据明细": qa_list,
    }


def build_report_md(full_data: dict) -> str:
    """根据 full_data 生成 Markdown 报告正文。"""
    lines = []
    lines.append("# QA 数据采集成果报告\n")
    lines.append(f"\n导出时间：{full_data['导出时间']}\n")
    lines.append("## 一、数据概览\n")
    ov = full_data["数据概览"]
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| Query 总数 | {ov['Query总数']} |")
    lines.append(f"| 已采集回答 | {ov['已采集回答数']} |")
    lines.append(f"| 平均回答长度 | {ov['平均回答长度']} 字 |")
    lines.append(f"| 引用链接 | {ov['引用链接数']} |")
    lines.append(f"| 链接(done/pending/error) | {ov['链接状态']['done']} / {ov['链接状态']['pending']} / {ov['链接状态']['error']} |")
    lines.append(f"| 已结构化内容 | {ov['已结构化内容数']} |")
    lines.append(f"| 视频资源 | {ov['视频资源数']} |")
    lines.append(
        f"| 视频状态(done/pending/error/skip) | "
        f"{ov['视频状态']['done']} / {ov['视频状态']['pending']} / {ov['视频状态']['error']} / {ov['视频状态']['skip']} |"
    )
    lines.append(f"| 待处理 Query | {ov['待处理Query数']} |")
    lines.append("\n## 二、已采集 QA 明细\n")
    lines.append("| Query ID | 意图类型 | 类目 | Query 文本 | 回答长度 | 引用数 |")
    lines.append("|----------|----------|------|-----------|----------|--------|")
    for item in full_data["QA数据明细"]:
        ans = item.get("answer") or {}
        length = ans.get("length") or 0
        link_count = len(item.get("links") or [])
        lines.append(
            f"| {item['query_id']} | {item.get('intent_type') or '-'} | {item.get('category') or '-'} | {item['query_text'][:40]}{'…' if len(item['query_text']) > 40 else ''} | {length}字 | {link_count} |"
        )
    lines.append("\n## 三、回答内容详情\n")
    for item in full_data["QA数据明细"]:
        ans = item.get("answer")
        if not ans or not ans.get("text"):
            continue
        lines.append(f"\n### {item['query_id']}: {item['query_text'][:50]}{'…' if len(item['query_text']) > 50 else ''}\n")
        lines.append(f"**类目**: {item.get('category') or '-'} | **意图**: {item.get('intent_type') or '-'} | **长度**: {ans.get('length', 0)}字\n")
        lines.append("<details>")
        lines.append("<summary>点击展开回答全文</summary>\n")
        lines.append((ans.get("text") or "").replace("\n\n", "\n\n").strip())
        lines.append("\n</details>")
    lines.append("\n## 四、引用链接与结构化内容摘要\n")
    for item in full_data["QA数据明细"]:
        links = item.get("links") or []
        if not links:
            continue
        lines.append(f"\n### {item['query_id']} 的引用链接\n")
        for ln in links:
            lines.append(f"- **{ln.get('link_id')}** [{ln.get('platform') or '-'}] {ln.get('link_url') or ''} — {ln.get('status') or '-'}")
            if ln.get("structured_content"):
                sc = ln["structured_content"]
                title = (sc.get("结构化内容") or {}).get("标题") or {}
                main = title.get("主标题") or "(无标题)"
                lines.append(f"  - 标题: {main[:80]}{'…' if len(main) > 80 else ''}")
            if ln.get("video_resource"):
                vr = ln["video_resource"]
                lines.append(
                    f"  - 视频: status={vr.get('status') or '-'} | "
                    f"video_id={vr.get('video_id') or '-'} | duration={vr.get('duration') or 0}s"
                )
    lines.append("\n---\n")
    return "\n".join(lines)


def build_full_md(full_data: dict) -> str:
    """完整格式结果用 Markdown 展示（便于阅读的完整数据）。"""
    lines = ["# QA 数据完整导出（Markdown 格式）\n", f"导出时间：{full_data['导出时间']}\n"]
    lines.append("## 数据概览\n")
    lines.append("```json\n")
    lines.append(json.dumps(full_data["数据概览"], ensure_ascii=False, indent=2))
    lines.append("\n```\n")
    lines.append("## QA 数据明细\n")
    for item in full_data["QA数据明细"]:
        lines.append(f"\n### {item['query_id']} | {item['query_text'][:60]}{'…' if len(item['query_text']) > 60 else ''}\n")
        lines.append(f"- 类目: {item.get('category') or '-'} | 意图: {item.get('intent_type') or '-'} | 状态: {item.get('status') or '-'}\n")
        ans = item.get("answer")
        if ans:
            lines.append(f"- 回答长度: {ans.get('length', 0)} 字\n")
            lines.append("<details><summary>回答全文</summary>\n\n")
            lines.append((ans.get("text") or "").strip())
            lines.append("\n\n</details>\n")
        links = item.get("links") or []
        if links:
            lines.append("**引用链接:**\n")
            for ln in links:
                lines.append(f"- [{ln.get('link_id')}] {ln.get('platform') or '-'} | {ln.get('status') or '-'} | {ln.get('link_url') or ''}\n")
                if ln.get("structured_content"):
                    js = json.dumps(ln["structured_content"], ensure_ascii=False, indent=2)
                    lines.append("  ```json\n  ")
                    lines.append(js[:2000] + ("  ...\n  ```\n" if len(js) > 2000 else "\n  ```\n"))
    return "\n".join(lines)


def export_all() -> tuple[Path, Path, Path]:
    """生成：报告 MD、完整 JSON、完整 MD，写入 export/，返回三个文件路径。"""
    _ensure_export_dir()
    full_data = build_full_data()
    json_path = _EXPORT_DIR / "qa_data_export.json"
    report_path = _EXPORT_DIR / "qa_data_report.md"
    full_md_path = _EXPORT_DIR / "qa_data_export.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(full_data, f, ensure_ascii=False, indent=2)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(build_report_md(full_data))
    with open(full_md_path, "w", encoding="utf-8") as f:
        f.write(build_full_md(full_data))
    return report_path, json_path, full_md_path


if __name__ == "__main__":
    rp, jp, mp = export_all()
    print(f"报告:   {rp}")
    print(f"JSON:   {jp}")
    print(f"完整MD: {mp}")
