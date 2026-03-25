"""爬取结果入库前压缩：评论 TopN、超长正文 LLM 去噪、硬上限防止 MySQL JSON/包过大。

- 评论：按 digg_count/点赞 降序，最多保留 20 条（与 docs 需求一致）。
- 正文：当 raw_text+paragraphs 合计过长时，用火山 seed 模型去掉导航/页脚/按钮等噪声，只保留主题相关正文。
- 硬上限：序列化后约 6MB UTF-8 仍超则截断 raw_text（与 migrate_pg_to_mysql 等场景协调）。
- 结构化 ``content_json`` / 抖音回写 ``raw_json``：见 ``shrink_json_object_for_storage``，避免 MySQL JSON 列或
  ``max_allowed_packet`` 导致写入失败。
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# 触发 LLM 清洗的字符数（中文约 4 字≈1token，128k 上下文下留足 system+输出）
_LLM_SANITIZE_TRIGGER_CHARS = 120_000
# 送入模型的最大字符（避免超长 prompt）
_LLM_INPUT_MAX_CHARS = 100_000
# 模型输出正文软上限
_LLM_OUTPUT_SOFT_MAX_CHARS = 500_000
# 单条 JSON 文档（raw / content 等）UTF-8 字节硬上限（低于常见 max_allowed_packet）
_JSON_STORAGE_MAX_BYTES = max(
    256 * 1024,
    int(os.getenv("QA_JSON_MAX_STORAGE_BYTES", str(6 * 1024 * 1024))),
)
_RAW_JSON_MAX_BYTES = _JSON_STORAGE_MAX_BYTES
_TRUNC_SUFFIX = "\n…[已截断以适配 MySQL JSON/包大小上限]"
_MAX_SHRINK_PASSES = 80

_TOP_COMMENTS_N = 20


def _comment_score(c: dict) -> int:
    if not isinstance(c, dict):
        return 0
    for k in ("digg_count", "liked_count", "like_count", "likes"):
        v = c.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return 0


def top_comments_by_engagement(comments: list[Any], *, max_n: int = _TOP_COMMENTS_N) -> list[dict]:
    """保留互动（点赞）最高的前 max_n 条评论。"""
    if not comments or max_n <= 0:
        return []
    scored: list[tuple[int, dict]] = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        scored.append((_comment_score(c), c))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = [dict(x[1]) for x in scored[:max_n]]
    if len(comments) > len(out):
        logger.debug("Comments capped: %d -> %d (by engagement)", len(comments), len(out))
    return out


def _approx_body_chars(raw: dict) -> int:
    rt = len((raw.get("raw_text") or "").strip())
    ps = raw.get("paragraphs") if isinstance(raw.get("paragraphs"), list) else []
    plen = sum(len(str(p)) for p in ps)
    return max(rt, plen)


def _llm_strip_navigation_noise(*, title: str, body: str, link_id: str) -> str:
    """用文本模型去掉网页噪声，只保留与主题相关的正文。"""
    from langchain_core.messages import HumanMessage

    from shared.volcengine_llm import get_chat_model

    chunk = body[:_LLM_INPUT_MAX_CHARS]
    if len(body) > _LLM_INPUT_MAX_CHARS:
        chunk += "\n\n[…下文已省略，以上为抓取长文本的前缀…]"

    prompt = (
        "你是网页正文清洗助手。下列文本来自 HTML 抓取，混入了导航、菜单、面包屑、页脚、"
        "版权声明、登录/注册提示、广告口号、按钮文案、侧边栏重复等与文章主题无关的内容。\n"
        "请只输出与文章主题相关的正文，使用自然段落；可保留必要的小标题；"
        "不要输出你的思考过程、不要复述本说明。\n"
        f"页面标题（供参考）：{title[:800]}\n\n"
        f"抓取草稿：\n{chunk}"
    )
    llm = get_chat_model(temperature=0.15)
    resp = llm.invoke([HumanMessage(content=prompt)])
    text = (getattr(resp, "content", None) or str(resp) or "").strip()
    if len(text) > _LLM_OUTPUT_SOFT_MAX_CHARS:
        text = text[:_LLM_OUTPUT_SOFT_MAX_CHARS] + "\n…[输出已截断]"
    if not text:
        raise RuntimeError("empty LLM output")
    logger.info(
        "raw_content_postprocess: LLM sanitized body for %s (%d -> %d chars)",
        link_id,
        len(body),
        len(text),
    )
    return text


def _apply_body_sanitize(raw: dict, link_id: str) -> None:
    title = (raw.get("title") or "")[:2000]
    ps = raw.get("paragraphs") if isinstance(raw.get("paragraphs"), list) else []
    body = raw.get("raw_text") or ""
    if ps:
        body = "\n\n".join(str(p) for p in ps if p)

    try:
        cleaned = _llm_strip_navigation_noise(title=title, body=body, link_id=link_id)
    except Exception as exc:
        logger.warning("LLM sanitize failed for %s: %s — fallback hard truncate", link_id, exc)
        cleaned = body[:_LLM_OUTPUT_SOFT_MAX_CHARS] + (
            "\n…[超长正文已硬截断，LLM 清洗失败]" if len(body) > _LLM_OUTPUT_SOFT_MAX_CHARS else ""
        )

    raw["raw_text"] = cleaned
    raw["paragraphs"] = [p for p in cleaned.split("\n\n") if p.strip()] if cleaned else []
    meta = raw.setdefault("metadata", {})
    if isinstance(meta, dict):
        meta["raw_sanitized"] = True
        meta["raw_sanitize_reason"] = f"body_len>={_LLM_SANITIZE_TRIGGER_CHARS}"


def _enforce_max_json_bytes(raw: dict, link_id: str) -> None:
    """最后防线：整份 raw 序列化不超过 _RAW_JSON_MAX_BYTES。"""
    try:
        s = json.dumps(raw, ensure_ascii=False)
    except (TypeError, ValueError):
        return
    b = len(s.encode("utf-8"))
    if b <= _RAW_JSON_MAX_BYTES:
        return
    rt = raw.get("raw_text") or ""
    if isinstance(rt, str) and len(rt) > 1000:
        over = b - _RAW_JSON_MAX_BYTES
        cut = max(500, len(rt) - over // 2)
        raw["raw_text"] = rt[:cut] + "\n…[raw_json 字节超限已截断]"
        if isinstance(raw.get("paragraphs"), list):
            raw["paragraphs"] = [raw["raw_text"]]
        meta = raw.setdefault("metadata", {})
        if isinstance(meta, dict):
            meta["raw_truncated_for_storage"] = True
        logger.warning(
            "raw_content_postprocess: hard-truncated raw_text for %s (json_bytes was ~%d)",
            link_id,
            b,
        )


def _json_utf8_byte_length(obj: Any) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError):
        return 1 << 30


def _find_longest_string_ref(obj: Any, depth: int = 0) -> tuple[Any, str | int, str] | None:
    """Locate the longest str value in nested dict/list for in-place truncation."""
    if depth > 40:
        return None
    best: tuple[Any, str | int, str] | None = None
    best_len = 0

    def consider(parent: Any, key: str | int, val: str) -> None:
        nonlocal best, best_len
        if len(val) > best_len:
            best_len = len(val)
            best = (parent, key, val)

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                consider(obj, k, v)
            elif isinstance(v, (dict, list)):
                sub = _find_longest_string_ref(v, depth + 1)
                if sub and len(sub[2]) > best_len:
                    best_len = len(sub[2])
                    best = sub
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                consider(obj, i, v)
            elif isinstance(v, (dict, list)):
                sub = _find_longest_string_ref(v, depth + 1)
                if sub and len(sub[2]) > best_len:
                    best_len = len(sub[2])
                    best = sub
    return best


def shrink_json_object_for_storage(
    obj: Any,
    *,
    max_bytes: int | None = None,
    link_id: str = "?",
    label: str = "json",
) -> Any:
    """将 dict/list 收缩到 UTF-8 序列化后不超过 *max_bytes*（默认 ``QA_JSON_MAX_STORAGE_BYTES``）。

    MySQL JSON 列与单条 SQL 报文受 ``max_allowed_packet`` 限制；LLM 结构化 ``content_json`` 可能极大，
    需在 ``json.dumps`` 入库前调用。优先截断嵌套中最长的字符串字段，多次迭代直至达标或无法再截断。
    """
    max_b = int(max_bytes) if max_bytes is not None else _JSON_STORAGE_MAX_BYTES
    if not isinstance(obj, (dict, list)):
        return obj
    for _pass in range(_MAX_SHRINK_PASSES):
        if _json_utf8_byte_length(obj) <= max_b:
            return obj
        ref = _find_longest_string_ref(obj)
        if ref is None:
            break
        parent, key, val = ref
        if not isinstance(val, str) or len(val) < 64:
            break
        new_len = max(32, len(val) * 2 // 3)
        parent[key] = val[:new_len] + _TRUNC_SUFFIX
        logger.warning(
            "json_storage: truncated long string in %s for %s (pass %d, target max_bytes=%d)",
            label,
            link_id,
            _pass + 1,
            max_b,
        )
    if _json_utf8_byte_length(obj) > max_b:
        logger.error(
            "json_storage: still over max_bytes=%d after shrink for %s (%s) — check data",
            max_b,
            link_id,
            label,
        )
    return obj


def postprocess_raw_for_storage(raw: dict, *, link_id: str = "") -> dict:
    """入库前调用：评论 Top20、超长正文 LLM 清洗、字节硬上限。"""
    if not isinstance(raw, dict):
        return raw
    if raw.get("skipped") or raw.get("error"):
        return raw

    # 1) 评论：按点赞保留 20 条
    cm = raw.get("comments")
    if isinstance(cm, list) and cm:
        raw["comments"] = top_comments_by_engagement(cm, max_n=_TOP_COMMENTS_N)

    # 2) 超长正文：非视频或正文过长均尝试清洗（视频以 desc 为主，过长同样处理）
    approx = _approx_body_chars(raw)
    if approx >= _LLM_SANITIZE_TRIGGER_CHARS:
        _apply_body_sanitize(raw, link_id or "?")

    # 3) 硬上限
    _enforce_max_json_bytes(raw, link_id or "?")

    return raw
