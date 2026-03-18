"""LLM-powered content extraction helpers using Volcengine API.

- Image description generation via the vision model
- Content summarisation via the text model
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from langchain_core.messages import HumanMessage
from shared.volcengine_llm import get_chat_model, get_vision_model, get_seedance_model

logger = logging.getLogger(__name__)


def _extract_llm_text(resp) -> str:
    """兼容 LangChain 不同版本的 invoke 返回格式。"""
    if resp is None:
        return ""
    if hasattr(resp, "content") and resp.content is not None:
        return str(resp.content).strip()
    return str(resp).strip()


def describe_image(image_url: str) -> dict:
    """Generate a description and topic for an image via the vision model.

    Returns {"description": str, "topic": str}.
    """
    if not image_url or len(image_url) < 10:
        return {"description": "", "topic": ""}
    skip_patterns = ("data:image/", "placeholder", "blank.gif", "1x1", "spacer")
    if any(p in image_url.lower() for p in skip_patterns):
        logger.debug("Skip describe_image for placeholder URL: %s", image_url[:60])
        return {"description": "", "topic": ""}
    try:
        llm = get_vision_model(temperature=0.1)
        msg = HumanMessage(
            content=[
                {
                    "type": "image_url",
                    "image_url": {"url": image_url},
                },
                {
                    "type": "text",
                    "text": (
                        "请用一句话描述这张图片的内容，然后用2-4个字给出图片主题分类"
                        "（如：产品展示、使用场景、数据图表、对比图、包装、截图等）。\n"
                        "输出格式：\n描述：<内容>\n主题：<分类>"
                    ),
                },
            ]
        )
        resp = llm.invoke([msg])
        text = _extract_llm_text(resp) or ""
        return _parse_describe_response(text)
    except Exception as exc:
        logger.warning("Image description failed for %s: %s", image_url[:80], exc)
        return {"description": "", "topic": ""}


_MIN_SUMMARISE_CHARS = 80

def summarise_text(text: str, max_length: int = 200) -> str:
    """Summarise a long piece of text using the text model."""
    clean = (text or "").strip()
    if not clean or len(clean) < _MIN_SUMMARISE_CHARS:
        return clean
    if len(clean) <= max_length:
        return clean
    try:
        llm = get_chat_model(temperature=0.2)
        prompt = (
            f"请将以下内容概括为不超过{max_length}字的摘要，保留核心信息：\n\n{text[:3000]}"
        )
        resp = llm.invoke([HumanMessage(content=prompt)])
        return _extract_llm_text(resp)
    except Exception as exc:
        logger.warning("Summarisation failed: %s", exc)
        return text[:max_length] + "…"


def enrich_douyin_video_llm(raw: dict, structured: dict, link_id: str = "") -> dict:
    """抖音视频：用 seedance 模型解析文案和特征，写入 元数据.LLM解析文案与特征。

    前置依赖：raw 须由 douyin-crawler（或 8080 下载接口）拉取，含 title/raw_text/comments 等；
    否则元数据与结构化内容会缺失，此处仅写入说明。
    """
    meta = structured.setdefault("元数据", {})
    title = (raw.get("title") or "").strip()
    raw_text = (raw.get("raw_text") or "").strip()
    comments = raw.get("comments", []) or []
    comments_text = " ".join(
        [str(c.get("text", "")).strip()[:200] for c in comments[:10] if isinstance(c, dict)]
    ).strip()
    subtitles = raw.get("subtitles", []) or []
    has_subtitles = any(
        isinstance(s, dict) and str(s.get("text", "")).strip() for s in subtitles
    )

    total_text_len = len(title) + len(raw_text) + len(comments_text)
    has_meaningful_text = total_text_len >= 15

    if not has_meaningful_text and not has_subtitles:
        meta["LLM解析文案与特征"] = ""
        meta["数据说明"] = "原始数据不足（标题+简介+评论<15字且无字幕），已跳过 seedance 调用。"
        logger.info("[llm_guard] skip seedance for %s: total_text=%d chars, subtitles=%s",
                    link_id, total_text_len, has_subtitles)
        return structured

    input_text = f"标题：{title}\n简介：{raw_text}\n评论摘要：{comments_text}"[:2000]

    try:
        llm = get_seedance_model(temperature=0.2)
        prompt = (
            "针对以下抖音视频文案与评论，请抽取并输出（每行一条）：\n"
            "核心主题：\n关键词：\n内容类型：\n情感倾向：\n"
            "内容：\n" + input_text
        )
        resp = llm.invoke([HumanMessage(content=prompt)])
        text = _extract_llm_text(resp)
        meta["LLM解析文案与特征"] = text[:500]
    except Exception as exc:
        logger.warning("Douyin seedance enrich failed for %s: %s", link_id, exc)
        meta["LLM解析文案与特征"] = ""
        meta["数据说明"] = "seedance 模型抽取失败，请检查 VOLCENGINE_SEEDANCE_MODEL 配置。"
    return structured


def enrich_images(images: list[dict]) -> list[dict]:
    """Add description + topic to each image dict using the vision model.

    Modifies images in place and returns them.
    """
    for img in images:
        url = img.get("图片URL") or img.get("url", "")
        if not url:
            continue
        # Only call LLM if description is missing
        if not img.get("图片描述") and not img.get("description"):
            result = describe_image(url)
            img["图片描述"] = result["description"]
            img["图片主题"] = result["topic"]
    return images


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_describe_response(text: str) -> dict:
    desc = ""
    topic = ""
    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith("描述：") or line.startswith("描述:"):
            desc = line.split("：", 1)[-1].split(":", 1)[-1].strip()
        elif line.startswith("主题：") or line.startswith("主题:"):
            topic = line.split("：", 1)[-1].split(":", 1)[-1].strip()
    if not desc:
        desc = text.strip().split("\n")[0][:100]
    return {"description": desc, "topic": topic}


# ------------------------------------------------------------------
# CLI test
# ------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Test summarisation
    sample = "这是一段很长的文本内容，用于测试摘要功能。" * 20
    print("Summary:", summarise_text(sample, 50))
