"""
对 qa_link 中的链接进行爬虫、收集内容、入库到 qa_link_content 时，按平台走不同解析方式。

- 抖音：抖音视频下载项目拉取 → LLM 解析文案和特征 → 写入 qa_link_content
- 通用：能直接抓取网页摘要和基本信息的网页（如官网），通用爬虫拉取正文+摘要 → 写入 qa_link_content
- 其他已识别平台（B站/小红书/知乎/什么值得买等）：各自专用爬虫或通用爬虫 → 写入 qa_link_content
"""

from __future__ import annotations

# 平台 → 解析方式标识（用于爬虫分发与后续结构化/摘要）
PLATFORM_PARSING_STRATEGY: dict[str, str] = {
    "抖音": "douyin_download_llm",      # 下载项目 + LLM 解析文案和特征
    "B站": "bilibili_api",              # B站 API/下载项目
    "小红书": "xiaohongshu_ssr",        # 小红书 SSR/专用解析
    "通用": "agent_web_summary",         # 能直接抓取网页摘要和基本信息的网页（如官网）
    "知乎": "generic_web",
    "CSDN": "generic_web",
    "什么值得买": "playwright_web",
    "微博": "generic_web",
    "头条": "playwright_web",
    "百度": "generic_web",
    "淘宝": "skip",                     # 商品页暂不抓正文
    "京东": "skip",
    "其他": "agent_web_summary",         # 未识别域名按可抓取页处理（网页摘要+基本信息）
}


def get_parsing_strategy(platform: str) -> str:
    """根据平台返回解析方式，未配置则走 agent_web_summary。"""
    return PLATFORM_PARSING_STRATEGY.get(platform, "agent_web_summary")


def should_crawl_content(platform: str) -> bool:
    """该平台是否抓取正文（False 则仅落 link 不抓内容）。"""
    return get_parsing_strategy(platform) != "skip"


def use_douyin_download_llm(platform: str) -> bool:
    """是否使用「抖音下载项目 + LLM 解析文案和特征」。"""
    return get_parsing_strategy(platform) == "douyin_download_llm"


def use_agent_web_summary(platform: str) -> bool:
    """是否按「直接抓取网页摘要和基本信息」处理（通用站如官网、或未识别站）。"""
    s = get_parsing_strategy(platform)
    return s in ("agent_web_summary", "generic_web", "playwright_web")
