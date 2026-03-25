"""Paragraph-level noise filter for web crawlers.

Detects and rejects text fragments that are buttons, site chrome, navigation,
copyright notices, social sharing, or other non-article UI elements.
"""

from __future__ import annotations

import re

# ── Exact-substring patterns (fast check) ──────────────────────────────────
_NOISE_SUBSTRINGS: tuple[str, ...] = (
    # Buttons / actions
    "添加到购物袋", "添加到购物车", "加入购物车", "立即购买", "立即使用", "立即下载",
    "立即注册", "立即领取", "立即体验", "免费试用",
    "下载APP", "下载客户端", "打开APP",
    "复制链接", "扫一扫", "一键收藏",
    # Sharing / social
    "分享到QQ", "分享到新浪微博", "分享到微信", "分享到朋友圈",
    # CSDN specific
    "确定要放弃本次机会", "福利倒计时", "订阅专栏", "篇文章 订阅专栏",
    "普通VIP年卡", "收录该内容",
    # Report / moderation
    "举报成功", "该举报信息",
    # Login / auth prompts
    "登录后参与", "登录后查看", "登录后发表", "登录后才能",
    # Loading
    "加载更多", "查看全部回答", "展开全部", "Chargement en cours",
    # Copyright
    "版权协议，转载请附上原文", "版权声明：本文为博主原创",
)

# ── Regex patterns (for structure-dependent noise) ─────────────────────────
_NOISE_REGEXES: list[re.Pattern] = [
    re.compile(r"最新推荐文章于\s*\d{4}"),
    re.compile(r"^\d+\s*点赞\s*踩?\s*\d*\s*收藏"),
    re.compile(r"点赞.*收藏.*评论.*分享.*复制链接"),
    re.compile(r"^\d+\s*人浏览$"),
    re.compile(r"^\d+分钟前\s+\d+人浏览"),
    re.compile(r"^发布时间：\d{4}-\d{2}-\d{2}"),
    re.compile(r"^热门搜索[:：]"),
    re.compile(r"^您现在的位置[:：]"),
    re.compile(r"^首页\s*/\s*\S+\s*/"),
    re.compile(r"©\s*\d{4}"),
    re.compile(r"^Blog\s+Research\s+API\s+Download"),
    re.compile(r"^Buy on Amazon\s+Read Review$"),
    re.compile(r"discounted\s+price\s*¥"),
    re.compile(r"原价\s*¥.*立省\s*¥"),
    re.compile(r"^个人中心\s+兑换中心"),
    re.compile(r"^关注\s+关注\s+\d+\s+点赞"),
    re.compile(r"^Preparing to download"),
    re.compile(r"\d+分钟前\s+\d+人浏览"),
    re.compile(r"微信公众号.*小程序.*小程序"),
]

_SUBSTRING_SET = frozenset(_NOISE_SUBSTRINGS)


def is_noise_paragraph(text: str) -> bool:
    """Return True if the paragraph is likely a button / UI chrome / non-content."""
    if not text:
        return True
    for sub in _SUBSTRING_SET:
        if sub in text:
            return True
    for pat in _NOISE_REGEXES:
        if pat.search(text):
            return True
    return False
