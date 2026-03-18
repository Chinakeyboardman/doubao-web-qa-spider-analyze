#!/usr/bin/env python3
"""
从浏览器读取抖音 Cookie，同步到 Douyin_TikTok_Download_API 的 config.yaml。

用法：
  python integration/sync_douyin_cookie.py           # 默认 Chrome
  python integration/sync_douyin_cookie.py chrome   # 指定浏览器
  python integration/sync_douyin_cookie.py edge     # Edge

注意：macOS 上 Safari 可能因沙盒限制无法读取，建议用 Chrome。
      请先在浏览器中打开 www.douyin.com 并登录，再运行本脚本。
"""

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "Douyin_TikTok_Download_API" / "crawlers" / "douyin" / "web" / "config.yaml"

BROWSERS = ["chrome", "safari", "edge", "firefox", "chromium", "brave"]


def _get_cookie_from_browser(browser: str) -> str:
    """从指定浏览器获取 douyin.com 的 Cookie 字符串。"""
    import browser_cookie3

    name = browser.lower()
    if name not in BROWSERS:
        raise ValueError(f"不支持的浏览器: {browser}，可选: {', '.join(BROWSERS)}")

    loader = getattr(browser_cookie3, name)
    cj = loader(domain_name="douyin.com")
    cookies = {c.name: c.value for c in cj if "douyin" in c.domain}
    if not cookies:
        raise RuntimeError(f"未在 {browser} 中找到 douyin.com 的 Cookie，请先在浏览器中打开抖音并登录")
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


def _update_config(cookie_str: str) -> None:
    """更新 config.yaml 中的 Cookie 字段。"""
    import yaml

    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    config["TokenManager"]["douyin"]["headers"]["Cookie"] = cookie_str

    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2, sort_keys=False)


def main():
    parser = argparse.ArgumentParser(description="从浏览器同步抖音 Cookie 到 Douyin API 配置")
    parser.add_argument("browser", nargs="?", default="chrome", help="浏览器: chrome, safari, edge, firefox 等")
    args = parser.parse_args()

    if not _CONFIG_PATH.exists():
        print(f"错误: 配置文件不存在 {_CONFIG_PATH}")
        sys.exit(1)

    try:
        cookie = _get_cookie_from_browser(args.browser)
        _update_config(cookie)
        print(f"✅ 已从 {args.browser} 同步 Cookie 到 {_CONFIG_PATH.name}")
        print("   请重启 Douyin API 使配置生效: ./integration/run_douyin_api.sh")
    except Exception as e:
        print(f"❌ 失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
