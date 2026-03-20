---
layout: default
title: 'DOUYIN_API_SETUP'
---

# 抖音视频下载 API 运行指南

> 与主项目 Python 版本隔离，避免环境混乱

## 版本隔离说明

| 组件 | Python | 位置 | 用途 |
|------|--------|------|------|
| 主项目 | 3.14 | `venv/` | pipeline、integration、web-crawler 等 |
| Douyin API | 3.12 | `Douyin_TikTok_Download_API/.venv/` | 8081 下载服务（独立 venv） |

两套环境互不干扰，主项目 `python` / `pip` 不会影响 Douyin API。

---

## 一键运行

### 1. 首次：安装 Python 3.12（一次性）

```bash
brew install python@3.12
```

- 安装到 `/opt/homebrew/opt/python@3.12/`，不覆盖系统 Python
- 主项目仍使用 `venv`（Python 3.14）

### 2. 启动 Douyin API

```bash
./integration/run_douyin_api.sh
```

- 启动前自动从 Chrome 同步抖音 Cookie（解决「获取数据失败」）
- 首次运行会自动创建 `Douyin_TikTok_Download_API/.venv` 并安装依赖
- 端口 8081（可在 .env 配置 DOUYIN_DOWNLOAD_API_PORT），文档：http://localhost:8081/docs

### 3. 小批量运行音频转写（另开终端）

```bash
source venv/bin/activate   # 主项目 venv
python integration/run.py audio-transcribe --query-ids Q0001 --audio-batch-size 1 --audio-concurrency 1
```

---

## 为什么需要 Python 3.12？

- Douyin_TikTok_Download_API 使用 `str | list` 等语法（需 3.10+）
- Python 3.13/3.14 下 pydantic_core 构建失败
- Python 3.12 与项目依赖兼容性最好
