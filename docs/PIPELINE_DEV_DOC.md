---
layout: default
title: 'PIPELINE_DEV_DOC'
---

# 豆包 QA 数据采集与分析系统 — 主流程开发文档

> 版本：v1.1.3 · 最后更新：2026-03-19
> 维护人：chenjiawei

---

## 一、系统总览

本系统围绕「用户搜索 Query → 豆包回答 → 引用链接 → 链接内容结构化」这条主链路，完成大规模 QA 数据的采集、爬取、清洗与归档。

### 1.1 主链路流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Pipeline 主流程                              │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌───────────┐    ┌───────────┐    ┌──────────────┐ │
│  │ Step 1   │    │ Step 2   │    │ Step 2.5  │    │ Step 2.6  │    │ Step 3       │ │
│  │ Collect  │───▶│ Crawl    │───▶│ Enrich    │───▶│ Audio/STT │───▶│ Structure    │ │
│  │ 采集回答  │    │ 爬取链接  │    │ 数据补全   │    │ 视频转写   │    │ 内容结构化   │ │
│  └──────────┘    └──────────┘    └───────────┘    └───────────┘    └──────────────┘ │
│       │               │               │               │                 │           │
│       ▼               ▼               ▼               ▼                 ▼           │
│   qa_query        qa_link        qa_link_content    qa_link_video    qa_link_content │
│   qa_answer       qa_link_content  (JSONB 补全)      +video_parse     (规范化 JSONB) │
│                   (raw JSONB)                                                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 技术栈一览

| 层级 | 技术 | 说明 |
|------|------|------|
| 语言 | Python 3.14 / Node.js 18+ | Python 为主流程，Node.js 仅用于抖音 Puppeteer 爬虫 |
| 数据库 | PostgreSQL 17 / MySQL 8.0+ | 通过 `DB_TYPE` 切换；统一库名 `doubao`，业务表加 `qa_` 前缀 |
| 浏览器自动化 | Playwright (Python) | 豆包网页采集（登录 + 深度思考链接提取） |
| 浏览器自动化 | Patchright (Node.js) | 抖音视频信息抓取（登录态 + 评论/时长等） |
| HTTP 客户端 | httpx (async) | 通用网页爬虫、API 调用 |
| HTML 解析 | BeautifulSoup + lxml | 通用网页、小红书正文提取 |
| LLM 框架 | LangChain + langchain-openai | 火山云 API 封装（文本摘要 / 视频特征抽取） |
| LLM 提供商 | 火山云 ARK API | seed_model / vision_model / seedance_model |
| 配置管理 | python-dotenv | 根目录 `.env` 统一配置 |
| CLI | argparse | `integration/run.py` 统一入口 |

---

## 二、Step 1 — Collect（采集回答）

### 2.1 功能

向豆包发送 Query，获取 AI 回答文本 + 深度思考参考资料链接，写入 `qa_answer` 和 `qa_link`。

### 2.2 两条采集路径

```
                    ┌──────────────────────┐
                    │     Step 1: Collect   │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
        ┌────────────────┐         ┌────────────────┐
        │  Web UI 模式    │         │  API 模式       │
        │  (默认，推荐)    │         │  (--api)        │
        └────────┬───────┘         └────────┬───────┘
                 │                          │
        Playwright 浏览器            OpenAI SDK
        doubao.com/chat/             火山云 ARK API
        ↓                           ↓
        深度思考 + 参考资料链接       回答文本（无链接）
        SMS 自动登录                 web_search tool
```

### 2.3 技术细节

| 项目 | 说明 |
|------|------|
| 入口 | `integration/doubao_web_collector.py` (Web) / `integration/doubao_query.py` (API) |
| 依赖 | Playwright chromium、httpx、psycopg2 |
| 登录方式 | SMS API 自动登录 → 保存 `state.json` → 后续复用 |
| 请求间隔 | Web: 60s/条；API: 180s/条 |
| 超时 | Web: 等待回答最长 240s（轮询文本稳定 5 次即认为完成） |
| 深度思考链接提取 | **逐步汇总+去重**：点击每个“搜索 x 个关键词”步骤后立即抽取链接并累加，避免仅末次面板可见导致遗漏 |
| LLM 使用 | **是 — 火山云 seed_model（仅 API 模式的回答生成）** |

### 2.4 日志案例

```
03:56:47 INFO  integration.pipeline --- Step 1: Collecting answers ---
03:56:47 INFO  integration.doubao_query Collecting answer for Q0001: 低糖水果坚果麦片推荐
03:56:48 INFO  httpx HTTP Request: POST https://ark.cn-beijing.volces.com/api/v3/chat/completions "HTTP/1.1 200 OK"
03:57:38 INFO  integration.doubao_query Done Q0001 — answer 1181 chars, 5 citations
03:57:38 INFO  integration.pipeline Step 1 done: 1 queries processed
```

### 2.5 数据流

```
输入: qa_query (status='pending')
      ┌────────────────────────────────┐
      │ query_id: Q0001                │
      │ query_text: 低糖水果坚果麦片推荐 │
      │ status: pending                │
      └────────────────────────────────┘

输出: qa_answer + qa_link (status='pending')
      ┌─────────────────────────────────────────────┐
      │ qa_answer:                                   │
      │   query_id: Q0001                            │
      │   answer_text: "关于低糖水果坚果麦片..."       │
      │   answer_length: 1181                        │
      │   has_citation: true                         │
      │   citation_count: 5                          │
      └─────────────────────────────────────────────┘
      ┌─────────────────────────────────────────────┐
      │ qa_link (×5):                                │
      │   link_id: Q0001_L001                        │
      │   link_url: https://www.douyin.com/video/... │
      │   platform: 抖音                             │
      │   content_format: 视频-有字幕                  │
      │   status: pending                            │
      └─────────────────────────────────────────────┘
```

### 2.6 时间预估

> **2026-03 起**：Collect 侧已多轮调优（深度思考步骤抽取、页面就绪判断、采集间隔等），单条 query 耗时较早期文档 **约降 30%~40%**（仍强依赖豆包侧生成速度与网络）。

| 场景 | 耗时 |
|------|------|
| 单条 Query (API) | 28~40s |
| 单条 Query (Web UI) | 40~90s（含等待回答生成） |
| 10 条批量 (API) | ~22min（含 180s 间隔） |
| 10 条批量 (Web) | ~15min（含 60s 间隔） |

---

## 三、Step 2 — Crawl（链接爬取）

### 3.1 功能

对 `qa_link` 中 `pending` 的链接，按平台路由到专用爬虫，抓取原始内容写入 `qa_link_content`。

### 3.2 平台路由

```
                    ┌──────────────────────┐
                    │   CrawlerManager     │
                    │   平台路由分发         │
                    └──────────┬───────────┘
                               │
          ┌────────┬───────────┼───────────┬──────────┐
          ▼        ▼           ▼           ▼          ▼
     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌────────┐ ┌────────┐
     │ 抖音    │ │ B站     │ │ 小红书   │ │ 通用   │ │ 淘宝   │
     │ Douyin  │ │Bilibili │ │  XHS    │ │Generic │ │ 京东   │
     │ Crawler │ │ Crawler │ │ Crawler │ │Web     │ │ Skip   │
     └────┬────┘ └────┬────┘ └────┬────┘ └───┬────┘ └────────┘
          │           │           │           │
          ▼           ▼           ▼           ▼
     8080 API    8080 API     httpx+BS4   httpx+BS4
     +DB回退      (hybrid)    SSR解析      HTML解析
```

### 3.3 各爬虫技术细节

| 爬虫 | 文件 | 技术 | 数据源 | 重试策略 |
|------|------|------|--------|---------|
| DouyinVideoCrawler | `web-crawler/crawlers/douyin_video.py` | httpx async | 8080 API → DB 回退 | 400 快速失败，其他 3 次指数退避 |
| BilibiliCrawler | `web-crawler/crawlers/bilibili_video.py` | httpx async | 8080 API hybrid | 3 次指数退避 |
| XiaohongshuCrawler | `web-crawler/crawlers/xiaohongshu.py` | httpx + BS4 | 直接 HTTP + SSR JSON | 3 次指数退避 |
| GenericWebCrawler | `web-crawler/crawlers/generic_web.py` | httpx + BS4 | 直接 HTTP + HTML 解析 | 3 次指数退避 |

### 3.4 抖音爬虫 URL 规范化

抖音链接格式多样，爬虫内部会自动规范化：

```
输入 URL                                         → 候选 URL 列表
─────────────────────────────────────────────────────────────────
https://www.iesdouyin.com/share/video/12345      → [原URL, douyin.com/video/12345, iesdouyin.com/share/video/12345]
https://www.douyin.com/video/12345               → [原URL]
https://v.douyin.com/xxxx                        → [原URL, 尝试解析 video_id]
```

### 3.5 质量保护机制

爬虫在写入 `qa_link_content` 时，会做质量评分保护：

```
新数据写入前:
  if 新数据是空壳(无标题+无评论+无视频信息):
      if 旧数据质量分 > 新数据质量分:
          → 跳过写入，保留旧数据
```

### 3.6 日志案例

```
03:57:38 INFO  crawler_manager Crawling [抖音] Q0001_L001: https://www.douyin.com/video/7614484602547942257
03:57:39 INFO  httpx HTTP Request: GET http://localhost:8080/api/hybrid/video_data "HTTP/1.1 200 OK"
03:57:39 INFO  crawler_manager Crawling [通用] Q0001_L002: https://www.ccreports.com.cn/reports/food/...
03:57:40 INFO  httpx HTTP Request: GET https://www.ccreports.com.cn/... "HTTP/1.1 404 Not Found"
03:57:40 WARNING crawlers.base [通用] attempt 1/3 failed — retrying in 2s
03:57:45 ERROR crawlers.base [通用] all 3 attempts failed
03:57:45 INFO  crawler_manager Skipping 淘宝 link Q0001_L005 (https://tmall.com/...)
03:57:45 INFO  crawler_manager Batch crawl done: 3 / 5 succeeded
```

### 3.7 数据流

```
输入: qa_link (status='pending')
      ┌──────────────────────────────────────────┐
      │ link_id: Q0001_L001                      │
      │ link_url: https://www.douyin.com/video/...│
      │ platform: 抖音                            │
      └──────────────────────────────────────────┘

输出: qa_link_content (JSONB)
      ┌──────────────────────────────────────────┐
      │ { "title": "都是麦片，减脂期...",          │
      │   "content_type": "video",               │
      │   "raw_text": "...",                     │
      │   "video_info": {                        │
      │     "aweme_id": "7614484602547942257",   │
      │     "duration": 45,                      │
      │     "cover_url": "https://...",          │
      │     "play_url": "https://..."            │
      │   },                                     │
      │   "comments": [...],                     │
      │   "metadata": {                          │
      │     "author": "健康测评师",               │
      │     "digg_count": 319,                   │
      │     "comment_count": 41, ...             │
      │   }                                      │
      │ }                                        │
      └──────────────────────────────────────────┘
```

### 3.8 时间预估

> **2026-03 起**：路由与 httpx 超时、抖音 400 快速失败、域名识别等已优化，**单条 link 常见路径**较此前 **约降 30%~50%**。

| 场景 | 耗时 |
|------|------|
| 单条链接（8080 API 正常） | **1~3s**（常见） |
| 单条链接（通用网页） | 0.8~2.5s |
| 单条链接（404 重试 3 次） | ~14s（2+4+8s 退避，与重试策略相关） |
| 10 条链接混合批量（并发 3） | **12~45s**（视平台与失败率） |
| LLM 使用 | **否** |

---

## 四、Step 2.5 — Enrich（抖音数据补全）

### 4.1 功能

利用 `douyin-crawler`（Node.js Puppeteer 爬虫）已经抓取到本地 DB 的 `douyin_videos` + `douyin_comments` 数据，补全 `qa_link_content` 中缺失的字段。

### 4.2 工作原理

```
┌─────────────────┐     video_id 匹配     ┌──────────────────┐
│ qa_link_content  │ ◀──────────────────── │ douyin_videos    │
│ (可能缺失数据)    │                       │ douyin_comments  │
└────────┬────────┘                       └──────────────────┘
         │                                       ▲
         │  merge_all()                          │
         ▼                                       │
┌─────────────────┐                    ┌──────────────────┐
│ 补全后的 JSONB    │                    │ douyin-scraper.js│
│ title/comments/  │                    │ Patchright 浏览器 │
│ duration/author  │                    │ 直接抓取抖音页面   │
└─────────────────┘                    └──────────────────┘
```

### 4.3 技术细节

| 项目 | 说明 |
|------|------|
| 入口 | `integration/douyin_data_merger.py` → `DouyinDataMerger.merge_all()` |
| 依赖 | psycopg2（直接读 `douyin_videos` / `douyin_comments` 表） |
| 匹配方式 | 从 `qa_link.link_url` 或已有 `content_json` 中提取 `video_id` |
| 合并策略 | 字段级择优：优先取非空值，数字优先取较大值 |
| 质量保护 | 合并后做 `_content_quality_score` 评分，低于旧分则跳过 |
| LLM 使用 | **否** |

### 4.4 补全的字段

| 字段 | 来源 | 说明 |
|------|------|------|
| title | douyin_videos.title | 视频标题 |
| author | douyin_videos.author | 作者名（含清洗） |
| duration | douyin_videos.raw_data → video.duration | 视频时长（ms→s） |
| comments | douyin_comments | 评论内容、点赞数、用户名 |
| publish_time | douyin_videos.raw_data | 发布时间 |
| popularity | douyin_videos (likes/comments/favorites/shares) | 热度指标组合 |
| cover_url / play_url | douyin_videos.raw_data | 封面和播放地址 |

### 4.5 日志案例

```
03:58:39 INFO  integration.pipeline --- Step 2.5: Enriching Douyin data from crawler DB ---
03:58:39 INFO  douyin_data_merger Scanning 3 Douyin links for enrichment
03:58:39 INFO  douyin_data_merger Enriched Q0001_L001 with 41 comments from douyin_videos
03:58:39 INFO  douyin_data_merger Skip Q0001_L003: no matching video_id found
03:58:39 INFO  integration.pipeline Step 2.5 done: 2 Douyin links enriched
```

### 4.6 时间预估

| 场景 | 耗时 |
|------|------|
| 单条补全 | <100ms（纯 DB 查询） |
| 10 条批量 | <1s |
| 全量扫描（~50 条抖音链接） | <3s |

### 4.7 Step 2.6 — Audio Transcribe（抖音视频音频转写）

在 `crawl` 和 `structure` 之间新增视频解析环节，核心状态写入 `qa_link_video`，并同步 `qa_link_content.video_parse_status`。  
关键约束：即使视频解析失败（例如无视频或下载失败），也不会阻塞后续 `structure`。

| 项目 | 说明 |
|------|------|
| 入口 | `integration/douyin_audio_transcriber.py` |
| 任务领取 | `claim_pending_video_parse_v2()`（`FOR UPDATE SKIP LOCKED`） |
| 状态机 | `pending → processing → done/error/skip` |
| 回写字段 | `stt_text`、`subtitles`、`transcribed_at`、`error_message` |
| LLM 使用 | **否**（调用转写 API，不走大模型摘要） |

**实测小批耗时（2026-03-20，多轮优化后）**

> 含：**流式下载**、大文件读超时、ffmpeg 多档容错、错误信息尾部截取、BGM/无口播时文本兜底等；长尾失败与重试明显减少。

| 场景 | 耗时 |
|------|------|
| 单条抖音视频（下载+提音+转写） | **25~75s**（常见短视频；原约 60~120s） |
| `--audio-concurrency 2` 两条并行 | 墙钟约为单条的 **0.8~1.2×**（受 ASR 配额与视频时长影响，非严格减半） |
| 命中无视频/不可转写 | **2~8s**（快速标记 `skip/error`） |

---

## 五、Step 3 — Structure（内容结构化）

### 5.1 功能

将 `qa_link_content` 中的原始 JSON 按文档规范（格式 A/B/C/D）转为标准化结构，并按平台做 LLM 后处理。

### 5.2 格式分类

```
┌──────────────────────────────────────────────────────┐
│                 ContentStructurer                     │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌────────┐│
│  │ 格式A    │  │ 格式B    │  │ 格式C  │  │ 格式D  ││
│  │ 图文无序  │  │ 图文有序  │  │ 视频   │  │ 视频   ││
│  │ 抖音图文  │  │ 知乎/官网│  │ 有字幕  │  │ 无字幕  ││
│  │ 小红书    │  │ 什么值得买│  │ B站    │  │ 抖音   ││
│  └──────────┘  └──────────┘  └────────┘  └────────┘│
│                                                      │
│  ──────── LLM 后处理层（可选） ────────               │
│                                                      │
│  ┌──────────────────────┐  ┌──────────────────────┐ │
│  │ 抖音: seedance 模型   │  │ 通用: seed_model     │ │
│  │ 文案特征抽取          │  │ 文本摘要生成          │ │
│  │ enrich_douyin_video  │  │ summarise_text       │ │
│  │ _llm()              │  │ ()                   │ │
│  └──────────────────────┘  └──────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### 5.3 技术细节

| 项目 | 说明 |
|------|------|
| 入口 | `data-clean/structurer.py` → `ContentStructurer.structure()` |
| 后处理 | `integration/pipeline.py` → `_post_process_by_platform()` |
| 格式映射 | `integration/citation_parser.py` → `determine_content_format()` |
| LLM 使用 | **是 — 两个调用点（见下方 LLM 清单）** |

### 5.4 结构化输出示例（格式 A — 抖音图文）

```json
{
  "链接ID": "Q0001_L001",
  "内容格式": "图文A",
  "结构化内容": {
    "标题": {"文本": "都是麦片，减脂期你最爱哪一款？", "字符位置": "0-16"},
    "正文": [
      {"段落序号": 1, "内容": "今天测了4个牌子的麦片...", "字符数": 120}
    ],
    "图片": [],
    "标签": ["麦片", "减脂"],
    "评论": [
      {"序号": 1, "内容": "桂格那个确实不错", "点赞数": 52, "发布时间": "2026-03-10"}
    ]
  },
  "元数据": {
    "作者": "健康测评师",
    "发布时间": "2026-03-08",
    "点赞数": 319,
    "评论数": 41,
    "收藏数": 28
  }
}
```

### 5.5 日志案例

```
03:58:39 INFO  integration.pipeline --- Step 3: Structuring content ---
03:58:39 INFO  structurer Unknown content_format '图文A' for Q0008_L002, using 图文B fallback
03:58:40 INFO  llm_extractor Douyin seedance enrich for Q0001_L001 — 486 chars
03:58:42 INFO  llm_extractor Summarise 通用 text for Q0001_L002 — 287 chars
03:58:42 INFO  integration.pipeline Step 3 done: 4 items structured
```

### 5.6 时间预估

> **2026-03 起**：默认 `structure-concurrency=5`，多路并发后 **10 条含 LLM** 墙钟显著低于早期「串行 30~80s」量级。

| 场景 | 耗时 |
|------|------|
| 单条结构化（纯规则，无 LLM） | <50ms |
| 单条结构化（含 LLM 后处理） | **2~5s**（常见） |
| 10 条含 LLM 后处理 | **18~45s**（并发 5 时；串行约 20~50s） |
| 10 条纯规则（关闭 LLM） | <1s |

---

## 六、辅助流程

### 6.1 抖音独立爬虫（douyin-crawler）

```
┌──────────────────────────────────────────┐
│  douyin-scraper.js (Node.js + Patchright)│
│                                          │
│  模式1: 推荐流模式                        │
│    node douyin-scraper.js 5              │
│    → 从推荐流抓取 5 个视频                 │
│                                          │
│  模式2: 指定 URL 模式                     │
│    node douyin-scraper.js --url <URL>    │
│    → 抓取指定视频信息                      │
│                                          │
│  输出: douyin_videos + douyin_comments   │
│  持久化: PostgreSQL (doubao 库)           │
│  登录态: Chrome user data dir 持久化      │
└──────────────────────────────────────────┘
```

| 项目 | 说明 |
|------|------|
| 文件 | `douyin-crawler/douyin-scraper.js` (~1226 行) |
| 依赖 | patchright (Playwright fork)、pg |
| 评论门槛 | 推荐流模式: `MIN_COMMENTS_THRESHOLD`（默认 5000）；指定 URL 模式: 无门槛 |
| LLM 使用 | **否** |

### 6.2 数据导出

```bash
python integration/run.py export
# 输出: export/qa_data_report.md + qa_data_export.json + qa_data_export.md
```

| 项目 | 说明 |
|------|------|
| 文件 | `integration/export_qa.py` |
| 依赖 | psycopg2 |
| LLM 使用 | **否** |

---

## 七、LLM 使用清单

### 7.1 完整调用点

| # | 调用位置 | 函数 | 模型 | 用途 | 必要性 |
|---|---------|------|------|------|--------|
| 1 | `integration/doubao_query.py` | `_call_api()` | seed_model | 生成 Query 回答 | **核心** — 这是业务的入口 |
| 2 | `data-clean/llm_extractor.py` | `enrich_douyin_video_llm()` | seedance_model | 抖音视频文案特征抽取 | 可选 — 可用规则替代 |
| 3 | `data-clean/llm_extractor.py` | `summarise_text()` | seed_model | 通用网页文本摘要 | 可选 — 可截断替代 |
| 4 | `data-clean/llm_extractor.py` | `describe_image()` | vision_model | 图片描述生成 | 可选 — 当前主流程未强依赖 |

### 7.2 LLM 成本分布（基于实测 2 条 Query）

```
┌────────────────────────────────────────────────────────────────┐
│  LLM 调用成本分布                                               │
│                                                                │
│  Step 1 (Collect)    ████████████████████████████   60%        │
│  Step 3 (#2 抖音)    ██████████████                 30%        │
│  Step 3 (#3 摘要)    █████                          10%        │
│  Step 2 (Crawl)      无 LLM                         0%        │
│  Step 2.5 (Enrich)   无 LLM                         0%        │
└────────────────────────────────────────────────────────────────┘
```

### 7.3 降本策略

| 策略 | 影响范围 | 预估节省 |
|------|---------|---------|
| 关闭 #2 抖音特征抽取（已有结构化数据足够） | Step 3 | 30% |
| 关闭 #3 文本摘要（直接截断正文前 300 字） | Step 3 | 10% |
| #1 换低成本模型（如 doubao-lite） | Step 1 | 40~50% |
| 添加 LLM 调用开关（env 配置） | 全局 | 灵活控制 |

---

## 八、数据库 Schema

> DDL 脚本按数据库类型分目录存放：`init-db/postgresql/` 和 `init-db/mysql/`。
> 使用 `python scripts/run_migrations.py init` 自动选择对应脚本。

### 8.1 核心表关系

```
qa_query (1) ───▶ (N) qa_answer
    │
    └──── (1) ───▶ (N) qa_link ───▶ (1) qa_link_content
                       │                  ▲
                       └──────▶ qa_link_video
                                          │
                   douyin_videos ──────────┘ (video_id 关联补全)
                   douyin_comments ────────┘
```

### 8.2 表结构速览

| 表名 | 主要字段 | 作用 |
|------|---------|------|
| `qa_query` | query_id, query_text, category, status | Query 池 |
| `qa_answer` | query_id, answer_text, citation_count, raw_data | 豆包回答 |
| `qa_link` | link_id, link_url, platform, content_format, status | 引用链接 |
| `qa_link_content` | link_id, raw_json, content_json, video_parse_status | 链接原始内容 + 结构化内容 |
| `qa_link_video` | link_id, video_id, play_url, stt_text, subtitles, status | 抖音视频资源与转写状态 |
| `douyin_videos` | video_id, title, author, likes, raw_data | 抖音视频（独立爬虫） |
| `douyin_comments` | video_id, username, content, likes | 抖音评论（独立爬虫） |

### 8.3 状态机

```
qa_query:  pending ──▶ processing ──▶ done
                                  └──▶ error ──(retry)──▶ pending

qa_link:   pending ──▶ processing ──▶ done
                                  └──▶ error ──(retry)──▶ pending

qa_link_video: pending ──▶ processing ──▶ done
                                       ├──▶ error ──(retry)──▶ pending
                                       └──▶ skip
```

---

## 九、数据库双引擎适配

### 9.1 架构概览

系统同时支持 PostgreSQL 和 MySQL，通过 `.env` 中的 `DB_TYPE` 环境变量切换。

```
┌────────────────────────────────────────────────────────────────────┐
│  调用层（15+ 文件，不直接依赖特定数据库驱动）                          │
│  doubao_query / doubao_web_collector / crawler_manager / pipeline  │
│  douyin_data_merger / douyin_audio_transcriber / run / export ...  │
└────────────────┬──────────────────────┬────────────────────────────┘
                 │                      │
                 ▼                      ▼
  ┌──────────────────────┐   ┌─────────────────────────┐
  │  shared/db.py        │   │  shared/sql_builder.py   │
  │  公共 API（不变）      │   │  跨方言 SQL 构建器        │
  │  execute / fetch_all │   │  sb.upsert_suffix()     │
  │  fetch_one / ...     │   │  sb.expand_any()        │
  └──────────┬───────────┘   │  sb.count_filter() ...  │
             │               └─────────────────────────┘
             ▼
  ┌──────────────────────┐
  │  shared/db_backend/  │
  │  get_backend() 工厂   │
  ├──────────┬───────────┤
  │ PG 后端  │ MySQL 后端 │
  │ psycopg2 │  PyMySQL   │
  └──────────┴───────────┘
```

### 9.2 核心组件

| 文件 | 作用 |
|------|------|
| `shared/config.py` | `CONFIG["db_type"]` = `postgresql` 或 `mysql` |
| `shared/db_backend/__init__.py` | `get_backend()` 工厂、`get_dialect()` 快捷函数 |
| `shared/db_backend/base.py` | `DBBackend` 抽象基类（连接/游标/JSON 适配） |
| `shared/db_backend/postgresql.py` | psycopg2 + RealDictCursor |
| `shared/db_backend/mysql.py` | PyMySQL + DictCursor + JSON 字符串自动反序列化 |
| `shared/sql_builder.py` | 方言 SQL 片段构建器（单例 `sb`） |
| `shared/claim_functions.py` | `claim_pending_*` 存储函数的 Python 等价实现 |

### 9.3 SQL 方言对照

| 功能 | PostgreSQL | MySQL |
|------|-----------|-------|
| 占位符 | `%s` | `%s`（两者一致） |
| 数组匹配 | `col = ANY(%s)` | `col IN (%s,%s,...)` |
| Upsert | `ON CONFLICT (k) DO UPDATE SET c = EXCLUDED.c` | `ON DUPLICATE KEY UPDATE c = VALUES(c)` |
| RETURNING | `UPDATE ... RETURNING col` | 不支持（事务内 SELECT 替代） |
| INTERVAL | `CURRENT_TIMESTAMP - INTERVAL '2 hours'` | `DATE_SUB(NOW(), INTERVAL 2 HOUR)` |
| 条件计数 | `COUNT(*) FILTER (WHERE cond)` | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` |
| JSON 取值 | `col->>'key'` | `JSON_UNQUOTE(JSON_EXTRACT(col, '$.key'))` |
| JSON 数组长度 | `jsonb_array_length(expr)` | `JSON_LENGTH(expr)` |
| 整数转换 | `(expr)::INTEGER` | `CAST(expr AS SIGNED)` |
| UPDATE JOIN | `UPDATE t SET ... FROM t2 WHERE ...` | `UPDATE t JOIN t2 ON ... SET ...` |

### 9.4 切换数据库

```bash
# .env 配置（选其一）
DB_TYPE=postgresql
DB_TYPE=mysql

# 初始化对应数据库
python scripts/run_migrations.py init

# 运行迁移
python scripts/run_migrations.py migrate_v8
```

### 9.5 MySQL 注意事项

- 要求 **MySQL 8.0+**（依赖 `FOR UPDATE SKIP LOCKED`、`CHECK` 约束、`JSON` 函数）
- PG JSONB 支持 GIN 索引，MySQL JSON 无等价索引；如有大量 JSON 查询可考虑 generated column + 普通索引
- PG 默认 READ COMMITTED，MySQL InnoDB 默认 REPEATABLE READ；`claim_pending_*` 函数在两种隔离级别下行为一致（已验证 `SKIP LOCKED` 语义）
- psycopg2 自动将 JSONB 转为 Python dict；PyMySQL 返回 JSON 为字符串 → `MySQLBackend.adapt_row()` 自动检测并 `json.loads()`

---

## 十、CLI 命令速查

```bash
# 全流程（推荐，指定 query 范围）
python integration/run.py run --query-ids Q0001,Q0002

# 分步执行
python integration/run.py collect --query-ids Q0001,Q0002
python integration/run.py crawl --query-ids Q0001,Q0002 --batch-size 50
python integration/run.py enrich-douyin --query-ids Q0001,Q0002
python integration/run.py audio-transcribe --query-ids Q0001,Q0002 --audio-batch-size 5
python integration/run.py structure --query-ids Q0001,Q0002
python integration/run.py structure --link-ids Q0001_L001,Q0001_L002

# 抖音独立爬虫
PGDATABASE=doubao node douyin-crawler/douyin-scraper.js --url https://www.douyin.com/video/xxx

# 辅助命令
python integration/run.py status
python integration/run.py retry
python integration/run.py export
python integration/run.py regenerate-content --link-ids Q0001_L001 --force
python integration/run.py web-login --manual
```

---

## 十一、端到端实测性能基线

> **历史样本（2 条 Query：Q0001, Q0002）** 的分段耗时，**占比仍有参考意义**；各环节在 2026-03 前后已多轮优化，**绝对秒数整体低于下表加总**，请以 **§11.1 优化后观测区间** 为准做容量规划。

| 步骤 | 耗时（历史样本） | 占比 | 瓶颈分析 |
|------|------|------|---------|
| Step 1: Collect | 98.84s | 35% | LLM 回答生成 + 深度思考搜索 |
| Step 2: Crawl | 40.79s | 14% | 抖音 API + 404 退避（已减轻） |
| Step 2.5: Enrich | 0.42s | <1% | 纯 DB 操作，极快 |
| Step 3: Structure | 142.08s | 50% | LLM（现已默认多路并发，墙钟显著缩短） |
| **总计** | **282.8s** | 100% | |

### 11.1 最新环节耗时估算（2026-03-20，优化后观测）

> 适用于当前默认配置（`crawl-concurrency=3`，`audio-concurrency=1~2`，`structure-concurrency=5`）。**单 query / 单 link** 常见路径较 2026-03-17 文档数据 **整体约降 30%~50%**（视视频时长、失败率、LLM 并发与火山侧延迟而定）。

| 环节 | 估算耗时 | 备注 |
|------|---------|------|
| Collect（Web） | **35~75s** / query | 深度思考与页面等待已调优 |
| Crawl | **约 1~3s** / link（抖音 API 正常） | 混合批总量：同类 20+ link 批次常见 **35~55s**（优于早期 ~68s） |
| Enrich-Douyin | <100ms / link | 纯 DB merge，通常不是瓶颈 |
| Audio-Transcribe | **25~75s** / 抖音视频 | 流式下载 + ASR；失败项 **2~8s** 快速返回 |
| Structure | **2~5s** / link（含 LLM） | 并发 5；可按 `--link-ids` 小批复盘 |

```
耗时分布（定性；绝对值已随优化下降，Structure 仍为 LLM 主段）:

Step 1 (Collect)   ████████████████████                      ~35%
Step 2 (Crawl)     ████████                                  ~14%
Step 2.5 (Enrich)  ▏                                         <1%
Step 3 (Structure) ██████████████████████████████             ~50%
                   （端到端总墙钟较历史 282s 样本明显缩短）
```

---

## 十二、已知可优化点

| # | 优化方向 | 当前问题 | 预估提速 |
|---|---------|---------|---------|
| A | ~~按 query 限定处理范围~~ | ✅ 已实现 `--query-ids` 参数 | 避免全库扫描 |
| B | 抖音 400 快速失败 | ✅ 已实现，不再触发 3 次重试 | 节省 ~14s/条 |
| C | Structure 阶段并发 | 默认已 **5 路并发**（`structure-concurrency=5`），10 条约 **18~45s** | 可按需调更高并发（注意配额） |
| D | 关闭可选 LLM | 抖音特征抽取和通用摘要非必须 | Step 3 减少 40% 耗时 |
| E | 确定性失败短路 | 404/淘宝等链接仍走完整重试 | 节省无效等待 |
| F | iesdouyin 域名识别 | ✅ 已修复，不再误走通用爬虫 | 避免无效重试 |
| G | ~~深度思考多步骤链接“逐步汇总+去重”~~ | ✅ 已实现：每步点击后即时抽取并按 URL 去重汇总 | 提升引用覆盖率与稳定性 |

---

## 十三、后期固化与性能提升计划

### 13.1 短期（1~2 周）

- **LLM 开关配置化**：通过 `.env` 变量控制 `ENABLE_DOUYIN_LLM` / `ENABLE_WEB_SUMMARY_LLM`，一键切换"无 LLM 模式"用于测试和低成本跑批
- **Structure 并发化**：✅ 默认已多路并发；若仍遇瓶颈可再调 `--structure-concurrency` 或异步化 gather
- **确定性失败短路**：对 404、商品页等链接直接标记 `done`/`skip`，不进入重试循环

### 13.2 中期（1~2 月）

- **最小可执行子项目**：将主链路 5 步（collect/crawl/enrich/audio/structure）抽离为独立可部署的 Python 包，去掉冗余依赖，启动时间从 ~3s 降至 <1s
- **批处理调度器**：引入简单的任务队列（基于 PG SKIP LOCKED 或 Redis），支持多进程并行跑批
- **低成本模型切换**：Step 3 的 LLM 后处理切换为 doubao-lite 或其他低价模型，成本降低 50%+

### 13.3 长期（3 月+）

- **增量处理机制**：基于 `updated_at` 时间戳只处理变更数据，避免全量重算
- **平台爬虫插件化**：新增平台只需实现 `BaseCrawler` 接口 + 注册到 `CrawlerManager`，零侵入扩展
- **数据质量监控面板**：自动统计各平台成功率、字段完整度、LLM 调用量，输出到日报

---

## 十四、配置参考

### 14.1 环境变量 (.env)

```bash
# 数据库引擎（postgresql 或 mysql，默认 postgresql）
DB_TYPE=postgresql

# PostgreSQL（DB_TYPE=postgresql 时使用）
PGHOST=localhost
PGPORT=5432
PGDATABASE=doubao
PGUSER=root
PGPASSWORD=xxxxx

# MySQL（DB_TYPE=mysql 时使用）
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=doubao
MYSQL_USER=root
MYSQL_PASSWORD=

# 抖音视频下载 API
DOUYIN_DOWNLOAD_API_URL=http://localhost:8080

# 火山云 LLM
VOLCENGINE_API_KEY=xxxxxxxx
VOLCENGINE_BASE_URL=https://ark.cn-beijing.volces.com/api/v3
VOLCENGINE_SEED_MODEL=ep-xxxxxxxxx     # 通用文本模型
VOLCENGINE_VISION_MODEL=ep-xxxxxxxxx   # 图片理解模型
VOLCENGINE_SEEDANCE_MODEL=ep-xxxxxxxxx # 抖音特征抽取模型

# SMS 自动登录
SMS_API_BASE_URL=https://sms.guangyinai.com
SMS_API_TOKEN=sk-xxxxxxx
```

### 14.2 前置服务

| 服务 | 启动方式 | 默认端口 |
|------|---------|---------|
| PostgreSQL（DB_TYPE=postgresql） | `brew services start postgresql@17` | 5432 |
| MySQL（DB_TYPE=mysql） | `brew services start mysql` | 3306 |
| Redis | `brew services start redis` | 6379 |
| Douyin_TikTok_Download_API | `cd Douyin_TikTok_Download_API && python main.py` | 8080 |

---

*文档结束。如有疑问请联系维护人。*
