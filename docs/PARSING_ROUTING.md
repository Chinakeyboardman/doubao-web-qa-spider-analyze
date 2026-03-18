# 链接解析方式路由说明

对 **qa_link** 中的链接进行**爬虫、收集内容、入库到 qa_link_content** 时，根据**网站特征**（由 `integration/citation_parser.identify_platform` 识别）走不同解析方式。

## 路由表

| 平台     | 解析方式               | 说明 |
|----------|------------------------|------|
| **抖音** | `douyin_download_llm`  | **前置**：由 **douyin-crawler/8080**（抖音视频下载项目）拉取视频数据 → 得到 视频标题、简介、评论、元数据（UP主、播放量、点赞数等）；**特征抽取**：用 **seedance 模型**（`.env` 中 `VOLCENGINE_SEEDANCE_MODEL`）解析文案与特征，写入 元数据.LLM解析文案与特征。未拉取前元数据与结构化内容会缺失。 |
| **B站**  | `bilibili_api`         | 调用下载/解析 API 获取视频信息与字幕 |
| **小红书** | `xiaohongshu_ssr`    | 小红书专用解析（SSR/页面结构） |
| **通用** | `agent_web_summary`    | **能直接抓取网页摘要和基本信息的网页**（如官网、标准文章页）：通用爬虫拉取正文后做摘要与基本信息抽取 |
| **知乎 / 什么值得买 / 微博 / 头条 / 百度** | `generic_web` | 当前走通用网页抓取（httpx+BS4），可后续按站单独优化 |
| **淘宝 / 京东** | `skip`              | 商品页暂不抓正文，仅落 qa_link |
| **其他** | `agent_web_summary`    | 未识别域名按「可抓取的通用页」处理，做网页摘要与基本信息 |

## 代码位置（爬虫 → 入库 qa_link_content）

- **平台识别**：`integration/citation_parser.py`（`identify_platform`, `PLATFORM_RULES`）
- **解析策略**：`integration/parsing_routing.py`（`PLATFORM_PARSING_STRATEGY`, `get_parsing_strategy`）
- **爬虫分发与入库**：`web-crawler/crawler_manager.py` — 按 platform 选爬虫，抓取后写入 `qa_link_content`
- **结构化**：`data-clean/structurer.py` + pipeline step_structure — 将 qa_link_content 中的 raw 转为规范 JSON
- **抖音 LLM / 通用摘要**：pipeline 结构化阶段按 platform 做后处理（文案与特征、网页摘要）

## 依赖说明

- **抖音**：元数据与结构化内容（视频标题、简介、评论、UP主、播放量等）**必须先**由 douyin-crawler/8080 拉取并写入 `qa_link_content`；否则结构化与特征抽取无输入，会缺失。视频相关特征抽取使用 **seedance 模型**（`.env` 配置 `VOLCENGINE_SEEDANCE_MODEL`，未配置时用 `VOLCENGINE_SEED_MODEL`）。
- **通用**：通用爬虫拉取正文后，可选 LLM 摘要（当前实现见 `llm_extractor.summarise_text`）。

## 扩展

- 新增平台：在 `citation_parser.PLATFORM_RULES` 增加域名与平台名，在 `parsing_routing.PLATFORM_PARSING_STRATEGY` 指定解析方式。
- 抖音 seedance 调用在 `data-clean/llm_extractor.py` 的 `enrich_douyin_video_llm`。
