---
layout: default
title: 'CHECKLIST'
---

# 项目 Checklist

## 一、架构决策（已确定）

| 项目 | 纳入方式 | 说明 |
|------|----------|------|
| 抖音视频下载 | **本地进程部署** | pip 安装依赖 + `python start.py` 启动 FastAPI 服务 |
| 豆包web爬虫 | **目录纳入** | 直接放在父项目下，作为子目录 |
| 配置 | **父目录统一** | 根目录 `.env` |
| 数据库 | **PostgreSQL 或 MySQL（`DB_TYPE` 切换）** | 各项目共用一个库，新业务表加前缀区分 |
| 部署方式 | **常规 Linux 部署** | PG、Redis 用系统服务，各项目用本地进程，不用 Docker |

### 开源项目保护原则（重要）

- **不修改**两个开源项目的原始目录名、结构和代码
- **引用方式**：在另一个文件夹中写调用/适配逻辑，通过 HTTP、读取其 DB 输出、或 import 其模块来使用，不侵入其源码

---

## 二、目录结构

```
doubao-web-qa-spider-analyze/
├── .env.example                # 统一配置模板
├── .env                        # 实际配置（gitignore）
├── requirements.txt            # 根依赖（Phase 4 新增）
├── docker-compose.yml          # 父项目编排（PG、Redis、抖音下载服务）
├── init-db/                    # 数据库初始化脚本
│   ├── postgresql/             # PostgreSQL DDL + 迁移脚本
│   │   ├── init.sql
│   │   └── migrate_v2~v8.sql
│   └── mysql/                  # MySQL DDL
│       └── init.sql
├── shared/                     # 共享工具模块
│   ├── config.py               # .env 配置加载（含 DB_TYPE）
│   ├── db.py                   # 数据库操作门面（PostgreSQL / MySQL 双引擎）
│   ├── db_backend/             # 数据库后端抽象
│   │   ├── __init__.py         # get_backend() 工厂
│   │   ├── base.py             # DBBackend 抽象基类
│   │   ├── postgresql.py       # psycopg2 后端
│   │   └── mysql.py            # PyMySQL 后端
│   ├── sql_builder.py          # 跨方言 SQL 构建器（单例 sb）
│   ├── claim_functions.py      # claim_pending_* Python 实现
│   └── volcengine_llm.py       # 火山云 LLM 封装
├── douyin-crawler/             # 抖音爬虫（原样保留，不修改）
├── Douyin_TikTok_Download_API/ # 抖音视频下载（原样保留，独立部署）
├── query-input/                # Query 导入脚本
├── integration/                # 业务对接层 + 流水线编排
│   ├── doubao_query.py         # 豆包回答采集（火山云 API）
│   ├── doubao_web_collector.py # 豆包网页采集（Playwright，含深度思考链接）
│   ├── citation_parser.py      # 引用链接解析（URL→平台+格式）
│   ├── douyin_data_merger.py   # 抖音数据补全（douyin_videos/comments → qa_link_content）
│   ├── pipeline.py             # 完整流水线编排
│   └── run.py                  # CLI 入口
├── web-crawler/                # 多平台链接内容抓取
│   ├── crawler_manager.py      # 爬虫调度器
│   └── crawlers/               # 平台爬虫
│       ├── base.py             # 基类（重试+限频）
│       ├── generic_web.py      # 通用网页（httpx+BS4）
│       ├── douyin_video.py     # 抖音视频（调 8080 API）
│       ├── bilibili_video.py   # B站视频（调 8080 API）
│       └── xiaohongshu.py      # 小红书
├── data-clean/                 # 数据清洗结构化
│   ├── structurer.py           # JSON 格式化（A/B/C/D）
│   └── llm_extractor.py        # LLM 辅助提取（图片描述+摘要）
└── README.md
```

---

## 三、数据库设计

**统一使用一个数据库**（如 `doubao`），所有表在同一库中。

| 来源 | 前缀 | 表名示例 | 说明 |
|------|------|----------|------|
| douyin-crawler 原有表 | 无 | douyin_videos, douyin_comments, video_tasks, video_task_steps | 保持原样 |
| 新业务 - QA 数据 | `qa_` | qa_query, qa_answer, qa_link, qa_link_content | 新建 |
| 新业务 - 网站爬虫 | `web_` | 待定 | 新建 |

---

## 四、实现路径

### 阶段 1：基础骨架 ✅ 已完成

- [x] 根目录 `.env.example` + `.env`（PG root:123456、Redis、火山云 API）
- [x] `docker-compose.yml`（已创建，但改为常规部署方式，仅作备用参考）
- [x] 新建空目录：`query-input/`、`web-crawler/`、`data-clean/`、`integration/`、`init-db/`
- [x] `init-db/init.sql`：新业务表建表语句（qa_ 前缀）
- [x] 父项目 `README.md`、`.gitignore`

### 阶段 2：建库建表 + Query 导入 ✅ 已完成

- [x] 在本地 PG 中创建 `doubao` 数据库（已存在）
- [x] 执行 `init-db/init.sql` 建 qa_ 业务表（4 张表 + 索引 + 触发器）
- [x] 编写 `query-input/import_queries.py` 导入脚本 + `requirements.txt`
- [x] 从 `Query生成_测试集.xlsx` 导入 1000 条 Query 到 `qa_query` 表（新增 1000 条）
- [x] 验证导入结果（1000 条：信息型 407、交易型 393、对比型 200）

### 阶段 3：常规部署打通 ✅ 已完成

前置条件：本地已有 PostgreSQL（root:123456@localhost:5432）、Redis（localhost:6379）

- [x] 抖音视频下载 API：安装 `integration/requirements.txt` 后，使用 `Douyin_TikTok_Download_API/venv/bin/python integration/start_douyin_api.py` 启动（端口 8080）；验证 `http://127.0.0.1:8080/docs` 返回状态码 `200`
- [x] douyin-crawler：确认可通过环境变量覆盖连接同一 PG `doubao` 库（验证命令注入 `PGDATABASE=doubao` 到 `douyin-crawler/worker` 配置，输出 `worker_db=doubao`、`connected_db=doubao`）
- [x] 验证各服务连通：TCP 检测 `5432/6379/8080` 均 `ok`；`uv run --directory douyin-crawler/worker python cli.py status` 在 `doubao` 库可连通，但报 `relation \"douyin_videos\" does not exist`（说明连通正常，待在 `doubao` 初始化 crawler 原表）

### 阶段 4：业务开发 ✅ 已完成

#### 4.1 基础设施 ✅

- [x] `init-db/migrate_v2.sql`：qa_query 和 qa_link 加 status 字段（pending/processing/done/error）并执行
- [x] `init-db/migrate_v3.sql`：qa_link_content.content_json 改为可空（raw_json/content_json 分离，爬虫只写 raw_json）
- [x] `shared/` 共享模块：`db.py`（PG 连接池+CRUD）、`config.py`（.env 加载）、`volcengine_llm.py`（火山云 LLM 封装）
- [x] 根目录 `requirements.txt`（python-dotenv / psycopg2-binary / httpx / beautifulsoup4 / lxml / langchain / langchain-openai）+ `venv` 安装
- [x] `.env.example` 补充 `VOLCENGINE_BASE_URL=https://ark.cn-beijing.volces.com/api/v3`
- [x] 验证火山云 API 连通性（1 条 query 测试 OK，模型 `ep-20260310153851-dq2dx`）

#### 4.2 豆包回答采集 ✅

- [x] `integration/doubao_query.py`：`DoubaoQueryCollector`，调用火山云 API 获取豆包风格回答，自动 fallback（web_search 工具需端点配置，当前使用基础调用）
- [x] `integration/citation_parser.py`：引用链接解析（URL → 平台识别 + content_format 判定，支持小红书/抖音/知乎/B站/什么值得买/淘宝/京东等）
- [x] 单条 query 测试：Q0002 "高蛋白混合麦片哪款好" → 1348 字回答 → qa_answer 写入 OK
- [x] 批量测试：3 条 query 批量采集全部成功（Q0001/Q0003/Q0004）

#### 4.3 链接内容抓取 ✅

- [x] `web-crawler/crawlers/base.py`：爬虫基类（指数退避重试 + 域名级限频）
- [x] `web-crawler/crawlers/generic_web.py`：通用网页爬虫（httpx + BeautifulSoup，提取标题/段落/图片/元数据）
- [x] `web-crawler/crawlers/douyin_video.py`：抖音视频（调用 Douyin_TikTok_Download_API `localhost:8080/api/hybrid/video_data` + 评论接口）
- [x] `web-crawler/crawlers/bilibili_video.py`：B站视频（调用 Download API `localhost:8080/api/bilibili/web/fetch_one_video`）
- [x] `web-crawler/crawlers/xiaohongshu.py`：小红书（SSR `__INITIAL_STATE__` JSON 提取 + HTML fallback）
- [x] `web-crawler/crawler_manager.py`：调度器（按 platform 分发 + 商品页自动跳过 + 原始内容写入 qa_link_content）
- [x] 通用爬虫测试：IT之家文章抓取成功（21 段落 + 5 图片）

#### 4.4 数据清洗结构化 ✅

- [x] `data-clean/structurer.py`：`ContentStructurer`，严格按 `docs/大规模qa数据获取.md` 4 种 JSON 格式输出
  - 格式A：图文无序（小红书/抖音图文）— 标题+正文段落+独立图片+标签+评论
  - 格式B：图文有序（知乎/什么值得买/通用）— 标题+内容流（文本与图片有序交错）
  - 格式C：视频有字幕 — 视频标题+简介+字幕内容+评论
  - 格式D：视频无字幕 — 视频标题+简介+STT待处理标记
- [x] `data-clean/llm_extractor.py`：LLM 辅助提取（图片描述生成 via Vision 模型 + 长文摘要 via 文本模型）
- [x] 所有格式 mock 数据测试通过

#### 4.5 流水线编排 + 测试 ✅

- [x] `integration/pipeline.py`：`QAPipeline` 串联 collect → crawl → structure，支持单步/全流程运行
- [x] **默认采集方式为网页（web）**：`QAPipeline(use_web=True)` 使用 `DoubaoWebCollector`，可拿到 深度思考/参考资料 真实链接；`--api` 时回退为 API 采集（无引用 URL）
- [x] **默认走模拟登陆**：web 采集前若未登录，会自动调用 SMS 模拟登陆；登录状态保存在 `integration/.browser_state/state.json`，后续运行会复用
- [x] `integration/run.py`：CLI 入口，支持子命令：`run` / `collect` / `crawl` / `structure` / `status` / `retry`；`run` 与 `collect` 默认 web+模拟登陆，可选 `--api`
- [x] 端到端测试：6 条 query 通过 pipeline 采集完成 + 1 条测试链接完整走通 crawl → structure
- [x] 数据完整性验证：qa_query(done:6) → qa_answer(6 条) → qa_link(done:1) → qa_link_content(1 条结构化 JSON)

#### 4.6 浏览器自动化增强（已实现，待配置测试）

- [x] Playwright 豆包网页自动化 → `integration/doubao_web_collector.py`
- [x] SMS API 自动登录（sms.guangyinai.com 手机号+验证码）+ 手动登录 fallback
- [x] 真实 DOM 选择器适配（已 headless 探测：login modal / phone input / checkbox / 验证码6位框）
- [x] 深度思考链接抓取：多层 fallback（侧栏/思考容器 `<a>` → 含「参考/资料」子树 → 容器文本 regex → 全页面链接）
- [x] 参考资料选择器完善：drawer/panel/sidebar/ref 等 class、aria、从含「参考」「资料」文案的子树取链；抽取为 0 时自动 dump `after_reference_panel.html` 便于排查
- [x] CLI 集成：`web-login [--manual]` / `web-collect` / `web-test` / `web-debug`
- [x] 配置集成：`.env` 新增 `SMS_API_*` 四项配置 → `shared/config.py` 自动加载

#### 4.7 抖音数据补全（douyin_videos/douyin_comments → qa_link_content） ✅

- [x] **问题诊断**：qa_link_content 中抖音数据几乎全为空（8080 API 返回不完整或失败），而 douyin-crawler 已在 douyin_videos/douyin_comments 中存有完整数据，但两套系统数据未打通
- [x] 在 `doubao` 库中创建 douyin_videos / douyin_comments 表（`PGDATABASE=doubao node douyin-crawler/init-db.js`），已有数据已从 `douyin` 库迁移
- [x] `integration/douyin_data_merger.py`：`DouyinDataMerger`，通过 video_id 关联 qa_link → douyin_videos + douyin_comments，补全 title/author/likes/favorites/comments(含location)/share_link 等字段
- [x] `web-crawler/crawlers/douyin_video.py` 增加 DB fallback：8080 API 失败时自动从 douyin_videos 表读取数据
- [x] `integration/pipeline.py` 新增 Step 2.5（`step_enrich_douyin`）：在 crawl 和 structure 之间补全抖音数据
- [x] `integration/run.py` 新增 `enrich-douyin` CLI 命令
- [x] 端到端验证：enrichment → regenerate-content 全流程走通，Q0002_L016 成功从空数据补全为含评论+LLM特征的完整结构化 JSON

##### 关键发现
- 现有 API 方式（`doubao_query.py`）返回的回答文本**不含真实 URL**，22条 query 仅 3 条有极少量引用
- 网页版**必须登录**才能使用聊天功能（guest 模式弹 login modal）
- 登录流程：点击登录 → 输入手机号 → 勾选协议 → 下一步 → 输入6位验证码 → 自动登录
- douyin-crawler 默认连接 `douyin` 库，需用 `PGDATABASE=doubao` 运行才能让数据进入同一 `doubao` 库
- 抖音数据补全流程：`enrich-douyin` 命令通过 URL 中的 video_id 匹配 douyin_videos，合并后由 structurer 重新格式化

#### 4.8 静默并发与跑批增强（进行中）

- [x] 全流程网页采集默认静默（`--headed` 才显示窗口），验证码阻塞时给出明确人工介入提示
- [x] 抖音本地 Node 爬虫调用增加静默保护：默认不触发本地浏览器抓取（`ENABLE_DOUYIN_LOCAL_SCRAPER=false`）
- [x] `crawler_manager.batch_crawl` 从串行改为受控并发（`asyncio.Semaphore`），并新增并发日志
- [x] CLI 新增并发参数：`--crawl-concurrency`（默认保守值 3）
- [x] CLI 新增按数量顺序挑选未跑过 query：`--limit`，并支持 `--category-prefix` 前缀过滤
- [x] 新增 `recollect` 子命令，支持按 query_id 重置 `qa_answer/qa_link/qa_link_content` 后定向重跑
- [x] 已完成指定 query 重跑：`Q0005,Q0007,Q0008,Q0009`
- [x] 已完成单条失败诊断并固化到主流程；历史临时诊断脚本已移除，避免维护重复入口
- [x] 失败根因对照验证：`Q0311` 在 `--force-login-check` 下，`headless` 无生成态/无答案，`headed` 可正常出答案并提取 46 条参考链接
- [x] 已按“单条重置+单条重采（headed）”修复：`Q0311,Q0266,Q0267,Q0268`
- [x] 新增风控自动恢复：`headless` 命中人机验证/风险超时时，自动临时切 `headed` 执行 `manual_login` 等待人工处理，完成后恢复原模式并自动重试当前 query
- [x] 表结构增强：`qa_answer.status`、`qa_link_content.status`、`qa_link_content.raw_json` 已落地（含线上 `ALTER TABLE IF NOT EXISTS` 迁移与历史数据回填）
- [x] 实测新链路：`Q0269`（error）重置后通过 `run.py collect` 成功回收，`qa_answer.status='done'`；并完成 1 条 crawl 验证 `qa_link_content.status='done'` 且 `raw_json` 已写入
- [x] 批量修复演练（5条 error）：`Q0271,Q0272,Q0273,Q0274` 全流程成功（collect+crawl+enrich+structure）；`Q0270` 仍因“已完成思考但0参考链接”失败，已导出 `after_reference_panel.html/png` 供定位
- [x] 规则修正：对“回答存在但0参考链接”的 query 按成功处理（不再误判 error）；已回归验证 `Q0270 -> qa_query.done / qa_answer.citation_count=0 / qa_link=0`
- [x] `data-clean/llm_extractor.py` 增加抖音空壳数据保护：当标题/简介/评论/字幕均为空时，直接跳过 seedance 调用并写入“未抓取到原始数据”说明，避免无效 LLM 费用
- [x] Web 采集健壮性增强：`doubao_web_collector.collect_one` 增加默认对话框可输入检查（chat_not_ready）、发送前后诊断日志（pre_send/post_send）、超时结构化诊断日志（timeout_diag）
- [x] 风控恢复顺序升级：`pipeline._collect_one_with_risk_recovery` 固化为「先换账号（switch_account）→ 再弹窗人工处理（headed manual）」并记录分阶段日志
- [x] 新增仅 Web 重跑命令：`integration/run.py recollect-web-only`（默认重跑 Q0011~Q0016,Q0305~Q0309），且 `run-sync --collect-batch-size` 默认改为 1（answer 串行）
- [x] 新增 Step 2.6 抖音音频转写：`integration/douyin_audio_transcriber.py`（下载视频→ffmpeg抽取压缩MP3→音频转写→回写 `raw_json.stt_text`）
- [x] 音频文件落盘规范：`export/{query_id}/{link_id}.mp3`，并在 `raw_json.audio_info` 写入 `audio_path/transcript_model/transcript_source`
- [x] Seed2 文件上传解析链路：改为 `Files API + Responses API(file_id)`；新增记录 `raw_json.audio_info.model_api_file_id`，音频不支持时自动回退 `input_video`
- [x] 抖音源视频保留：`export/{query_id}/{link_id}.mp4`（与 mp3 同命名规范），并在 `raw_json.audio_info.video_path` 记录来源路径
- [x] 上传解析降频与队列化：`wait_for_processing` 轮询间隔改为可配置（默认15s）、等待上限默认600s；新增 `claim_pending_video_parse` 队列抢占函数（基于 `video_parse_status`）
- [x] Step2.6 进一步优化：文件处理等待改为退避轮询（默认20s起步，最长60s，超时900s）；`responses.create` 做 SDK 参数兼容降级；`audio_info.audio_path` 仅在音频解析成功时写入；`transcript_model` 统一脱敏为 `seed2`
- [x] `integration/pipeline.py` 接入 Step 2.6（位于 Step2.5 enrich 与 Step3 structure 之间）
- [x] `integration/run.py` 增加 `audio-transcribe` 子命令；`run-sync` 增加 `audio_worker`（`--audio-query-window`、`--audio-concurrency`）
- [x] `data-clean/structurer.py` 支持 `stt_text`：无字幕时可生成 `字幕内容`（格式C）或 `语音转文本`（格式D）
- [x] 新增单元测试 `tests/test_douyin_audio.py`（7项，1项 live skip），覆盖抽音频压缩、转写调用封装、批量回写与跳过保护
- [x] 小规模全流程验证（Q0008,Q0010）：成功生成 `export/Q0008/Q0008_L013.mp3`、`export/Q0010/Q0010_L013.mp3`，并回写 DB 的 `stt_text`
- [x] 新增抖音专用视频解析状态：`qa_link_content.video_parse_status`（pending/processing/done/error/skip），Step 2.6 全链路状态流转已接入，`run.py status` 已可查看统计
- [ ] 连续监控跑批到 `done 且有链接` 累计 50（进行中，按 `category LIKE '3C数码%'`）

#### 4.10 qa_link_video 视频资源管理表 ✅

- [x] 新建 `qa_link_video` 表（`init-db/migrate_v6.sql`）：管理抖音视频下载/转写全流程，独立 status 状态机
- [x] `web-crawler/crawler_manager.py`：抖音链接保存 raw_json 时同时创建 qa_link_video 记录
- [x] `integration/douyin_data_merger.py`：Enrich 后同步更新 qa_link_video 视频元数据
- [x] `integration/douyin_audio_transcriber.py`：改用 qa_link_video 做状态管理（claim_pending_video_parse_v2），新增 `_sync_to_content` 将 STT 结果回写 qa_link_content
- [x] `integration/pipeline.py`：status/retry 增加 qa_link_video 统计和重置
- [x] `integration/run.py`：_monitor_and_stop 增加 video 状态监控
- [x] `shared/utils.py`：提取公共函数（to_raw_dict, has_meaningful_subtitles, extract_video_id_from_url, resolve_video_id）
- [x] 单元测试 `tests/test_link_video.py`（22项全通过），覆盖 shared/utils、_upsert_link_video、_sync_to_content、_set_video_status、batch_process_v2、structurer 兼容
- [x] 集成验证：Q0003_L012 全链路（claim → download → STT → sync → structure）通过
- [x] 旧数据回填：163 条抖音记录已迁移，0 状态不一致
- [x] 清理 integration 历史调试脚本（`test_douyin_stt.py`、`test_captcha_detect.py`、`compare_douyin_sources.py`、`fetch_douyin_content.py`、`web_collect_probe.py`），并同步更新相关文档引用
- [x] 新增回归测试：`tests/test_link_video.py` 增加旧订单抓不到抖音数据场景；新增 `tests/test_run_sync_video_status.py` 固化 run-sync 视频状态统计（24 项通过）
- [x] 小步验证：旧数据 `Q0006` 仅 1 条音频转写+结构化（pending 9→8）；新订单 `Q0071` run-sync 已验证 collect→crawl→audio 链路可达（为便于分析已主动中止剩余批次）
- [x] 新增精细化结构化入口：`run.py structure` 支持 `--link-ids`，可按单条/少量 link 执行 structure（避免一次跑太多）
- [x] 导出脚本补齐 5 张 QA 表：`export_db_excel.py` 新增 `qa_link_video` sheet；`export_qa.py` 新增视频资源统计与字段导出
- [x] CLI 启动预检：`run.py` 增加 PostgreSQL/抖音下载 API 自动提示（依赖未就绪时 fail-fast 或 warn）
- [x] 文档更新：`CLI_COMMANDS.md` 补充 audio/link-ids/5表导出与依赖预检说明；`PIPELINE_DEV_DOC.md` 补齐 Step2.6 与最新耗时区间（2026-03-20：Collect/Crawl/Audio/Structure 各节与 §11 已按优化后观测再修订）
- [x] docs 目录清理：移除过期 `douyin-crawler-worker-README.md`、`douyin-crawler-CLAUDE.md`，新增 `docs/README.md` 导航
- [x] GitHub Pages：`docs/` 增加 Jekyll（`_config.yml`、`index.md`、`Gemfile`、各文档 YAML 头），`docs/README.md` / 根 `README.md` 写明启用方式；站点 <https://chinakeyboardman.github.io/doubao-web-qa-spider-analyze/>
- [x] 开源与合规：根目录 `LICENSE`（MIT）、`docs/DISCLAIMER.md`（学习/测试用途、法律与平台责任声明）；根 `README.md` / `docs/README.md` / `docs/index.md` 链出声明
- [x] `run-sync` crawl worker：由「区间内按 qa_query.id 取前 N 个 query」改为「区间内仍有 `qa_link.status=pending` 的 query」（`_select_query_ids_with_pending_links`），避免大区间内后半段 pending link 长期轮不到、crawl/link_contents 不涨

#### 4.9 平台识别修复 + 空内容数据修复 ✅

- [x] `integration/citation_parser.py`：PLATFORM_RULES 新增 CSDN（`csdn.net` → `CSDN`）
- [x] `integration/parsing_routing.py`：加 CSDN 路由（`generic_web`）；头条/什么值得买改为 `playwright_web`
- [x] `web-crawler/crawlers/generic_web.py`：CSDN 正文提取优化（优先 `#article_content` 容器 + 嵌套段落去重）
- [x] `web-crawler/crawlers/playwright_web.py`：新增 PlaywrightWebCrawler（headless + stealth），用于 JS 渲染页面
- [x] `web-crawler/crawler_manager.py`：头条/什么值得买注册到 PlaywrightWebCrawler
- [x] `integration/fix_empty_content.py`：一次性修复脚本（CSDN 平台修正 8 条 + 头条/什么值得买空内容重置 44 条）
- [x] `init-db/migrate_v3.sql` 已执行：`content_json` 改为可空
- [x] 验证：平台识别全部通过；头条 Playwright 爬取成功（title+23k文本）；什么值得买部分成功（stealth 可绕过部分反爬）

#### 4.10 raw_text 兜底整合 ✅

- [x] `data-clean/structurer.py`：格式 C/D 中 视频简介 使用 `_video_desc_with_fallback(raw, stt_text)` 整合 raw_text/caption/stt_text
- [x] 逻辑：视频简介从 `raw_text/caption/stt_text` 选择长度最长文本，减少信息丢失

#### 4.11 ASR 音频转写替代视频理解 ✅

- [x] `integration/douyin_audio_transcriber.py`：新增 `transcribe_with_volcengine_asr(audio_path)`，走 Flash ASR（base64 上传本地 mp3）
- [x] `process_one` 改为 ASR 音频优先（mp4 下载 -> ffmpeg 抽 mp3 -> ASR），失败后回退 Seed2 视频理解，再失败回退 raw_text/caption
- [x] `_sync_to_content` 增强：支持写入 ASR `utterances` 到 `raw_json.subtitles`，并同步回写 `qa_link_video.subtitles`
- [x] `shared/config.py` 新增 `asr` 配置段；`.env.example` 新增 ASR endpoint/resource/model/timeout 配置
- [x] 回归测试更新：`tests/test_douyin_audio.py` 增加 ASR mock 与三层 fallback 用例，`tests/test_link_video.py` 回归通过
- [x] 主流程渐进验证（1条→10条）：`audio-transcribe` + `structure --link-ids` 闭环通过（11条 done）；当前因 ASR 凭证未配置，实际来源均为 `seed2_video/input_video`

#### 4.12 复合唯一键 + ASR 请求修复 ✅

- [x] `init-db/migrate_v7.sql`：`qa_link_video` 唯一约束从 `link_id` 改为 `(link_id, model_api_input_type)`，允许同一 link 同时存音频和视频解析记录
- [x] 回填逻辑：已完成的旧记录标为 `input_video`，待处理记录标为 `input_audio`；`model_api_input_type` 设为 NOT NULL DEFAULT `'input_audio'`
- [x] `claim_pending_video_parse_v2` 函数重建：返回新增字段 `vid`（PK）+ `model_api_input_type`；UPDATE 匹配改用 `WHERE v.id = sub.vid`
- [x] `douyin_audio_transcriber.py`：`_set_video_status`/`_sync_to_content`/`batch_process` 全部改为按 PK `vid` 定位 `qa_link_video` 行
- [x] `crawler_manager._upsert_link_video` / `douyin_data_merger._sync_link_video_metadata`：`ON CONFLICT` 从 `(link_id)` 改为 `(link_id, model_api_input_type)`
- [x] `init-db/init.sql`：表定义同步更新，新建表即使用复合唯一键
- [x] ASR 请求改用 `requests.post`（官方 demo 方式），替代 `httpx`；响应通过 `X-Api-Status-Code` header 判断成功/失败，附带 logid 等调试信息
- [x] 测试更新：`test_douyin_audio.py` 改 mock `requests.post`，`test_link_video.py` mock 数据增加 `vid`/`model_api_input_type`；34 项用例全部通过

#### 4.13 失败数据修复与 ASR 并发默认 ✅

- [x] **失败数据分析**：`qa_link_video.status='error'` 多为「Connection refused」（下载 API 未启动/重启）等瞬时故障；ASR 并发超限会报 `45000292 quota exceeded` 并自动回退 Seed2
- [x] **修复脚本**：`init-db/reset_video_transient_errors.sql` 将上述瞬时错误行重置为 `pending` 并清空 `error_message`、`retry_count`，同步 `qa_link_content.video_parse_status`；执行前确保下载 API 已稳定运行
- [x] **ASR 并发**：`run-sync` 与 `audio-transcribe` 的 `--audio-concurrency` 默认均为 **2**，建议≤2 避免 ASR 并发配额；帮助文案已注明
- [x] **run-sync 启动检查**：`run-sync` 纳入 `douyin_api_required`，下载 API 不可达时直接退出，避免成批 Connection refused
- [x] **下载/ffmpeg 优化**：`download_video` 对 RemoteProtocolError/ConnectError 自动重试一次；`process_one` 在 ffmpeg 提取失败时回退 Seed2（视频文件），再失败用 raw_text 兜底
- [x] **瞬时错误重置**：用代码执行重置时，除 Connection refused/Errno 61 外，可包含 `%peer closed connection%`、`%RemoteProtocol%`、`%quota exceeded for types: concurrency%`，便于重跑

#### 4.14 MySQL 双数据库适配 ✅

- [x] **架构设计**：`shared/db.py` 公共 API（`execute`/`fetch_all`/`fetch_one`/`execute_returning`/`get_cursor`/`get_connection`）保持不变，底层通过 `shared/db_backend/` 工厂模式分发到 PostgreSQL 或 MySQL 后端
- [x] `shared/config.py`：新增 `DB_TYPE`（`postgresql`/`mysql`，默认 `postgresql`）及 `mysql` 配置块（`MYSQL_HOST`/`MYSQL_PORT`/`MYSQL_DATABASE`/`MYSQL_USER`/`MYSQL_PASSWORD`）
- [x] `shared/db_backend/`：新建后端包（`base.py` 抽象基类 + `postgresql.py` psycopg2 + `mysql.py` PyMySQL）；MySQL 后端自动将 JSON 字符串反序列化为 Python dict
- [x] `shared/db.py`：重构为使用 `get_backend()` 工厂；`execute_returning` 新增 `returning_select` 参数适配 MySQL 无 RETURNING 场景
- [x] `shared/sql_builder.py`：15+ 跨方言 SQL 工具（`upsert_suffix`/`expand_any`/`expand_not_all`/`returning_clause`/`interval_ago`/`count_filter`/`json_extract_text`/`json_extract`/`json_array_length`/`json_key_exists`/`cast_int` 等），模块级单例 `sb`
- [x] `shared/claim_functions.py`：`claim_pending_queries`/`claim_pending_links`/`claim_pending_video_parse_v2` 三个 PG 存储函数的 Python 实现，使用 `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE`（MySQL 8.0+ 同样支持）
- [x] 调用层迁移（15+ 文件）：所有 PG 专有语法替换为 `sql_builder` 跨方言写法
  - `ON CONFLICT ... DO UPDATE SET ... = EXCLUDED.col` → `sb.upsert_suffix()` / MySQL `ON DUPLICATE KEY UPDATE ... = VALUES(col)`
  - `ANY(%s)` / `<> ALL(%s)` → `sb.expand_any()` / `sb.expand_not_all()`
  - `COUNT(*) FILTER (WHERE ...)` → `sb.count_filter()`
  - `CURRENT_TIMESTAMP - INTERVAL '2 hours'` → `sb.interval_ago(2)`
  - `col->>'key'` / `jsonb_array_length()` / `col ? 'key'` → `sb.json_extract_text()` / `sb.json_array_length()` / `sb.json_key_exists()`
  - `(expr)::INTEGER` → `sb.cast_int()`
  - `UPDATE ... FROM ...` (PG) → `UPDATE ... JOIN ...` (MySQL)
- [x] 存储函数调用替换：4 处 `SELECT * FROM claim_pending_*()` → Python 函数直调
- [x] `init-db/` 目录重构：现有脚本复制到 `init-db/postgresql/`；新建 `init-db/mysql/init.sql`（`AUTO_INCREMENT`/`JSON`/`ON UPDATE CURRENT_TIMESTAMP` 等 MySQL 语法）
- [x] `scripts/run_migrations.py`：按 `DB_TYPE` 自动选择 `init-db/postgresql/` 或 `init-db/mysql/` 下的脚本执行
- [x] `requirements.txt`：新增 `pymysql>=1.1.0`
- [x] `.env.example`：新增 `DB_TYPE`/`MYSQL_HOST`/`MYSQL_PORT`/`MYSQL_DATABASE`/`MYSQL_USER`/`MYSQL_PASSWORD`
- [x] 新增测试：`tests/test_sql_builder.py`（30 项，覆盖两种方言全部 helper）+ `tests/test_db_backend.py`（6 项，后端工厂 + MySQL JSON 适配）
- [x] 原有测试全部通过：70 passed, 1 skipped（mock 层不受影响，公共 API 无变化）

##### 关键约束
- MySQL 版本要求 **8.0+**（依赖 `FOR UPDATE SKIP LOCKED`、`CHECK` 约束、`JSON` 函数）
- 默认 `DB_TYPE=postgresql`，不配置时行为与改造前完全一致（向后兼容）
- PG 的 JSONB 自动转 dict，MySQL 的 JSON 返回字符串 → 在 `MySQLBackend.adapt_row()` 统一处理
- PG `UPDATE ... FROM ...` 语法在 MySQL 需改为 `UPDATE ... JOIN ...`，涉及 `pipeline.py` 中 3 处

---

## 五、子项目清单（7个）

| # | 项目 | 目录 | 状态 | 技术栈 |
|---|------|------|------|--------|
| 1 | query-input | `query-input/` | ✅ 已完成 | openpyxl + psycopg2，Excel 导入 |
| 2 | 抖音爬虫 | `douyin-crawler/` | 已有 | Node + Python + Celery，目录纳入 |
| 3 | 抖音视频下载 | `Douyin_TikTok_Download_API/` | 已有 | FastAPI，pip + 本地进程部署 |
| 4 | 共享模块 | `shared/` | ✅ 已完成 | python-dotenv + psycopg2 + pymysql + langchain-openai |
| 5 | 业务编排 | `integration/` | ✅ 已完成 | 火山云 API + 流水线 CLI |
| 6 | 网站爬虫 | `web-crawler/` | ✅ 已完成 | httpx + BeautifulSoup + 调用 Download API |
| 7 | 数据清洗 | `data-clean/` | ✅ 已完成 | JSON 格式化 + LLM 辅助提取 |
