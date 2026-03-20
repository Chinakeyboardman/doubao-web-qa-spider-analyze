# doubao-web-qa-spider-analyze

豆包 Web 端 QA 数据采集与分析系统。通过多个子项目协作，完成「Query 输入 → 豆包爬取 → 引用链接抓取 → 视频下载 → 数据清洗入库」的全流程。

## 目录结构

```
├── .env.example                # 统一配置模板（复制为 .env 使用）
├── docker-compose.yml          # 一键启动 PG、Redis、抖音下载 API
├── init-db/                    # 新业务表初始化（qa_ 前缀）
│   └── init.sql
├── douyin-crawler/             # 豆包 web 爬虫（开源，原样保留）
│   └── README.md               # 豆包爬虫说明（保留在子项目内）
├── Douyin_TikTok_Download_API/ # 抖音视频下载（开源，Docker 独立部署）
├── query-input/                # Query 导入脚本
├── web-crawler/                # 网站爬虫（按平台路由到对应爬虫）
├── data-clean/                 # 数据清洗（格式 A/B/C/D 结构化）
├── integration/                # 业务对接层（调用开源项目，不侵入其源码）
├── shared/                     # 共享配置与工具
├── docs/                       # 项目文档
│   ├── CHECKLIST.md            # 项目 Checklist
│   ├── PIPELINE_DEV_DOC.md     # Pipeline 开发文档
│   ├── PARSING_ROUTING.md      # 爬虫平台路由说明
│   ├── 方案设计.md
│   ├── 大规模qa数据获取.md
│   ├── 数据收集工作流.md
│   ├── 跨平台兼容性分析.md
│   ├── 获取可登录手机号和验证码.md
│   └── 多账号并发重构方案（豆包+抖音）.md
└── README.md
```

## 子项目说明

| # | 项目 | 目录 | 状态 | 说明 |
|---|------|------|------|------|
| 1 | Query 输入 | `query-input/` | 待建 | 表格/DB 导入，本地脚本 |
| 2 | 豆包 web 爬虫 | `douyin-crawler/` | 已有 | 目录纳入，不修改源码 |
| 3 | 抖音视频下载 | `Douyin_TikTok_Download_API/` | 已有 | 本地进程部署，HTTP API 调用 |
| 4 | 网站爬虫 | `web-crawler/` | 待开发 | Langchain + 火山云 |
| 5 | 数据清洗 | `data-clean/` | 待开发 | Python CRUD 后端 |

## 快速开始

### 1. 配置

```bash
cp .env.example .env
# 编辑 .env，填入实际的数据库密码、火山云 API Key 等
```

### 2. 基础服务（系统服务方式）

确保 PostgreSQL 和 Redis 已运行：

```bash
# macOS
brew services start postgresql
brew services start redis

# Linux
sudo systemctl start postgresql
sudo systemctl start redis
```

初始化新业务表：

```bash
psql -h localhost -U root -d doubao -f init-db/init.sql
```

### 3. 抖音视频下载 API

```bash
cd Douyin_TikTok_Download_API
pip install -r requirements.txt
python start.py
# 默认监听 0.0.0.0:80，可在 config.yaml 中修改 Host_Port
```

API 文档：`http://localhost:80/docs`

### 4. 豆包 web 爬虫

需要本地 Chrome + Node.js 环境。

```bash
cd douyin-crawler
npm install
cd worker && uv sync
```

详见 `douyin-crawler/README.md`。

### 5. Query 导入

```bash
cd query-input
pip install -r requirements.txt
python import_queries.py
```

### 6. 异步批量跑批（run-sync）

```bash
# 方式一：按数量，取前 20 条 pending 跑批（推荐）
./venv/bin/python integration/run.py run-sync --limit 20

# 方式二：指定 query_id 范围
./venv/bin/python integration/run.py run-sync --start-query-id Q0001 --end-query-id Q0020

# 日志自动写入 output/run_sync_*.log
```

> 完整命令列表见 [docs/CLI_COMMANDS.md](docs/CLI_COMMANDS.md)

## 数据库

所有项目共用同一 PostgreSQL 实例和数据库。

| 来源 | 表前缀 | 说明 |
|------|--------|------|
| douyin-crawler 原有 | 无 | douyin_videos、video_tasks 等，保持原样 |
| QA 新业务 | `qa_` | qa_query、qa_answer、qa_link、qa_link_content、qa_link_video |

## 文档

**在线浏览（GitHub Pages）**：<https://chinakeyboardman.github.io/doubao-web-qa-spider-analyze/>（仓库 **Settings → Pages** 选择分支 `main`、目录 `/docs` 后生效）

项目文档集中在 `docs/` 目录，主要包含：

- **CLI_COMMANDS.md**：**脚本命令速查**（run/collect/crawl/structure/run-sync/export 等）
- **CHECKLIST.md**：项目进度与 Checklist
- **PIPELINE_DEV_DOC.md**：Pipeline 步骤、技术栈、CLI、Schema 说明
- **大规模qa数据获取.md**：数据模型与 JSON 格式 A/B/C/D 规范
- **方案设计.md**：子项目设计与 Langchain/火山云选型
- **PARSING_ROUTING.md**：爬虫平台路由规则
- **docs/README.md**：文档导航（建议先读）

## 开源项目保护原则

- `douyin-crawler/` 和 `Douyin_TikTok_Download_API/` **不做任何修改**
- 业务适配逻辑写在 `integration/` 等独立目录中
