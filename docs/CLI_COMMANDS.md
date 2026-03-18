# 脚本命令速查

> 所有命令均在项目根目录执行，使用 `./venv/bin/python` 或激活 venv 后 `python`。

> `integration/run.py` 已内置依赖预检查：当 PostgreSQL 或抖音下载 API 未启动时，会在命令开始前给出可执行的启动提示，避免长时间启动后才报错。
>
> **error.log**：所有命令的 ERROR 及以上级别日志会追加写入 `output/error.log`，便于排查。

---

## 0. 依赖服务快速启动（推荐）

```bash
# macOS
brew services start postgresql@17 || brew services start postgresql
brew services start redis

# 抖音下载 API（用于 crawl/audio 的抖音链路）
cd Douyin_TikTok_Download_API && python start.py
```

> 若未启动，`run.py` 会自动提示缺失项；`audio-transcribe` 会直接 fail-fast，避免无意义等待。

---

## 一、Pipeline 主流程（integration/run.py）

### 1.1 全流程

```bash
# 全流程：collect → crawl → enrich → audio-transcribe → structure（默认 10 条）
./venv/bin/python integration/run.py run --limit 10

# 指定 query 范围
./venv/bin/python integration/run.py run --query-ids Q0001,Q0002,Q0003

# 按 category 过滤
./venv/bin/python integration/run.py run --limit 10 --category-prefix 3C数码

# API 模式（无参考资料链接）
./venv/bin/python integration/run.py run --api --limit 10

# 显示浏览器窗口（默认后台静默）
./venv/bin/python integration/run.py run --limit 5 --headed
```

| 参数 | 说明 | 默认 |
|------|------|------|
| `--batch-size` | 批大小 | 10 |
| `--limit` | 未指定 query-ids 时取前 N 条 pending | - |
| `--query-ids` | 逗号分隔 query_id | - |
| `--category-prefix` | category 前缀过滤 | - |
| `--api` | 改用 API 采集 | - |
| `--headed` | 显示浏览器 | - |
| `--crawl-concurrency` | 爬取并发数 | 3 |

### 1.2 分步执行

```bash
# 仅采集
./venv/bin/python integration/run.py collect --limit 10

# 仅爬取
./venv/bin/python integration/run.py crawl --batch-size 50

# 仅补全抖音数据
./venv/bin/python integration/run.py enrich-douyin

# 仅下载并转写抖音视频音频（Step 2.6）
./venv/bin/python integration/run.py audio-transcribe --audio-batch-size 5 --audio-concurrency 2

# 仅结构化
./venv/bin/python integration/run.py structure

# 结构化指定 query
./venv/bin/python integration/run.py structure --query-ids Q0001,Q0002,Q0003

# 结构化指定 link（小批量分析推荐）
./venv/bin/python integration/run.py structure --link-ids Q0001_L001,Q0001_L002

# 结构化并发数（默认 5）
./venv/bin/python integration/run.py structure --structure-concurrency 8
```

### 1.2.1 音频转写参数（audio-transcribe）

| 参数 | 说明 | 默认 |
|------|------|------|
| `--query-ids` | 仅处理指定 query 的抖音链接 | - |
| `--audio-concurrency` | 下载+转写并发 | 2 |
| `--audio-batch-size` | 单轮消费 link 数 | 1000 |

### 1.3 异步批量（run-sync）

```bash
# 按数量：取前 20 条 pending 跑批（推荐）
./venv/bin/python integration/run.py run-sync --limit 20

# 按范围：指定 query_id 区间
./venv/bin/python integration/run.py run-sync --start-query-id Q0001 --end-query-id Q0020

# 指定日志路径
./venv/bin/python integration/run.py run-sync --limit 20 --log-file output/my_run.log

# 控制音频环节吞吐（便于小批分析）
./venv/bin/python integration/run.py run-sync --limit 10 --audio-batch-size 2 --audio-concurrency 1

# 显示浏览器（便于处理人机验证）
./venv/bin/python integration/run.py run-sync --limit 20 --headed
```

**日志**：默认写入 `output/run_sync_{start}_{end}_{时间戳}.log`

**登录/人机验证**：run-sync 启动前会做登录预检查，失败则直接退出。若采集时频繁超时或失败，建议：
1. 先执行 `web-login --manual` 完成登录/验证
2. 或使用 `--headed` 显示浏览器窗口，便于人工处理验证码

| 参数 | 说明 | 默认 |
|------|------|------|
| `--limit` | 取前 N 条 pending | - |
| `--start-query-id` | 起始 query_id | - |
| `--end-query-id` | 结束 query_id | - |
| `--log-file` | 日志路径 | 自动生成 |
| `--collect-batch-size` | collect 单批（串行建议） | 1 |
| `--crawl-batch-size` | crawl 单批 | 50 |
| `--crawl-concurrency` | crawl 并发 | 3 |
| `--structure-concurrency` | structure LLM 并发 | 5 |

### 1.4 持续跑批（run-until）

```bash
# 直到 done 且有链接达到 50 条
./venv/bin/python integration/run.py run-until --target-done-with-links 50

# 按 category 过滤
./venv/bin/python integration/run.py run-until --category-prefix 3C数码 --target-done-with-links 30
```

---

## 二、状态与修复

```bash
# 查看状态
./venv/bin/python integration/run.py status

# 重置失败项为 pending（含 processing 超 2 小时）
./venv/bin/python integration/run.py retry

# 重置所有 error（含不可重试）
./venv/bin/python integration/run.py retry --all

# 重置指定 query 后重采
./venv/bin/python integration/run.py recollect --query-ids Q0001,Q0002,Q0003

# 重置后仅Web采集 answer（不走 API）
./venv/bin/python integration/run.py recollect-web-only --query-ids Q0011,Q0012

# 不传 query-ids 时，默认重跑 Q0011~Q0016,Q0305~Q0309
./venv/bin/python integration/run.py recollect-web-only
```

---

## 三、内容重生与导出

```bash
# 重生 content_json（默认仅最近 2 小时 done 链接）
./venv/bin/python integration/run.py regenerate-content

# 重生指定链接
./venv/bin/python integration/run.py regenerate-content --link-ids Q0001_L001,Q0001_L002

# 重生全量（谨慎）
./venv/bin/python integration/run.py regenerate-content --all

# 允许低质量覆盖
./venv/bin/python integration/run.py regenerate-content --force

# 导出报告 + JSON + MD
./venv/bin/python integration/run.py export

# 导出到 XLSX
./venv/bin/python integration/run.py export-excel

# 指定 XLSX 路径
./venv/bin/python integration/run.py export-excel --output export/qa_20250116.xlsx
```

**说明**：导出 `status` 为 `done` 或 `error` 的数据（`qa_query` / `qa_answer` / `qa_link` / `qa_link_content` / `qa_link_video` 五张表）。

---

## 四、豆包 Web 登录与采集

```bash
# 登录（保存 session 供后续复用）
./venv/bin/python integration/run.py web-login

# 手动登录（弹窗）
./venv/bin/python integration/run.py web-login --manual

# 直接网页采集（含深度思考链接）
./venv/bin/python integration/run.py web-collect --batch-size 5

# 采集指定 query
./venv/bin/python integration/run.py web-collect --query-id Q0001

# 测试单条（不写库）
./venv/bin/python integration/run.py web-test --query-text 低糖水果坚果麦片推荐

# 导出页面 HTML + 截图（调试用）
./venv/bin/python integration/run.py web-debug
```

---

## 五、Query 导入

```bash
cd query-input
pip install -r requirements.txt

# 默认读取上级目录 Query生成_测试集.xlsx
python import_queries.py

# 指定文件
python import_queries.py --file /path/to/file.xlsx

# 不写库（仅打印）
python import_queries.py --dry-run
```

---

## 六、一次性修复脚本

```bash
# 空内容修复（dry-run，仅打印）
./venv/bin/python integration/fix_empty_content.py

# 实际执行
./venv/bin/python integration/fix_empty_content.py --apply
```

**说明**：修复 CSDN 平台识别 + 头条/什么值得买空内容重置。用完后可手动删除 `integration/fix_empty_content.py`。

---

## 七、数据库迁移

```bash
# 执行迁移（需先配置 .env）
psql -h localhost -U root -d doubao -f init-db/init.sql
psql -h localhost -U root -d doubao -f init-db/migrate_v2.sql
psql -h localhost -U root -d doubao -f init-db/migrate_v3.sql
psql -h localhost -U root -d doubao -f init-db/migrate_v6.sql
```

---

## 八、常用组合示例

```bash
# 跑 20 条异步 + 日志
./venv/bin/python integration/run.py run-sync --limit 20

# 查看 pending 数量
./venv/bin/python integration/run.py status

# 重置卡住的 processing
./venv/bin/python integration/run.py retry

# 导出报告
./venv/bin/python integration/run.py export
```


### 旧数据漏跑补齐

各 worker 会按状态选取：
未采集的 query → collect
未爬的 link → crawl
未补全的抖音 link → enrich
未转写的 video → audio
未结构化的 content → structure

```bash
# 补齐 Q0001～Q0050 的漏跑
./venv/bin/python integration/run.py run-sync --start-query-id Q0001 --end-query-id Q0050
# 或先 retry 卡住的 processing，再跑
./venv/bin/python integration/run.py retry
./venv/bin/python integration/run.py run-sync --start-query-id Q0001 --end-query-id Q0100

```

