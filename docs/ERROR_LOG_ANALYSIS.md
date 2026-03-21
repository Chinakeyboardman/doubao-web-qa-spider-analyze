# error.log（历史备份）错误归纳

> 样本：`output/error.log.bak1`（约 12 万行，**ERROR 约 5600+ 条**）。`error.log` 仅接收 **ERROR** 级别，INFO/WARNING（如 structure 部分日志）不一定出现。

## 1. 占比最高：抖音音频转写（`integration.douyin_audio_transcriber`）

| 子类 | 含义 | 处理建议 |
|------|------|----------|
| **ffmpeg 抽音频失败** / `moov atom not found` | 本地 mp4 **不完整或封装损坏**（下载中断、CDN 只返回片段、偶发 API 异常） | 代码已加 **ffprobe 下载后校验**；失败会给出明确原因；可删 `export/media/{query_id}/*.mp4` 后重跑 `audio-transcribe` |
| **stderr 里出现整段 `ffmpeg version`** | 旧版日志曾把 **完整 ffmpeg 横幅** 打进异常；现 **`_ffmpeg_error_snippet` 取 stderr 尾部** | 新日志应更易读 |
| **`Connection refused` (Errno 61)** | **抖音下载 API**（默认 `localhost:8080`）或上游未启动 | 先启动 `Douyin_TikTok_Download_API`，再跑 audio |
| **SeedASR 静音 / 空文本** | 视频无口播或音轨为纯 BGM | 已有 **文案兜底**（`text_content_bgm_no_asr` / raw_text） |

## 2. 爬取失败（`crawlers.base`）

| 子类 | 含义 |
|------|------|
| **`all 3 attempts failed for https://...`** | 海外站、反爬、超时、404 等；**非代码 bug** |

建议：对业务无关外链可接受 `qa_link.status=error`；必要时换 Playwright/代理。

## 3. 豆包 Web 采集（`integration.doubao_web_collector`）

- **CAPTCHA / human verification**：需 **headed 登录**或人工过验证码。
- **Login button not found**：页面改版或会话失效，需重新 `web-login`。

## 4. 与「结构化（Step 3）」的关系

- 在该 **error.log 备份** 中 **几乎无** `structure` / `structurer` / `content_json` 类 ERROR。
- **结构化失败** 多在 **终端** 或 **`output/run_sync_*.log`**（INFO），不一定写入 `error.log`。
- **音频失败** 会导致 **`video_parse_status` / `qa_link_video` 卡住或 error**，间接让整条链路「等音频」，观感像「没走到结构化」——优先修 **下载 + ffprobe + 抖音 API 可达**。

## 5. 推荐排错顺序

1. `python integration/run.py status` — 看 `douyin_video_parse` pending/error。
2. 确认抖音下载 API 可访问；必要时 `curl http://127.0.0.1:8080/docs`。
3. 单条重试：`python integration/run.py audio-transcribe --query-ids Qxxxx --audio-batch-size 1 --audio-concurrency 1`
4. 再跑：`python integration/run.py structure --link-ids Qxxxx_Lyyy`
