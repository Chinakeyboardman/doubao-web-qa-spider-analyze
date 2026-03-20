# docs 导航

## 许可与免责声明

- **开源许可证**：仓库根目录 [`LICENSE`](../LICENSE)（MIT）。
- **免责声明（爬虫/采集场景必读）**：[`DISCLAIMER.md`](DISCLAIMER.md)（学习测试用途、合规义务、无担保与责任限制）。

## GitHub Pages 文档站

仓库 **Settings → Pages** 中：**Build and deployment → Branch `main`，Folder `/docs`**，保存后即可在线浏览：

**<https://chinakeyboardman.github.io/doubao-web-qa-spider-analyze/>**

- 首页为 [`index.md`](index.md)（导航）；本 `README.md` 仅在 GitHub 浏览 `docs/` 目录时展示，不参与 Jekyll 构建。
- 本地预览（可选）：`cd docs && bundle install && bundle exec jekyll serve`，浏览器打开 `http://127.0.0.1:4000/doubao-web-qa-spider-analyze/`（需安装 Ruby / Bundler）。

---

为减少重复文档和过期说明，`docs/` 按「日常运行优先」组织如下：

## 1) 先看这三份

- `CLI_COMMANDS.md`：命令速查，含依赖服务预检说明（最常用）
- `CHECKLIST.md`：当前进度、关键变更、验收记录
- `PIPELINE_DEV_DOC.md`：完整主流程说明、性能基线（各 Step 与 **§11** 已按 2026-03 优化后观测更新）、Schema

## 2) 流程与规范

- `大规模qa数据获取.md`：A/B/C/D 结构化格式标准
- `PARSING_ROUTING.md`：平台识别与解析路由
- `方案设计.md`：总体架构设计与技术选型

## 3) 抖音链路专项

- `DOUYIN_API_SETUP.md`：抖音下载 API 启动与排错
- `抖音视频内容获取方案.md`：视频下载/转写链路细节

## 4) 历史和专项参考

- `数据收集工作流.md`
- `跨平台兼容性分析.md`
- `图形验证码-识别与拖拽逻辑.md`
- `获取可登录手机号和验证码.md`
- `多账号并发重构方案（豆包+抖音）.md`

> 已移除与当前主流程无关的 worker/agent 迁移文档，避免命令混淆。
