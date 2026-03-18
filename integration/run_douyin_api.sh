#!/bin/bash
# 启动抖音视频下载 API（默认 8081 端口，可在 .env 配置 DOUYIN_DOWNLOAD_API_PORT）
# 使用 Douyin 项目专用 venv，与主项目 venv 完全隔离，避免版本混乱
#
# 首次运行会自动执行一次性安装（需 brew install python@3.12）
# 日常使用：./integration/run_douyin_api.sh
# 启动前自动从 Chrome 同步抖音 Cookie（解决「获取数据失败」）

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOUYIN_DIR="$PROJECT_ROOT/Douyin_TikTok_Download_API"
VENV_DIR="$DOUYIN_DIR/.venv"

# 优先使用 brew 安装的 Python 3.12（与主项目 3.14 隔离）
PY312=""
for p in /opt/homebrew/opt/python@3.12/bin/python3.12 \
         /opt/homebrew/bin/python3.12 \
         /usr/local/opt/python@3.12/bin/python3.12; do
  if [[ -x "$p" ]]; then
    PY312="$p"
    break
  fi
done

if [[ -z "$PY312" ]]; then
  echo "❌ 未找到 Python 3.12"
  echo ""
  echo "请先安装（与系统 Python 隔离，不会影响现有环境）："
  echo "  brew install python@3.12"
  echo ""
  echo "安装后重新运行本脚本。"
  exit 1
fi

# 首次运行：创建 venv 并安装依赖
if [[ ! -d "$VENV_DIR" ]] || [[ ! -f "$VENV_DIR/bin/uvicorn" ]]; then
  echo "📦 首次运行：创建 Douyin API 专用 venv（Python 3.12）..."
  "$PY312" -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install -q --upgrade pip
  "$VENV_DIR/bin/pip" install -q -r "$DOUYIN_DIR/requirements.txt" python-dotenv
  echo "✅ 安装完成"
  echo ""
fi

# 读取端口（与 start_douyin_api.py 一致）
PORT=$(grep -E "^DOUYIN_DOWNLOAD_API_PORT=" "$PROJECT_ROOT/.env" 2>/dev/null | cut -d= -f2 || echo "8081")
PORT=${PORT:-8081}

# 若端口已被占用，先结束旧进程
OLD_PID=$(lsof -ti :$PORT 2>/dev/null || true)
if [[ -n "$OLD_PID" ]]; then
  echo "🔄 结束占用 $PORT 的旧进程 (PID $OLD_PID)..."
  kill $OLD_PID 2>/dev/null || true
  sleep 2
fi

# 启动前自动从浏览器同步抖音 Cookie
echo "🍪 同步抖音 Cookie..."
cd "$PROJECT_ROOT"
if source venv/bin/activate 2>/dev/null && python3 integration/sync_douyin_cookie.py chrome 2>/dev/null; then
  echo "  ✅ 已同步"
else
  echo "  ⚠ 跳过（请确保 Chrome 已打开抖音网页，或手动运行: python integration/sync_douyin_cookie.py chrome）"
fi

# 使用 Douyin venv 启动 API
cd "$PROJECT_ROOT"
exec "$VENV_DIR/bin/python" integration/start_douyin_api.py
