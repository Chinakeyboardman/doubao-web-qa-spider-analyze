#!/usr/bin/env python3
"""
启动抖音视频下载 API 服务

使用 Douyin_TikTok_Download_API 项目自带的 venv (Python 3.13) 启动，
避免与主项目 Python 3.14 的依赖冲突。

端口从根目录 .env 的 DOUYIN_DOWNLOAD_API_PORT 读取（默认 8081）。
"""

import os
import subprocess
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
douyin_api_path = project_root / "Douyin_TikTok_Download_API"
api_venv_python = douyin_api_path / "venv" / "bin" / "python"

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

port = int(os.getenv("DOUYIN_DOWNLOAD_API_PORT", "8081"))
host = "0.0.0.0"

if __name__ == "__main__":
    if not api_venv_python.exists():
        print(f"Error: API venv not found at {api_venv_python}")
        print("Please set up the Douyin API venv first:")
        print(f"  cd {douyin_api_path} && python3.13 -m venv venv && ./venv/bin/pip install -r requirements.txt")
        sys.exit(1)

    print(f"启动抖音视频下载 API 服务")
    print(f"  Python: {api_venv_python}")
    print(f"  端口: {port}")
    print(f"  文档: http://localhost:{port}/docs")
    print()

    subprocess.run(
        [
            str(api_venv_python), "-m", "uvicorn",
            "app.main:app",
            "--host", host,
            "--port", str(port),
            "--log-level", "info",
        ],
        cwd=str(douyin_api_path),
    )
