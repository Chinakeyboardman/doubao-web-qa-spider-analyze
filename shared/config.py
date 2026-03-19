"""Centralized configuration loaded from root .env file."""

import os
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

CONFIG = {
    "db_type": os.getenv("DB_TYPE", "postgresql").lower(),
    "pg": {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "doubao"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
    },
    "mysql": {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "dbname": os.getenv("MYSQL_DATABASE", "doubao"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
    },
    "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    "douyin_api": {
        "url": os.getenv("DOUYIN_DOWNLOAD_API_URL", "http://localhost:8081"),
        "port": int(os.getenv("DOUYIN_DOWNLOAD_API_PORT", "8081")),
    },
    "volcengine": {
        "api_key": os.getenv("VOLCENGINE_API_KEY", "") or os.getenv("ARK_API_KEY", ""),
        "base_url": os.getenv(
            "VOLCENGINE_BASE_URL",
            "https://ark.cn-beijing.volces.com/api/v3",
        ),
        "seed_model": os.getenv("VOLCENGINE_SEED_MODEL", ""),
        "vision_model": os.getenv("VOLCENGINE_VISION_MODEL", ""),
        "seedance_model": os.getenv("VOLCENGINE_SEEDANCE_MODEL", ""),  # 抖音视频特征抽取专用
    },
    "oss": {
        "endpoint": os.getenv("ALIYUN_OSS_ENDPOINT", ""),
        "access_key": os.getenv("ALIYUN_OSS_ACCESS_KEY", ""),
        "secret_key": os.getenv("ALIYUN_OSS_SECRET_KEY", ""),
        "bucket": os.getenv("ALIYUN_OSS_BUCKET", ""),
        "region": os.getenv("ALIYUN_OSS_REGION", ""),
    },
    "asr": {
        "app_id": os.getenv("VOLCENGINE_ASR_APP_ID", ""),
        "access_token": os.getenv("VOLCENGINE_ASR_ACCESS_TOKEN", ""),
        "resource_id": os.getenv("VOLCENGINE_ASR_RESOURCE_ID", "volc.seedasr.auc"),
        "submit_url": os.getenv(
            "VOLCENGINE_ASR_SUBMIT_URL",
            "https://openspeech.bytedance.com/api/v3/auc/bigmodel/submit",
        ),
        "query_url": os.getenv(
            "VOLCENGINE_ASR_QUERY_URL",
            "https://openspeech.bytedance.com/api/v3/auc/bigmodel/query",
        ),
    },
    "sms_api": {
        "base_url": os.getenv("SMS_API_BASE_URL", "https://sms.guangyinai.com"),
        "token": os.getenv("SMS_API_TOKEN", ""),
        "device_id": os.getenv("SMS_DEVICE_ID", "doubao-crawler-01"),
        "platform": os.getenv("SMS_PLATFORM", "doubao"),
    },
    "crawler": {
        # 设为 false 时跳过 SSL 校验，可访问“不安全”需点高级通过的网站（仅爬虫用）
        "verify_ssl": os.getenv("CRAWLER_VERIFY_SSL", "true").lower() not in ("0", "false", "no"),
    },
}
