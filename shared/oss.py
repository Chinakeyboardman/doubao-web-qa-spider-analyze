"""阿里云 OSS 文件上传与公共 URL 生成工具。"""

from __future__ import annotations

import logging
from pathlib import Path

import oss2

from shared.config import CONFIG

logger = logging.getLogger(__name__)

_OSS_CFG = CONFIG.get("oss", {})
_ENDPOINT = (_OSS_CFG.get("endpoint") or "").strip()
_ACCESS_KEY = (_OSS_CFG.get("access_key") or "").strip()
_SECRET_KEY = (_OSS_CFG.get("secret_key") or "").strip()
_BUCKET_NAME = (_OSS_CFG.get("bucket") or "").strip()

_bucket: oss2.Bucket | None = None


def _get_bucket() -> oss2.Bucket:
    global _bucket
    if _bucket is not None:
        return _bucket
    if not _ENDPOINT or not _ACCESS_KEY or not _SECRET_KEY or not _BUCKET_NAME:
        raise RuntimeError(
            "OSS 配置不完整，请检查 ALIYUN_OSS_ENDPOINT / ACCESS_KEY / SECRET_KEY / BUCKET"
        )
    auth = oss2.Auth(_ACCESS_KEY, _SECRET_KEY)
    _bucket = oss2.Bucket(auth, f"https://{_ENDPOINT}", _BUCKET_NAME)
    return _bucket


def get_public_url(oss_key: str) -> str:
    """根据 OSS key 返回公共读 URL（Bucket 需设置为 public-read）。"""
    return f"https://{_BUCKET_NAME}.{_ENDPOINT}/{oss_key}"


def upload_file(local_path: str | Path, oss_key: str) -> str:
    """上传本地文件到 OSS，返回公共访问 URL。

    如果远端已存在同名 key 且大小一致则跳过上传。
    """
    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"本地文件不存在: {local_path}")

    bucket = _get_bucket()
    local_size = local_path.stat().st_size

    try:
        head = bucket.head_object(oss_key)
        if head.content_length == local_size:
            logger.debug("[oss] 跳过已存在: %s (%d bytes)", oss_key, local_size)
            return get_public_url(oss_key)
    except oss2.exceptions.NoSuchKey:
        pass

    bucket.put_object_from_file(oss_key, str(local_path))
    logger.info("[oss] 已上传: %s -> %s (%d bytes)", local_path.name, oss_key, local_size)
    return get_public_url(oss_key)
