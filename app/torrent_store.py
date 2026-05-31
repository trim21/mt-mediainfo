from __future__ import annotations

from typing import TYPE_CHECKING

import opendal
import xxhash
from sslog import logger

from app.config import S3Mixin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _s3_key(tid: int) -> str:
    h = xxhash.xxh32(str(tid).encode()).hexdigest()
    return f"torrents/{h[:2]}/{h[2:4]}/{tid}.torrent"


def create_operator(c: S3Mixin) -> opendal.Operator:
    kwargs: dict[str, str] = {
        "bucket": c.s3_bucket,
        "region": c.s3_region,
        "endpoint": c.s3_endpoint,
        "access_key_id": c.s3_access_key_id,
        "secret_access_key": c.s3_secret_access_key,
    }
    if c.s3_root:
        kwargs["root"] = c.s3_root

    return opendal.Operator("s3", **kwargs)


class TorrentStore:
    def __init__(self, config: S3Mixin):
        self.__op = create_operator(config)

    def write(self, tid: int, content: bytes) -> None:
        key = _s3_key(tid)
        self.__op.write(key, content)
        logger.debug("wrote torrent {} to s3 ({})", tid, key)

    def read(self, tid: int) -> bytes | None:
        key = _s3_key(tid)
        try:
            return self.__op.read(key)
        except opendal.exceptions.NotFound:
            logger.debug("torrent {} not found in s3", tid)
        return None


def generate_presigned_url(
    client: S3Client,
    *,
    bucket: str,
    key: str,
    download_filename: str | None = None,
    expires_in: int = 3600 * 24,
) -> str:
    params: dict[str, str] = {"Bucket": bucket, "Key": key}
    if download_filename:
        params["ResponseContentDisposition"] = f'attachment; filename="{download_filename}"'
    return client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=expires_in,
    )
