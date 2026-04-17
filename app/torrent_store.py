from __future__ import annotations

import opendal
import xxhash
from sslog import logger

from app.config import S3Mixin


def _s3_key(tid: int) -> str:
    h = xxhash.xxh32(str(tid).encode()).hexdigest()
    return f"torrents/{h[:2]}/{h[2:4]}/{tid}.torrent"


def _create_operator(c: S3Mixin) -> opendal.Operator:
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
        self.__op = _create_operator(config)

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
