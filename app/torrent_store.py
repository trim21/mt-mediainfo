from __future__ import annotations

import opendal
import xxhash
from sslog import logger

from app.config import Config
from app.db import Database


def _s3_key(tid: int) -> str:
    h = xxhash.xxh32(str(tid).encode()).hexdigest()
    return f"torrents/{h[:2]}/{h[2:4]}/{tid}.torrent"


def _create_operator(c: Config) -> opendal.Operator:
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
    def __init__(self, config: Config, db: Database):
        self.__db = db
        self.__op = _create_operator(config)

    def write(self, tid: int, info_hash: str, content: bytes) -> None:
        key = _s3_key(tid)
        self.__op.write(key, content)
        logger.debug("wrote torrent {} to s3 ({})", tid, key)

    def read(self, tid: int) -> bytes | None:
        key = _s3_key(tid)
        try:
            return bytes(self.__op.read(key))
        except Exception:
            logger.debug("torrent {} not found in s3, falling back to pg", tid)

        row = self.__db.fetch_val(
            "select content from torrent where tid = $1 limit 1",
            [tid],
        )
        return row

    def migrate_batch(self, limit: int = 100) -> int:
        rows: list[tuple[int, bytes]] = self.__db.fetch_all(
            "select tid, content from torrent limit $1",
            [limit],
        )

        if not rows:
            return 0

        migrated = 0
        for tid, content in rows:
            key = _s3_key(tid)
            try:
                self.__op.write(key, content)
            except Exception:
                logger.exception("failed to write torrent {} to s3", tid)
                continue

            self.__db.execute("delete from torrent where tid = $1", [tid])
            migrated += 1

        logger.info("migrated {} torrents from pg to s3", migrated)
        return migrated
