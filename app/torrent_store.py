"""Torrent content storage backends.

Provides a protocol for storing and retrieving raw torrent file bytes,
with concrete implementations backed by PostgreSQL (existing behaviour)
and S3-compatible object storage.
"""

from __future__ import annotations

import io
from typing import Protocol

import boto3
from sslog import logger

from app.db import Database


class TorrentStore(Protocol):
    """Minimal interface for persisting raw torrent file bytes."""

    def put(self, tid: int, info_hash: str, content: bytes) -> None: ...

    def get(self, tid: int) -> bytes | None: ...


def create_torrent_store(config: object, db: Database) -> TorrentStore:
    """Create the appropriate :class:`TorrentStore` based on *config*.

    When S3 credentials are configured (``s3_enabled``), an
    :class:`S3TorrentStore` is returned; otherwise the legacy
    :class:`PgTorrentStore` is used.

    *config* is typed as ``object`` to avoid a circular import with
    ``app.config``; at runtime it is expected to be a :class:`Config`
    instance.
    """
    from app.config import Config

    assert isinstance(config, Config)
    if config.s3_enabled:
        assert config.s3_endpoint is not None
        assert config.s3_bucket is not None
        assert config.s3_access_key is not None
        assert config.s3_secret_key is not None
        logger.info(
            "using S3 torrent store (endpoint={}, bucket={})",
            config.s3_endpoint,
            config.s3_bucket,
        )
        return S3TorrentStore(
            db,
            endpoint_url=config.s3_endpoint,
            bucket=config.s3_bucket,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            region=config.s3_region or "",
            prefix=config.s3_prefix,
        )
    logger.info("using PostgreSQL torrent store")
    return PgTorrentStore(db)


class PgTorrentStore:
    """Store torrent content in the PostgreSQL ``torrent`` table (legacy)."""

    def __init__(self, db: Database) -> None:
        self.__db = db

    def put(self, tid: int, info_hash: str, content: bytes) -> None:
        self.__db.execute(
            """
            insert into torrent (tid, info_hash, content)
            VALUES ($1, $2, $3)
            on conflict (tid) do nothing
            """,
            [tid, info_hash, content],
        )

    def get(self, tid: int) -> bytes | None:
        return self.__db.fetch_val(
            "select content from torrent where tid = $1 limit 1",
            [tid],
        )


class S3TorrentStore:
    """Store torrent content in an S3-compatible object store.

    Objects are stored under ``{prefix}{tid}.torrent`` in the configured
    bucket.  A metadata-only row (without ``content``) is still written to
    the ``torrent`` table so that daily-stats counting queries keep working.

    **Fallback behaviour**: :meth:`get` first tries S3; if the object does
    not exist it falls back to the PostgreSQL ``torrent.content`` column.
    When a fallback hit occurs the content is automatically uploaded to S3
    and the PG row is cleared, effectively migrating the row on-the-fly.
    """

    def __init__(
        self,
        db: Database,
        *,
        endpoint_url: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "",
        prefix: str = "torrents/",
    ) -> None:
        self.__db = db
        self.__bucket = bucket
        self.__prefix = prefix

        kwargs: dict[str, str] = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
        }
        if region:
            kwargs["region_name"] = region
        self.__s3 = boto3.client("s3", **kwargs)

    @property
    def s3_client(self) -> object:
        """Expose the underlying boto3 S3 client (used by the migration task)."""
        return self.__s3

    @property
    def bucket(self) -> str:
        return self.__bucket

    @property
    def prefix(self) -> str:
        return self.__prefix

    def _key(self, tid: int) -> str:
        return f"{self.__prefix}{tid}.torrent"

    def put(self, tid: int, info_hash: str, content: bytes) -> None:
        key = self._key(tid)
        self.__s3.upload_fileobj(io.BytesIO(content), self.__bucket, key)
        logger.debug("uploaded torrent {} to s3://{}/{}", tid, self.__bucket, key)

        # Insert a metadata-only row so count(*) queries in daily_stats keep working.
        self.__db.execute(
            """
            insert into torrent (tid, info_hash, content)
            VALUES ($1, $2, ''::bytea)
            on conflict (tid) do nothing
            """,
            [tid, info_hash],
        )

    def upload_bytes(self, tid: int, content: bytes) -> None:
        """Upload raw bytes to S3 without touching the database row."""
        key = self._key(tid)
        self.__s3.upload_fileobj(io.BytesIO(content), self.__bucket, key)

    def get(self, tid: int) -> bytes | None:
        key = self._key(tid)
        try:
            resp = self.__s3.get_object(Bucket=self.__bucket, Key=key)
            return resp["Body"].read()  # type: ignore[no-any-return]
        except self.__s3.exceptions.NoSuchKey:
            pass

        # Fallback: read from PG and lazily migrate to S3
        content: bytes | None = self.__db.fetch_val(
            "select content from torrent where tid = $1 and length(content) > 0 limit 1",
            [tid],
        )
        if content is None:
            logger.warning("torrent {} not found in S3 or PG", tid)
            return None

        logger.info("torrent {} found in PG, migrating to S3", tid)
        self.__s3.upload_fileobj(io.BytesIO(content), self.__bucket, key)
        self.__db.execute(
            "update torrent set content = ''::bytea where tid = $1",
            [tid],
        )
        return content
