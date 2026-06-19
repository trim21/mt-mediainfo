from __future__ import annotations

import asyncio
import gzip

import asyncpg
import opendal
import orjson
from sslog import logger

from app.db import Database
from app.torrent import File, parse_torrent
from app.torrent_store import TorrentStore, _s3_key


def encode_cached_files(files: list[File]) -> bytes:
    data = [{"p": f.path, "l": f.length} for f in files]
    return gzip.compress(orjson.dumps(data))


def decode_cached_files(data: bytes) -> list[File]:
    raw = orjson.loads(gzip.decompress(data))
    return [File(length=r["l"], path=tuple(r["p"])) for r in raw]


async def get_cached_files(
    tid: int,
    pool: asyncpg.Pool,
    s3_op: opendal.Operator,
) -> list[File] | None:
    row = await pool.fetchrow("select files from thread_file_cache where tid = $1", tid)
    if row is not None:
        try:
            return decode_cached_files(row[0])
        except Exception:
            logger.warning("failed to decode cached files for tid {}", tid)

    try:
        tc = await asyncio.to_thread(s3_op.read, _s3_key(tid))
    except opendal.exceptions.NotFound:
        return None
    t = parse_torrent(tc)
    files = t.as_files()
    await pool.execute(
        """insert into thread_file_cache (tid, files) values ($1, $2) on conflict (tid) do update set files = excluded.files""",
        tid,
        encode_cached_files(files),
    )
    return files


def get_torrent_files(
    tid: int,
    db: Database,
    store: TorrentStore,
) -> list[File] | None:
    row = db.fetch_one("select files from thread_file_cache where tid = $1", [tid])
    if row is not None:
        try:
            return decode_cached_files(row[0])
        except Exception:
            logger.warning("failed to decode cached files for tid {}", tid)

    tc = store.read(tid)
    if tc is None:
        return None
    t = parse_torrent(tc)
    files = t.as_files()
    db.execute(
        """insert into thread_file_cache (tid, files) values ($1, $2) on conflict (tid) do update set files = excluded.files""",
        [tid, encode_cached_files(files)],
    )
    return files
