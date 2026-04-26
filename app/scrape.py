import enum
import io
import os
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import orjson
import psycopg.rows
import pydantic
import zstandard
from bencode2 import BencodeDecodeError
from sslog import logger

from app.config import ScrapeConfig
from app.const import PRIORITY_CATEGORY, SELECTED_CATEGORY
from app.db import Database
from app.kv import KVConfig
from app.mt import MTeamAPI, MTeamRequestError, TorrentFileError, httpx_network_errors
from app.torrent import find_largest_video_file, parse_torrent
from app.torrent_store import TorrentStore, _create_operator
from app.utils import get_info_hash_v1_from_content, parse_obj

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


class RunResult(enum.Enum):
    ok = "ok"
    rate_limited = "rate_limited"
    error = "error"


class Scrape:
    mteam_client: MTeamAPI
    __db: Database

    def __init__(self, c: ScrapeConfig):
        self.__db = Database(c.pg_dsn())
        self.__db.wait_db_migration()
        self.mteam_client = MTeamAPI(c)
        self.__store = TorrentStore(c)
        self.__op = _create_operator(c)

        self.__kv = KVConfig(self.__db)

    def scrape_detail(self, limit: int = 0) -> None:
        """Fetch torrent details for threads missing mediainfo, or fill tid gaps."""

        effective_limit = limit or 100

        threads = self.__db.fetch_all(
            """
            select tid from thread
            where
              deleted = false and
              mediainfo_at is null and
              seeders != 0 and
              category = any($1)
            order by (category = any($3)) desc, tid asc
            limit $2
            """,
            [SELECTED_CATEGORY, effective_limit, PRIORITY_CATEGORY],
        )

        if not threads:
            # No pending threads, try to fill gaps in tid sequence
            threads = self.__db.fetch_all(
                """
                select s.gap_tid as tid from (
                    select tid, lead(tid) over (order by tid) as next_tid
                    from thread
                ) t, lateral generate_series(t.tid + 1, t.next_tid - 1) as s(gap_tid)
                where t.next_tid - t.tid > 1 and t.next_tid is not null
                order by s.gap_tid asc
                limit $1
                """,
                [effective_limit],
            )

        for (tid,) in threads:
            logger.info("fetch detail {}", tid)
            try:
                r = self.mteam_client.torrent_detail(tid)
            except MTeamRequestError as e:
                if e.message == "種子未找到":
                    self.__db.execute(
                        """
                        insert into thread (tid, deleted)
                        values ($1, true)
                        on conflict (tid) do update set deleted = true
                        """,
                        [tid],
                    )
                    continue
                raise

            self.__db.execute(
                """
                insert into thread (tid, size, mediainfo, category, seeders, deleted, mediainfo_at)
                values ($1, $2, $3, $4, $5, false, current_timestamp)
                on conflict (tid) do update set
                  size = excluded.size,
                  mediainfo = excluded.mediainfo,
                  category = excluded.category,
                  seeders = excluded.seeders,
                  deleted = false,
                  mediainfo_at = current_timestamp
                """,
                [
                    tid,
                    r.size,
                    (r.mediainfo or "").replace("\x00", ""),
                    int(r.category),
                    r.status.seeders,
                ],
            )

    def scrape_search(self) -> None:
        """Scrape thread list using /torrent/search sorted by CREATED_DATE ASC.

        Resumes from the cursor stored in the config table (key 'search_cursor'),
        fetches one page at a time. Topped torrents (toppingLevel != "0") appear
        first and are excluded when advancing the cursor.
        """
        row = self.__kv.get("search_cursor")
        if row is not None:
            cursor = datetime.strptime(row, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_SHANGHAI)
        else:
            cursor = datetime(1970, 1, 1, tzinfo=TZ_SHANGHAI)

        while True:
            start_str = cursor.strftime("%Y-%m-%d %H:%M:%S")

            result = self.mteam_client.search(
                upload_date_start=start_str,
                page_size=100,
                sort_field="CREATED_DATE",
                sort_direction="ASC",
            )

            if not result.data:
                break

            logger.info(
                "search from {}: {} items (total {})",
                start_str,
                len(result.data),
                result.total,
            )

            for item in result.data:
                self.__db.execute(
                    """
                    insert into thread (tid, size, category, seeders, deleted, upload_at)
                    values ($1, $2, $3, $4, false, $5)
                    on conflict (tid) do update set
                    size = excluded.size,
                    category = excluded.category,
                    seeders = excluded.seeders,
                    upload_at = excluded.upload_at,
                    deleted = false
                    """,
                    [
                        int(item.id),
                        item.size,
                        int(item.category),
                        item.status.seeders,
                        item.createdDate,
                    ],
                )

            # Advance cursor using the last non-topped item's createdDate
            non_topped = [i for i in result.data if i.status.toppingLevel == "0"]
            if not non_topped:
                # All items are topped, no progress possible
                break

            last_date = non_topped[-1].createdDate
            new_cursor = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=TZ_SHANGHAI
            )
            if new_cursor <= cursor:
                break
            cursor = new_cursor

            self.__kv.set("search_cursor", last_date)

    def fetch_torrent(self) -> bool:
        threads = self.__db.fetch_all(
            """
            select tid from thread
            where
              deleted = false and
              info_hash = '' and
              mediainfo_at is not null and
              mediainfo = '' and
              torrent_invalid = '' and
              seeders != 0 and
              category = any($1)
            order by (category = any($2)) desc, tid asc
            limit 50
            """,
            [SELECTED_CATEGORY, PRIORITY_CATEGORY],
        )

        if not threads:
            return True

        for (tid,) in threads:
            logger.info("fetch torrent of thread {}", tid)
            try:
                tc = self.mteam_client.download_torrent(tid=tid)
            except TorrentFileError:
                logger.warning("torrent file error for thread {}", tid)
                self.__db.execute(
                    """update thread set torrent_invalid = $2 where tid = $1""",
                    [tid, "file error"],
                )
                continue

            try:
                t = parse_torrent(tc)
            except (pydantic.ValidationError, BencodeDecodeError):
                logger.exception("failed to parse torrent of {}", tid)
                self.__db.execute(
                    """update thread set torrent_invalid = $2 where tid = $1""",
                    [tid, "parse error"],
                )
                continue

            info_hash = get_info_hash_v1_from_content(tc)

            files_data = [(i, f.name, f.length) for i, f in enumerate(t.as_files())]
            keep_idx = find_largest_video_file(files_data)
            selected_size = (
                next((size for i, _, size in files_data if i == keep_idx), -1)
                if keep_idx is not None
                else -1
            )

            with self.__db.connection() as conn, conn.transaction():
                self.__store.write(tid, tc)
                conn.execute(
                    """update thread set info_hash = $2, size = $3, selected_size = $4, torrent_fetched_at = current_timestamp where tid = $1""",
                    [tid, info_hash, t.total_length, selected_size],
                )
        return False

    def backfill_selected_size(self) -> None:
        """Backfill selected_size for threads that already have a torrent but selected_size=0."""
        while True:
            tids: list[tuple[int]] = self.__db.fetch_all(
                """
                select tid from thread
                where selected_size = 0 and info_hash != ''
                limit 200
                """,
            )

            if not tids:
                return

            for (tid,) in tids:
                tc = self.__store.read(tid)
                if tc is None:
                    continue
                try:
                    t = parse_torrent(tc)
                except (pydantic.ValidationError, BencodeDecodeError):
                    continue

                files_data = [(i, f.name, f.length) for i, f in enumerate(t.as_files())]
                keep_idx = find_largest_video_file(files_data)
                selected_size = (
                    next((size for i, _, size in files_data if i == keep_idx), -1)
                    if keep_idx is not None
                    else -1
                )

                self.__db.execute(
                    """update thread set size = $2, selected_size = $3 where tid = $1""",
                    [tid, t.total_length, selected_size],
                )

    @staticmethod
    def __is_rate_limited(e: MTeamRequestError) -> bool:
        return e.message in ("請求過於頻繁", "今日下載配額用盡")

    def __run_fetch(self) -> RunResult:
        """Returns (result, no_pending)."""
        try:
            self.backfill_selected_size()
            self.fetch_torrent()
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to fetch torrents")
            return RunResult.error
        return RunResult.ok

    def __run_scrape(self, limit: int) -> RunResult:
        try:
            self.scrape_detail(limit=limit)
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to fetch threads")
            return RunResult.error
        return RunResult.ok

    def __run_search(self) -> RunResult:
        try:
            self.scrape_search()
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to search threads")
            return RunResult.error
        return RunResult.ok

    def scrape_mediainfo(self, limit: int = 100) -> None:
        """Fetch mediainfo via /torrent/mediaInfo for threads missing it."""
        threads = self.__db.fetch_all(
            """
            select tid from thread
            where
              deleted = false and
              mediainfo_at is null and
              seeders != 0 and
              category = any($1)
            order by (category = any($3)) desc, tid asc
            limit $2
            """,
            [SELECTED_CATEGORY, limit, PRIORITY_CATEGORY],
        )

        for (tid,) in threads:
            logger.info("fetch mediainfo {}", tid)
            try:
                mediainfo = self.mteam_client.torrent_mediainfo(tid)
            except MTeamRequestError as e:
                if e.message == "種子未找到":
                    self.__db.execute(
                        """
                        update thread set deleted = true where tid = $1
                        """,
                        [tid],
                    )
                    continue
                raise

            self.__db.execute(
                "update thread set mediainfo = $2, mediainfo_at = current_timestamp where tid = $1",
                [tid, (mediainfo or "").replace("\x00", "")],
            )

    def __run_mediainfo(self, limit: int) -> RunResult:
        try:
            self.scrape_mediainfo(limit=limit)
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to fetch mediainfo")
            return RunResult.error
        return RunResult.ok

    def backup_to_s3(self, backup_date: date) -> None:
        """Dump thread and job tables as zstd-compressed JSON Lines to S3."""
        cctx = zstandard.ZstdCompressor()
        for table in ("thread", "job", "node"):
            with (
                io.BytesIO() as buf,
                self.__db.connection() as conn,
                conn.cursor(row_factory=psycopg.rows.dict_row) as cur,
            ):
                raw_size = 0
                with cctx.stream_writer(buf, closefd=False) as writer:
                    for row in cur.stream(f"SELECT * FROM {table}"):
                        encoded = orjson.dumps(row)
                        writer.write(encoded)
                        writer.write(b"\n")
                        raw_size += len(encoded)
                        raw_size += 1
                key = f"backups/{backup_date}/{table}.jsonl.zst"
                self.__op.write(
                    key,
                    buf.getvalue(),
                    user_metadata={"raw-size": str(raw_size)},
                )
                logger.info("backed up {} to s3 ({})", table, key)
        self.__kv.set("last_backup_date", backup_date.isoformat())

        cutoff = backup_date - timedelta(days=7)
        for entry in self.__op.scan("backups/"):
            # entry.path looks like "backups/2026-04-12/thread.jsonl.zst"
            parts = entry.path.split("/")
            if len(parts) < 2:
                continue
            try:
                entry_date = date.fromisoformat(parts[1])
            except ValueError:
                continue
            if entry_date < cutoff:
                self.__op.delete(entry.path)
                logger.info("deleted old backup {}", entry.path)

    def __run_backup(self) -> RunResult:
        today = datetime.now(TZ_SHANGHAI).date()
        last = self.__kv.get("last_backup_date")
        if last == today.isoformat():
            logger.info("skipping backup (already done today)")
            return RunResult.ok
        try:
            self.backup_to_s3(today)
        except Exception:
            logger.exception("failed to backup to s3")
            return RunResult.error
        return RunResult.ok

    def __run(self) -> None:
        limit = parse_obj(int, os.environ.get("SCRAPE_LIMIT", "100"))
        cooldown = timedelta(minutes=20)
        interval = 2 * 60  # 2 minutes

        # Earliest time each operation is allowed to run again
        epoch = datetime.now(TZ_SHANGHAI)
        next_allowed: dict[str, datetime] = {
            "fetch": epoch,
            "search": epoch,
            "mediainfo": epoch,
            "scrape": epoch,
            "backup": epoch,
        }

        runners: dict[str, Callable[[], RunResult]] = {
            "fetch": lambda: self.__run_fetch(),
            "search": lambda: self.__run_search(),
            "mediainfo": lambda: self.__run_mediainfo(limit),
            "scrape": lambda: self.__run_scrape(limit),
            "backup": lambda: self.__run_backup(),
        }

        while True:
            logger.info("fetch torrents")
            now = datetime.now(TZ_SHANGHAI)

            for name, run in runners.items():
                if now < next_allowed[name]:
                    logger.info(
                        "skipping {} (rate-limited until {})",
                        name,
                        next_allowed[name].strftime("%H:%M:%S"),
                    )
                    continue
                result = run()
                if result == RunResult.rate_limited:
                    next_allowed[name] = datetime.now(TZ_SHANGHAI) + cooldown

            status_parts: list[str] = []
            for name in runners:
                if datetime.now(TZ_SHANGHAI) < next_allowed[name]:
                    status_parts.append(
                        f"{name}: limited until {next_allowed[name].strftime('%H:%M:%S')}"
                    )
                else:
                    status_parts.append(f"{name}: ok")
            logger.info("status: {}", ", ".join(status_parts))

            time.sleep(interval)

    def start(self) -> None:
        self.__run()
