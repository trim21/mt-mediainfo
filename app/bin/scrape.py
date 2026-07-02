import enum
import io
import os
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from multiprocessing.pool import ThreadPool
from typing import Any, LiteralString, cast

import orjson
import psycopg.rows
import pydantic
from bencode2 import BencodeDecodeError
from sslog import logger

from app._zstd import writer as zstd_writer
from app.config import ScrapeConfig
from app.const import (
    PRIORITY_CATEGORY,
    SELECTED_CATEGORY,
    TZ_SHANGHAI,
    search_cursor_key,
)
from app.db import Database
from app.db.kv import KVConfig
from app.file_cache import encode_cached_files, get_torrent_files
from app.mt import MTeamAPI, MTeamRequestError, TorrentFileError, httpx_network_errors
from app.torrent import (
    compute_bdmv_selection,
    compute_selection,
    is_bdmv,
    is_bdmv_from_files,
    parse_torrent,
)
from app.torrent_store import TorrentStore, create_operator
from app.utils import date_to_int, get_info_hash_v1_from_content, parse_obj


class RunResult(enum.Enum):
    ok = "ok"
    rate_limited = "rate_limited"
    error = "error"


class RunStatus(str, enum.Enum):
    running = "running"
    ok = "ok"
    rate_limited = "rate_limited"
    error = "error"


class Scrape:
    mteam_client: MTeamAPI
    __db: Database
    __config: ScrapeConfig

    KV_QUOTA_EXHAUSTED = "quota_exhausted.today"

    def __init__(self, c: ScrapeConfig):
        self.__config = c
        self.__db = Database(c.pg_dsn())
        self.__db.wait_db_migration()
        self.mteam_client = MTeamAPI(c)
        self.__store = TorrentStore(c)
        self.__op = create_operator(c)

        self.__kv = KVConfig(self.__db)

    def _record_quota_exhausted(self) -> None:
        key = self.KV_QUOTA_EXHAUSTED + "." + datetime.now(TZ_SHANGHAI).isoformat()
        self.__kv.set(key, "1", ttl=timedelta(hours=4))

    def _is_quota_exhausted_today(self) -> bool:
        key = self.KV_QUOTA_EXHAUSTED + "." + datetime.now(TZ_SHANGHAI).isoformat()
        return self.__kv.get(key) is not None

    def _log_scrape_error(self, tid: int, op: str, e: MTeamRequestError) -> None:
        self.__db.execute(
            """insert into scrape_error (tid, op, code, message) values ($1, $2, $3, $4)""",
            [tid, op, e.code, e.message],
        )

    def scrape_detail(self, limit: int = 0) -> None:
        """Fetch torrent details for threads missing mediainfo, or fill tid gaps."""

        effective_limit = limit or 100

        threads = self.__db.fetch_all(
            """
            select tid from pending_mediainfo_threads
            where category = any($1)
            order by (mediainfo = '') desc, (category = any($3)) desc, seeders desc, tid asc
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
                insert into thread (tid, size, api_mediainfo, category, seeders, deleted, api_mediainfo_at)
                values ($1, $2, $3, $4, $5, false, current_timestamp)
                on conflict (tid) do update set
                  size = excluded.size,
                  api_mediainfo = excluded.api_mediainfo,
                  category = excluded.category,
                  seeders = excluded.seeders,
                  deleted = false,
                  api_mediainfo_at = current_timestamp
                """,
                [
                    tid,
                    r.size,
                    (r.mediainfo or "").replace("\x00", ""),
                    int(r.category),
                    r.status.seeders,
                ],
            )

    def scrape_search(self, *, mode: str) -> None:
        """Scrape thread list using /torrent/search sorted by CREATED_DATE ASC.

        Resumes from the cursor stored in the config table,
        fetches one page at a time. Topped torrents (toppingLevel != "0") appear
        first and are excluded when advancing the cursor.
        """
        cursor_key = search_cursor_key(mode)
        row = self.__kv.get(cursor_key)
        if row is not None:
            cursor = datetime.strptime(row, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ_SHANGHAI)
        else:
            cursor = datetime(1970, 1, 1, tzinfo=TZ_SHANGHAI)

        pages = 0
        while pages < 5:
            start_str = cursor.strftime("%Y-%m-%d %H:%M:%S")

            result = self.mteam_client.search(
                upload_date_start=start_str,
                page_size=100,
                sort_field="CREATED_DATE",
                sort_direction="ASC",
                mode=mode,
            )

            if not result.data:
                break

            pages += 1
            logger.info(
                "search({}) from {}: {} items (total {})",
                mode,
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

            self.__kv.set(cursor_key, last_date, ttl=timedelta(days=100))

    TORRENT_DL_LIMIT = 10
    TORRENT_DL_TTL = timedelta(days=2)
    DAILY_TORRENT_LIMIT = 2900
    DAILY_TORRENT_TTL = timedelta(days=2)

    def _torrent_dl_count_key(self, tid: int, today: str) -> str:
        return f"torrent_dl:{today}:{tid}"

    def _get_torrent_dl_count(self, tid: int, today: str) -> int:
        val = self.__kv.get(self._torrent_dl_count_key(tid, today))
        return int(val) if val else 0

    def _inc_torrent_dl_count(self, tid: int, today: str) -> None:
        key = self._torrent_dl_count_key(tid, today)
        val = self.__kv.get(key)
        self.__kv.set(key, str(int(val) + 1) if val else "1", ttl=self.TORRENT_DL_TTL)

    def _daily_torrent_count_key(self, today: str) -> str:
        return f"daily_torrent_dl:{today}"

    def _get_daily_torrent_count(self, today: str) -> int:
        val = self.__kv.get(self._daily_torrent_count_key(today))
        return int(val) if val else 0

    def _inc_daily_torrent_count(self, today: str) -> None:
        key = self._daily_torrent_count_key(today)
        val = self.__kv.get(key)
        self.__kv.set(key, str(int(val) + 1) if val else "1", ttl=self.DAILY_TORRENT_TTL)

    def fetch_torrent(self) -> bool:
        threads = self.__db.fetch_all(
            """
            select tid from pending_torrent_threads
            where category = any($1)
            order by (category = any($2)) desc, seeders desc, tid asc
            limit 100
            """,
            [SELECTED_CATEGORY, PRIORITY_CATEGORY],
        )

        if not threads:
            threads = self.__db.fetch_all(
                """
                select tid from thread
                where deleted = false
                  and seeders = 0
                  and category = any($1)
                  and api_mediainfo_at is not null
                  and mediainfo = ''
                  and api_mediainfo = ''
                  and info_hash = ''
                  and torrent_invalid = ''
                order by (category = any($2)) desc, tid asc
                limit 100
                """,
                [SELECTED_CATEGORY, PRIORITY_CATEGORY],
            )

        if not threads:
            return True

        today = datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")

        for (tid,) in threads:
            if self._get_daily_torrent_count(today) >= self.DAILY_TORRENT_LIMIT:
                logger.info(
                    "daily torrent download limit reached ({}/{}), stopping",
                    self._get_daily_torrent_count(today),
                    self.DAILY_TORRENT_LIMIT,
                )
                return False

            count = self._get_torrent_dl_count(tid, today)
            if count >= self.TORRENT_DL_LIMIT:
                logger.debug(
                    "skipping torrent {} (downloaded {}/{} today)",
                    tid,
                    count,
                    self.TORRENT_DL_LIMIT,
                )
                continue

            logger.info("fetch torrent of thread {}", tid)
            try:
                tc = self.mteam_client.download_torrent(tid=tid)
            except MTeamRequestError as e:
                if "相同種子當天最多下載" in e.message:
                    self.__kv.set(
                        self._torrent_dl_count_key(tid, today),
                        str(self.TORRENT_DL_LIMIT),
                        ttl=self.TORRENT_DL_TTL,
                    )
                    logger.warning(
                        "torrent {} hit daily download limit, skipping until tomorrow",
                        tid,
                    )
                    continue
                self._log_scrape_error(tid, "fetch_torrent", e)
                if self.__is_rate_limited(e):
                    raise
                logger.warning("fetch torrent {} failed: {} {}", tid, e.code, e.message)
                continue
            except TorrentFileError:
                logger.warning("torrent file error for thread {}", tid)
                self.__db.execute(
                    """update thread set torrent_invalid = $2 where tid = $1""",
                    [tid, "file error"],
                )
                continue

            self._inc_torrent_dl_count(tid, today)
            self._inc_daily_torrent_count(today)

            try:
                t = parse_torrent(tc)
            except pydantic.ValidationError, BencodeDecodeError:
                logger.exception("failed to parse torrent of {}", tid)
                self.__db.execute(
                    """update thread set torrent_invalid = $2 where tid = $1""",
                    [tid, "parse error"],
                )
                continue

            info_hash = get_info_hash_v1_from_content(tc)

            files = t.as_files()

            typ = "bdmv" if is_bdmv(torrent=t) else ""
            if typ:
                logger.info("torrent {} is bdmv", tid)
                selected_size, selected_index, priority = compute_bdmv_selection(files)
            else:
                selected_size, selected_index, priority = compute_selection(files)

            cached = encode_cached_files(files)

            with self.__db.connection() as conn, conn.transaction():
                self.__store.write(tid, tc)
                conn.execute(
                    """update thread set info_hash = $2, size = $3, selected_size = $4, selected_index = $5, priority = $6, type = $7, torrent_fetched_at = current_timestamp where tid = $1""",
                    [
                        tid,
                        info_hash,
                        t.total_length,
                        selected_size,
                        selected_index,
                        priority,
                        typ,
                    ],
                )
                conn.execute(
                    """insert into thread_file_cache (tid, files) values ($1, $2) on conflict (tid) do update set files = excluded.files""",
                    [tid, cached],
                )
        return False

    def _backfill_bdmv(self, tid: int) -> None:
        """Re-identify BDMV threads: update selected_size and selected_index from cached files."""
        files = get_torrent_files(tid, self.__db, self.__store)
        if not files:
            return

        typ = "bdmv" if is_bdmv_from_files(files) else ""
        total_length = sum(f.length for f in files)
        if typ:
            selected_size, selected_index, _priority = compute_bdmv_selection(files)
        else:
            selected_size, selected_index, _priority = compute_selection(files)
        self.__db.execute(
            """update thread set selected_size = $2, selected_index = $3, size = $4, type = $5 where tid = $1""",
            [tid, selected_size, selected_index, total_length, typ],
        )

    def run_backfill(
        self,
        name: str,
        source: str,
        handler: Callable[[int], None],
        *,
        args: Sequence[Any] = (),
        status_name: str = "",
        concurrency: int = 8,
    ) -> RunResult:
        total = self.__db.fetch_val("select count(*) from backfill_task where name = $1", [name])

        if total == 0:
            query = cast(
                LiteralString,
                f"insert into backfill_task (name, tid) select $1, tid from ({source}) t on conflict do nothing",
            )
            self.__db.execute(query, [name, *args])
            total = self.__db.fetch_val(
                "select count(*) from backfill_task where name = $1", [name]
            )
            if total == 0:
                return RunResult.ok

        pending_count = self.__db.fetch_val(
            "select count(*) from backfill_task where name = $1 and status = 'pending'",
            [name],
        )
        if pending_count == 0:
            if status_name:
                self.__update_detail(status_name, f"done {total}/{total}")
            return RunResult.ok

        tids: list[tuple[int]] = self.__db.fetch_all(
            "select tid from backfill_task where name = $1 and status = 'pending' order by tid",
            [name],
        )

        done_count = total - pending_count
        error_count = 0

        def _run_one(tid: int) -> bool:
            try:
                handler(tid)
                self.__db.execute(
                    "update backfill_task set status = 'done', updated_at = current_timestamp where name = $1 and tid = $2",
                    [name, tid],
                )
                return True
            except Exception:
                logger.exception("backfill {} failed for tid {}", name, tid)
                self.__db.execute(
                    "update backfill_task set status = 'error', error = $3, updated_at = current_timestamp where name = $1 and tid = $2",
                    [name, tid, "handler error"],
                )
                return False

        with ThreadPool(processes=concurrency) as pool:
            for ok in pool.imap_unordered(_run_one, [tid for (tid,) in tids]):
                done_count += 1
                if not ok:
                    error_count += 1
                if status_name:
                    self.__update_detail(status_name, f"{done_count}/{total}")

        if error_count:
            return RunResult.error
        return RunResult.ok

    @staticmethod
    def __is_rate_limited(e: MTeamRequestError) -> bool:
        return e.message == "請求過於頻繁"

    def __run_fetch_torrents(self) -> RunResult:
        """Returns (result, no_pending)."""
        if self._is_quota_exhausted_today():
            logger.info("daily download quota exhausted for today, skipping fetch_torrent")
            return RunResult.rate_limited
        try:
            self.fetch_torrent()
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if e.message == "今日下載配額用盡":
                self._record_quota_exhausted()
                logger.info("daily download quota exhausted for today")
                return RunResult.rate_limited
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
            self.scrape_search(mode="normal")
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to search threads")
            return RunResult.error

        try:
            self.scrape_search(mode="adult")
        except httpx_network_errors:
            return RunResult.error
        except MTeamRequestError as e:
            if self.__is_rate_limited(e):
                logger.info("operator {!r} get rate limited: {}", e.op, e.message)
                return RunResult.rate_limited
            logger.exception("failed to search adult threads")
            return RunResult.error
        return RunResult.ok

    def scrape_mediainfo(self, limit: int = 10000) -> None:
        """Fetch mediainfo via /torrent/mediaInfo for threads missing it."""
        threads = self.__db.fetch_all(
            """
            select tid from pending_mediainfo_threads
            order by (category = any($2)) desc, seeders desc, tid asc
            limit $1
            """,
            [limit, SELECTED_CATEGORY],
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
                "update thread set api_mediainfo = $2, api_mediainfo_at = current_timestamp where tid = $1",
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
        for table in ("thread", "job", "node"):
            with (
                io.BytesIO() as buf,
                self.__db.connection() as conn,
                conn.cursor(row_factory=psycopg.rows.dict_row) as cur,
            ):
                raw_size = 0
                with zstd_writer(buf) as w:
                    for row in cur.stream(f"SELECT * FROM {table}"):
                        encoded = orjson.dumps(row)
                        w.write(encoded)
                        w.write(b"\n")
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
            if entry_date < cutoff and entry_date.day != 1 and entry_date != date(2026, 5, 30):
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

    def _pg_dump_to_s3(self, backup_date: date) -> None:
        """Dump the entire database with pg_dump and upload to S3."""
        c = self.__config

        env = os.environ.copy()
        env["PGPASSWORD"] = c.pg_password or ""

        args = [
            "pg_dump",
            "--host",
            c.pg_host,
            "--port",
            str(c.pg_port),
            "--username",
            c.pg_user or "postgres",
            "--dbname",
            c.pg_db,
            "--no-owner",
            "--no-acl",
            "--no-comments",
            "--exclude-table-data",
            "thread_file_cache",
        ]

        if c.pg_sslmode:
            env["PGSSLMODE"] = c.pg_sslmode
        if c.pg_ssl_rootcert:
            env["PGSSLROOTCERT"] = c.pg_ssl_rootcert
        if c.pg_ssl_cert:
            env["PGSSLCERT"] = c.pg_ssl_cert
        if c.pg_ssl_key:
            env["PGSSLKEY"] = os.path.join(tempfile.gettempdir(), "pg-client.key")

        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_path = os.path.join(tmp_dir, "dump.sql")
            args_with_file = [*args, "--file", sql_path]

            result = subprocess.run(
                args_with_file,
                env=env,
                capture_output=True,
                check=False,
            )

            if result.returncode != 0:
                err = result.stderr.decode(errors="replace")
                logger.error("pg_dump failed (exit {}): {}", result.returncode, err)
                raise RuntimeError(f"pg_dump failed: {err}")

            zst_path = os.path.join(tmp_dir, "dump.sql.zst")
            with (
                open(sql_path, "rb") as src,
                open(zst_path, "wb") as dst,
                zstd_writer(dst) as w,
            ):
                while chunk := src.read(65536):
                    w.write(chunk)

            key = f"pg_dumps/{backup_date}/dump.sql.zst"
            with (
                open(zst_path, "rb") as src,
                self.__op.open(key, "wb") as s3,
            ):
                while chunk := src.read(65536):
                    s3.write(chunk)
            logger.info("pg_dump backed up to s3 ({})", key)

        cutoff = backup_date - timedelta(days=7)
        for entry in self.__op.scan("pg_dumps/"):
            parts = entry.path.split("/")
            if len(parts) < 2:
                continue
            try:
                entry_date = date.fromisoformat(parts[1])
            except ValueError:
                continue
            if entry_date < cutoff and entry_date.day != 1 and entry_date != date(2026, 5, 30):
                self.__op.delete(entry.path)
                logger.info("deleted old pg_dump {}", entry.path)

    def __run_pg_dump(self) -> RunResult:
        today = datetime.now(TZ_SHANGHAI).date()
        last = self.__kv.get("last_pg_dump_date")
        if last == today.isoformat():
            logger.info("skipping pg_dump (already done today)")
            return RunResult.ok
        try:
            self._pg_dump_to_s3(today)
            self.__kv.set("last_pg_dump_date", today.isoformat())
        except Exception:
            logger.exception("failed to pg_dump to s3")
            return RunResult.error
        return RunResult.ok

    def _export_mediainfo_to_s3(self, export_date: date) -> int:
        """Export incremental mediainfo and return count."""
        date_int = date_to_int(export_date)
        date_str = export_date.isoformat()
        key = f"exports/{export_date}/mediainfo_export.jsonl.zst"

        tids: list[int] = []
        compressed: bytes = b""
        with (
            self.__db.connection() as conn,
            conn.cursor(row_factory=psycopg.rows.dict_row) as cur,
        ):
            buf = io.BytesIO()
            with zstd_writer(buf) as writer:
                for row in cur.stream(
                    """select tid, mediainfo, hard_coded_subtitle from thread
                       where api_mediainfo != ''
                         and mediainfo != ''
                         and mediainfo != api_mediainfo
                         and exported_at = 0
                         and seeders != 0
                         and deleted = false"""
                ):
                    entry = orjson.dumps({
                        "id": row["tid"],
                        "mediainfo": row["mediainfo"],
                        "hardcoded_subtitle": row["hard_coded_subtitle"],
                    })
                    writer.write(entry + b"\n")
                    tids.append(row["tid"])

            compressed = buf.getvalue()

        if not tids:
            logger.info("no new mediainfo to export")
            return 0

        self.__db.execute(
            """insert into export_record (export_date, status, exported_count)
               values ($1, 'running', $2)
               on conflict (export_date) do update set
                 status = excluded.status,
                 exported_count = excluded.exported_count,
                 error = ''""",
            [date_str, len(tids)],
        )

        self.__db.execute(
            "update thread set exported_at = $1 where tid = any($2)",
            [date_int, tids],
        )

        self.__op.write(key, compressed)

        self.__db.execute(
            "update export_record set status = 'success' where export_date = $1",
            [date_str],
        )

        logger.info("exported {} mediainfo entries to s3 ({})", len(tids), key)
        return len(tids)

    def __run_export_mediainfo(self) -> RunResult:
        today = datetime.now(TZ_SHANGHAI).date()
        if today.day != 1:  # 1st of the month
            return RunResult.ok
        last = self.__kv.get("last_export_mediainfo_date")
        if last == today.isoformat():
            logger.info("skipping mediainfo export (already done today)")
            return RunResult.ok
        try:
            self._export_mediainfo_to_s3(today)
            self.__kv.set("last_export_mediainfo_date", today.isoformat())
        except Exception as e:
            self.__db.execute(
                "update export_record set status = 'failed', error = $2 where export_date = $1",
                [today.isoformat(), str(e)],
            )
            logger.exception("failed to export mediainfo to s3")
            return RunResult.error
        return RunResult.ok

    def __update_status(
        self,
        name: str,
        result: RunStatus,
        next_allowed: datetime | None,
        *,
        category: str = "",
        detail: str = "",
    ) -> None:
        self.__db.execute(
            """
            insert into scrape_status (name, last_run_at, last_result, next_allowed_at, category, detail)
            values ($1, current_timestamp, $2, $3, $4, $5)
            on conflict (name) do update set
              last_run_at = case when excluded.last_result = 'running' then current_timestamp else scrape_status.last_run_at end,
              last_result = excluded.last_result,
              next_allowed_at = excluded.next_allowed_at,
              category = excluded.category,
              detail = excluded.detail
            """,
            [name, result.value, next_allowed, category, detail],
        )

    def __update_detail(self, name: str, detail: str) -> None:
        self.__db.execute(
            "update scrape_status set detail = $2 where name = $1",
            [name, detail],
        )

    @staticmethod
    def _backfill_task_name(status_name: str) -> str:
        _, _, name = status_name.partition("-backfill-")
        return name

    def __run_backfills(
        self,
        backfill_runners: dict[str, Callable[[], RunResult]],
        interval: int,
    ) -> None:
        while True:
            all_done = True
            for name, run in backfill_runners.items():
                backfill_name = self._backfill_task_name(name)
                total = self.__db.fetch_val(
                    "select count(*) from backfill_task where name = $1",
                    [backfill_name],
                )
                pending = self.__db.fetch_val(
                    "select count(*) from backfill_task where name = $1 and status = 'pending'",
                    [backfill_name],
                )
                if total > 0 and pending == 0:
                    self.__update_status(
                        name,
                        RunStatus.ok,
                        None,
                        category="backfill",
                        detail=f"done {total}/{total}",
                    )
                    continue
                all_done = False
                self.__update_status(name, RunStatus.running, None, category="backfill")
                try:
                    run()
                except Exception:
                    logger.exception("backfill {} failed", name)
                    self.__update_status(name, RunStatus.error, None, category="backfill")
                    continue
                done = self.__db.fetch_val(
                    "select count(*) from backfill_task where name = $1 and status = 'done'",
                    [backfill_name],
                )
                remaining = self.__db.fetch_val(
                    "select count(*) from backfill_task where name = $1 and status = 'pending'",
                    [backfill_name],
                )
                status = RunStatus.running if remaining > 0 else RunStatus.ok
                self.__update_status(
                    name,
                    status,
                    None,
                    category="backfill",
                    detail=f"{done}/{total}",
                )
            if all_done:
                logger.info("all backfills completed")
                return
            time.sleep(interval)

    def __run_regular(
        self,
        runners: dict[str, Callable[[], RunResult]],
        interval: int,
    ) -> None:
        cooldown = timedelta(minutes=5)

        epoch = datetime.now(TZ_SHANGHAI)
        next_allowed: dict[str, datetime] = {key: epoch for key in runners}

        while True:
            self.__kv.cleanup()
            logger.info("scrape")
            now = datetime.now(TZ_SHANGHAI)

            for name, run in runners.items():
                if now < next_allowed[name]:
                    logger.info(
                        "skipping {} (rate-limited until {})",
                        name,
                        next_allowed[name].strftime("%H:%M:%S"),
                    )
                    continue
                self.__update_status(name, RunStatus.running, next_allowed[name], category="scrape")
                result = run()
                if result == RunResult.rate_limited:
                    next_allowed[name] = datetime.now(TZ_SHANGHAI) + cooldown
                na = next_allowed[name]
                self.__update_status(
                    name,
                    RunStatus(result.value),
                    na,
                    category="scrape",
                    detail=na.astimezone(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S"),
                )

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
        interval = 60
        scrape_limit = parse_obj(int, os.environ.get("SCRAPE_LIMIT", "10000"))

        runners: dict[str, Callable[[], RunResult]] = {
            "0-search": lambda: self.__run_search(),
            "1-mediainfo": lambda: self.__run_mediainfo(scrape_limit),
            "2-fetch-detail": lambda: self.__run_scrape(scrape_limit),
            "3-fetch-torrent": lambda: self.__run_fetch_torrents(),
            "4-backup": lambda: self.__run_backup(),
            "5-pg-dump": lambda: self.__run_pg_dump(),
            "6-export-mediainfo": lambda: self.__run_export_mediainfo(),
        }

        backfill_runners: dict[str, Callable[[], RunResult]] = {}

        all_names = list(runners) + list(backfill_runners)
        self.__db.execute("delete from scrape_status where not name = any($1)", [all_names])

        backfill_names = [self._backfill_task_name(n) for n in backfill_runners]
        self.__db.execute("delete from backfill_task where not name = any($1)", [backfill_names])

        backfill_done = threading.Event()

        def _backfill_wrapper() -> None:
            self.__run_backfills(backfill_runners, interval)
            backfill_done.set()

        t_regular = threading.Thread(
            target=self.__run_regular, args=(runners, interval), daemon=True
        )
        t_backfill = threading.Thread(target=_backfill_wrapper, daemon=True)
        t_regular.start()
        t_backfill.start()

        while True:
            time.sleep(5)
            if not t_regular.is_alive():
                logger.error("regular scrape thread exited unexpectedly")
                raise SystemExit(1)
            if not backfill_done.is_set() and not t_backfill.is_alive():
                logger.error("backfill thread exited unexpectedly")
                raise SystemExit(1)
