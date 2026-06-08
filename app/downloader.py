from __future__ import annotations

import contextlib
import dataclasses
import enum
import io
import json
import os.path
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString, cast

import jinja2
import psycopg
import qbittorrentapi
from psycopg.rows import dict_row
from rich.console import Console
from rtorrent_rpc import RTorrent
from sslog import logger

from app.bt_client import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentNotFoundError,
    TorrentState,
)
from app.config import DownloaderConfig
from app.const import (
    BT_TAG_DOWNLOADING,
    BT_TAG_FILE_SELECTED,
    BT_TAG_PROCESS_ERROR,
    BT_TAG_PROCESSING,
    BT_TAG_SELECTING_FILES,
    PRIORITY_CATEGORY,
    SELECTED_CATEGORY,
    TZ_SHANGHAI,
    ItemStatus,
    pick_order_clause,
)
from app.db import Connection, Database
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_mediainfo_from_file
from app.mt import MTeamDomain
from app.qb_client import QBittorrentClient
from app.rpc import (
    RPC_DELETE_TORRENT,
    RPC_PING,
    DeleteTorrentPayload,
    PingPayload,
    process_commands,
)
from app.rt_client import RTorrentClient
from app.torrent import find_largest_video_file
from app.torrent_store import TorrentStore
from app.utils import human_readable_size, must_find_executable, set_torrent_comment


def format_exc(e: Exception) -> str:
    f = io.StringIO()
    with f:
        f.write(f"{e}\n")
        Console(legacy_windows=True, width=1000, file=f, no_color=True).print_exception()
        return f.getvalue()


class Status(enum.IntEnum):
    unknown = 0
    downloading = 1
    done = 3


@dataclasses.dataclass(frozen=True, slots=True)
class PickContext:
    picked: int = 0
    has_pending: bool = False
    no_space: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class LoopContext:
    min_eta: float = 300


def _pick_query(config: DownloaderConfig) -> LiteralString:
    # Inline the pending_download_threads view predicates instead of selecting from
    # the view, because FOR UPDATE cannot be used on views.  The query locks
    # candidate rows on the thread table directly so that concurrent downloaders
    # pick disjoint sets without a global advisory lock — PostgreSQL's row-level
    # SKIP LOCKED handles the coordination natively.  Columns are listed
    # explicitly to avoid transferring the large mediainfo/api_mediainfo text
    # columns over the wire.
    order_clause = pick_order_clause(config.pick_strategy, 3)

    seeder_clause: LiteralString = cast(LiteralString, config.seeder_condition)

    return f"""
    select
        thread.tid, thread.size, thread.info_hash, thread.seeders,
        thread.category, thread.deleted, thread.created_at, thread.upload_at,
        thread.api_mediainfo_at, thread.torrent_fetched_at, thread.selected_size,
        thread.torrent_invalid, thread.generated_mediainfo_at, thread.exported_at,
        thread.selected_index
    from thread
    where
        thread.deleted = false
        and thread.seeders != 0
        and thread.mediainfo = ''
        and thread.api_mediainfo = ''
        and thread.info_hash != ''
        and thread.selected_size > 0
        and thread.selected_size < $1
        and thread.category = any ($2)
        and thread.selected_index is not null
        and array_length(thread.selected_index, 1) > 0
        and ({seeder_clause})
        and not exists (select 1 from job where job.tid = thread.tid)
    {order_clause}
    limit 100
    for update of thread skip locked
    """


@dataclasses.dataclass(kw_only=True, frozen=True)
class Downloader:
    db: Database
    config: DownloaderConfig
    client: BTClient
    store: TorrentStore
    mediainfo_bin: str = dataclasses.field(
        default_factory=lambda: must_find_executable("mediainfo")
    )
    ffprobe_bin: str = dataclasses.field(default_factory=lambda: must_find_executable("ffprobe"))
    ffmpeg_bin: str = dataclasses.field(default_factory=lambda: must_find_executable("ffmpeg"))
    thread_filter_template: jinja2.Template | None = None

    @classmethod
    def new(cls, cfg: DownloaderConfig) -> Downloader:
        logger.info("initializing downloader {}, node_id={}", cfg.version, cfg.node_id)
        logger.info("connecting to database...")
        db = Database(cfg.pg_dsn())
        logger.info("database pool created")
        if cfg.rt_url:
            client: BTClient = RTorrentClient(RTorrent(cfg.rt_url))
        elif cfg.qb_url:
            client = QBittorrentClient(
                qbittorrentapi.Client(
                    host=str(cfg.qb_url),
                    password=cfg.qb_url.password,
                    username=cfg.qb_url.username,
                    SIMPLE_RESPONSES=True,
                    FORCE_SCHEME_FROM_HOST=True,
                    VERBOSE_RESPONSE_LOGGING=False,
                    RAISE_NOTIMPLEMENTEDERROR_FOR_UNIMPLEMENTED_API_ENDPOINTS=True,
                    REQUESTS_ARGS={"timeout": 10},
                ),
            )
        else:
            raise ValueError("no download client configured: set RT_URL or QB_URL")

        logger.info("download client created, type={}", type(client).__name__)

        thread_filter_template: jinja2.Template | None = None
        if cfg.thread_filter:
            thread_filter_template = jinja2.Environment().from_string(cfg.thread_filter)

        store = TorrentStore(cfg)
        logger.info("torrent store created")

        return Downloader(
            config=cfg,
            db=db,
            client=client,
            store=store,
            thread_filter_template=thread_filter_template,
        )

    def __post_init__(self) -> None:
        logger.info("testing database connection...")
        try:
            self.db.fetch_val("select version()")
        except Exception:
            logger.exception("failed to connect to database")
            sys.exit(1)

        logger.info("successfully connect to database")

        logger.info("waiting for database migrations...")
        self.db.wait_db_migration()

        logger.info("connecting to download client...")
        version = self.client.app_version()
        logger.info("successfully connect to download client {}", version)

        logger.info("using mediainfo at {}", self.mediainfo_bin)
        logger.info("using ffprobe at {}", self.ffprobe_bin)
        logger.info("using ffmpeg at {}", self.ffmpeg_bin)

    def start(self) -> None:
        logger.info("downloader started, entering main loop")
        interval = 1
        while True:
            self.__heart_beat()
            self.__wait_for_notify(interval)
            interval = 60 * 5
            try:
                self.__process_commands()
            except Exception:
                logger.exception("failed to process commands")

            try:
                loop_ctx = self.__run_at_interval()
            except Exception:
                logger.exception("failed to run")
                loop_ctx = LoopContext()

            interval = max(60, min(int(loop_ctx.min_eta), 300))

            logger.info("loop done, sleeping for {}s", interval)

    def __wait_for_notify(self, timeout: float) -> None:
        """Wait for a PG notification or until timeout expires."""
        try:
            channel = f"node_rpc_{self.config.node_id}"
            with psycopg.connect(self.config.pg_dsn(), autocommit=True) as conn:
                conn.execute(f'LISTEN "{channel}"')  # type: ignore
                for _ in conn.notifies(timeout=timeout, stop_after=1):
                    pass
        except Exception:
            time.sleep(timeout)

    def __process_commands(self) -> None:
        """Poll and execute pending RPC commands for this downloader."""
        process_commands(
            self.db,
            self.config.node_id,
            {
                RPC_DELETE_TORRENT: self.__handle_cmd_delete_torrent,
                RPC_PING: self.__handle_cmd_ping,
            },
        )

    @staticmethod
    def __handle_cmd_ping(_payload: PingPayload) -> dict[str, str]:
        return {"pong": "ok"}

    def _delete_torrent_files(self, info_hash: str) -> None:
        save_path = Path(self.config.download_path) / info_hash.lower()
        shutil.rmtree(save_path, ignore_errors=True)

    def __handle_cmd_delete_torrent(self, payload: DeleteTorrentPayload) -> dict[str, str]:
        self.client.torrents_delete(torrent_hashes=payload.info_hash, delete_files=True)
        self._delete_torrent_files(payload.info_hash)
        self.__update_job_status(
            status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
            info_hash=payload.info_hash,
            removed_reason="rpc",
        )
        return {"info_hash": payload.info_hash}

    def __run_at_interval(self) -> LoopContext:
        completed, min_eta = self.__process_torrents()
        ctx = self.__pick_and_add_jobs()
        if not completed and ctx.picked == 0 and ctx.no_space and ctx.has_pending:
            self.__maybe_evict_slowest()
        return LoopContext(min_eta=min_eta)

    def __heart_beat(self) -> None:
        debug_info = self.client.get_node_debug_info()
        self.db.execute(
            """
            insert into node (id, last_seen, version, debug_info) values ($1, $2, $3, $4)
            on conflict (id) do update set last_seen = excluded.last_seen, version = excluded.version, debug_info = excluded.debug_info
            """,
            [
                self.config.node_id,
                datetime.now(tz=TZ_SHANGHAI),
                self.config.version,
                json.dumps(debug_info, indent=2, ensure_ascii=False),
            ],
        )

    def __set_tags(self, info_hash: str, *, remove: str, add: str) -> None:
        """Swap informational tags on a torrent."""
        self.client.torrents_remove_tags(tags=[remove], torrent_hashes=info_hash)
        self.client.torrents_add_tags(tags=[add], torrent_hashes=info_hash)

    @staticmethod
    def __update_job_on_conn(
        conn: Connection,
        node_id: str,
        *,
        status: str,
        info_hash: str = "",
        tid: int = 0,
        failed_reason: str = "",
        removed_reason: str = "",
    ) -> None:
        if info_hash:
            conn.execute(
                """update job set status = $1, failed_reason = $2, removed_reason = $3, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where info_hash = $4 and node_id = $5""",
                [status, failed_reason, removed_reason, info_hash, node_id],
            )
        else:
            conn.execute(
                """update job set status = $1, failed_reason = $2, removed_reason = $3, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where tid = $4 and node_id = $5""",
                [status, failed_reason, removed_reason, tid, node_id],
            )
            conn.execute(
                "delete from job_download_size where info_hash = (select job.info_hash from job where job.tid = $1 and job.node_id = $2) and node_id = $2",
                [tid, node_id],
            )

    def __update_job_status(
        self,
        *,
        status: str,
        info_hash: str = "",
        tid: int = 0,
        failed_reason: str = "",
        removed_reason: str = "",
    ) -> None:
        """Update job status. Identify job by info_hash or tid (plus node_id)."""
        with self.db.connection() as conn, conn.transaction():
            self.__update_job_on_conn(
                conn,
                self.config.node_id,
                status=status,
                info_hash=info_hash,
                tid=tid,
                failed_reason=failed_reason,
                removed_reason=removed_reason,
            )

    def __process_torrents(self) -> tuple[bool, float]:
        """Process all torrents in the download client in a single pass.

        Returns (completed, min_eta) where min_eta is the smallest ETA
        among downloading torrents (seconds), or 300 if none.
        """
        t0 = time.monotonic()
        logger.info("__process_torrents")
        torrents = self.client.torrents_info()
        t1 = time.monotonic()
        logger.info("torrents_info: {} torrents in {:.1f}s", len(torrents), t1 - t0)
        now = datetime.now(tz=TZ_SHANGHAI)
        completed = False
        if not torrents:
            logger.info("client has no torrents")
            return False, 300
        # Mark jobs as removed-from-client if their torrent is no longer in client
        torrent_hashes = [x.hash for x in torrents]
        self.db.execute(
            """
                update job set
                  status = $1,
                  removed_reason = $6,
                  updated_at = $5
                where (not info_hash = any($2)) and node_id = $3 and status = $4
                """,
            [
                ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                torrent_hashes,
                self.config.node_id,
                ItemStatus.DOWNLOADING,
                now,
                "manual",
            ],
        )

        # Fetch all downloading jobs for this node, and which ones have unselected category
        job_rows: list[tuple[str, bool]] = self.db.fetch_all(
            """
            select job.info_hash,
                   not (thread.category = any($3)) as unselected
            from job
            join thread on (thread.tid = job.tid)
            where job.node_id = $1 and job.status = $2
            """,
            [self.config.node_id, ItemStatus.DOWNLOADING, SELECTED_CATEGORY],
        )
        managed_hashes: set[str] = {info_hash for info_hash, _ in job_rows}
        unselected_hashes: set[str] = {
            info_hash for info_hash, unselected in job_rows if unselected
        }

        stale_cutoff = now - timedelta(days=2)

        stalled_rows = self.db.fetch_all(
            """
            select info_hash
            from job_download_size
            where node_id = $1
              and info_hash = any($2)
            group by info_hash
            having max(recorded_at) < $3
            """,
            [self.config.node_id, list(managed_hashes), stale_cutoff],
        )
        stalled_hashes: set[str] = {r[0] for r in stalled_rows}
        t2 = time.monotonic()
        logger.info(
            "db queries done in {:.1f}s, managed={} unselected={} stalled={}",
            t2 - t1,
            len(managed_hashes),
            len(unselected_hashes),
            len(stalled_hashes),
        )

        counts: dict[str, int] = {}
        downloading_updates: list[Torrent] = []
        min_eta = 300.0
        for t in torrents:
            # Torrent not in managed (downloading) jobs — check if it has a job at all
            if t.hash not in managed_hashes:
                self.__handle_unmanaged_torrent(t)
                counts["unmanaged"] = counts.get("unmanaged", 0) + 1
                continue

            # Cleanup stalled torrents (no download size recorded for 3+ days)
            if t.hash in stalled_hashes:
                logger.info("cleanup stalled torrent {}", t.name)
                self.__update_job_status(
                    status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                    info_hash=t.hash,
                    removed_reason="stalled",
                )
                self.db.execute(
                    "update thread set torrent_invalid = 'stalled' where info_hash = $1",
                    [t.hash],
                )
                self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                self._delete_torrent_files(t.hash)
                counts["stalled"] = counts.get("stalled", 0) + 1
                continue

            # Torrent in error state → mark failed and delete
            if t.state == TorrentState.ERRORED:
                logger.info("torrent {} in error state", t.name)
                self.__update_job_status(
                    status=ItemStatus.FAILED,
                    info_hash=t.hash,
                    failed_reason=t.error_message or "torrent error",
                )
                self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                self._delete_torrent_files(t.hash)
                counts["errored"] = counts.get("errored", 0) + 1
                continue

            # Skip torrents that failed processing
            if BT_TAG_PROCESS_ERROR in t.tags:
                counts["process_error"] = counts.get("process_error", 0) + 1
                continue

            # Cleanup torrents whose category is no longer selected
            if t.hash in unselected_hashes:
                logger.info("cleanup unselected category torrent {}", t.hash)
                self.__update_job_status(
                    status=ItemStatus.SKIPPED,
                    info_hash=t.hash,
                    failed_reason="category no longer selected",
                )
                self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                self._delete_torrent_files(t.hash)
                counts["unselected"] = counts.get("unselected", 0) + 1
                continue  # Upload complete → process mediainfo
            if t.state == TorrentState.UPLOADING:
                completed = True
                self.__set_tags(t.hash, remove=BT_TAG_DOWNLOADING, add=BT_TAG_PROCESSING)
                try:
                    self.__process_local_torrent(t)
                except Exception as e:
                    self.__update_job_status(
                        status=ItemStatus.FAILED,
                        info_hash=t.hash,
                        failed_reason=format_exc(e),
                    )
                    self.client.torrents_add_tags(
                        tags=[BT_TAG_PROCESS_ERROR], torrent_hashes=t.hash
                    )
                    logger.error("failed to process local torrent {}", e)
                counts["uploading"] = counts.get("uploading", 0) + 1
                continue

            # File not yet selected → select files, clear limit
            if BT_TAG_FILE_SELECTED not in t.tags:
                self.__fix_file_selection(t)
                self.client.torrents_set_download_limit(limit=0, torrent_hashes=t.hash)
                self.client.torrents_add_tags(tags=[BT_TAG_FILE_SELECTED], torrent_hashes=t.hash)
                if t.state == TorrentState.PAUSED:
                    self.client.torrents_resume(torrent_hashes=t.hash)
                counts["need_select"] = counts.get("need_select", 0) + 1
                continue

            # Paused → resume
            if t.state == TorrentState.PAUSED:
                logger.info("resuming stopped torrent {} (tags={})", t.name, t.tags)
                self.__set_tags(t.hash, remove=BT_TAG_SELECTING_FILES, add=BT_TAG_DOWNLOADING)
                self.client.torrents_resume(torrent_hashes=t.hash)
                counts["paused"] = counts.get("paused", 0) + 1
                continue

            # Downloading — update progress
            counts["downloading"] = counts.get("downloading", 0) + 1
            downloading_updates.append(t)
            if 0 < t.eta < ETA_INF:
                min_eta = min(min_eta, t.eta)
            continue

        # Batch update all downloading torrents in one transaction
        if downloading_updates:
            t_db = time.monotonic()
            hashes = [t.hash for t in downloading_updates]
            progresses = [t.progress for t in downloading_updates]
            dlspeeds = [t.dlspeed for t in downloading_updates]
            etas = [t.eta for t in downloading_updates]
            errors = [t.error_message for t in downloading_updates]
            completeds = [t.completed for t in downloading_updates]

            def _debug_info(t: Torrent) -> str:
                try:
                    data = self.client.get_torrent_debug_info(t.hash)
                    return json.dumps(data, indent=2, ensure_ascii=False)
                except Exception as e:
                    return format_exc(e)

            debug_infos = [_debug_info(t) for t in downloading_updates]
            with self.db.connection() as conn, conn.transaction():
                conn.execute(
                    """
                    update job set
                      progress = data.progress,
                      dlspeed = data.dlspeed,
                      eta = data.eta,
                      error_message = data.error_message,
                      debug_info = data.debug_info,
                      updated_at = $1
                    from unnest($2::text[], $3::float[], $4::int[], $5::int[], $6::text[], $9::text[])
                      as data(info_hash, progress, dlspeed, eta, error_message, debug_info)
                    where job.info_hash = data.info_hash
                      and job.node_id = $7
                      and job.status = $8
                    """,
                    [
                        now,
                        hashes,
                        progresses,
                        dlspeeds,
                        etas,
                        errors,
                        self.config.node_id,
                        ItemStatus.DOWNLOADING,
                        debug_infos,
                    ],
                )
                conn.execute(
                    """
                    insert into job_download_size (info_hash, node_id, size)
                    select data.info_hash, $1, data.size
                    from unnest($2::text[], $3::int8[]) as data(info_hash, size)
                    where (
                        select jds.size from job_download_size jds
                        where jds.info_hash = data.info_hash and jds.node_id = $1
                        order by jds.recorded_at desc limit 1
                    ) is distinct from data.size
                    """,
                    [self.config.node_id, hashes, completeds],
                )
            logger.info(
                "batch update {} downloading torrents in {:.1f}s",
                len(downloading_updates),
                time.monotonic() - t_db,
            )
        t3 = time.monotonic()
        logger.info(
            "process_torrents done in {:.1f}s (loop={:.1f}s) counts={} min_eta={:.0f}s",
            t3 - t0,
            t3 - t2,
            counts,
            min_eta,
        )
        return completed, min_eta

    def __handle_unmanaged_torrent(self, t: Torrent) -> None:
        """Handle a torrent in the download client that has no active downloading job.

        Try to reclaim if a job exists with removed-by-client status
        (e.g. was prematurely marked due to async add), otherwise delete it.
        """
        with self.db.connection() as conn, conn.transaction():
            restored = conn.fetch_val(
                """
                update job set status = $1, failed_reason = '', updated_at = current_timestamp
                where info_hash = $2 and node_id = $3 and status = $4
                returning info_hash
                """,
                [
                    ItemStatus.DOWNLOADING,
                    t.hash,
                    self.config.node_id,
                    ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                ],
            )
        if restored:
            logger.info("reclaimed job for torrent {}", t.hash)
            return

        logger.info("{} not managed, deleting from client", t.hash)
        self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
        self._delete_torrent_files(t.hash)

    def __fix_file_selection(self, t: Torrent) -> None:
        """Fix file priorities for torrents that are downloading all files."""
        files = self.client.torrents_files(torrent_hash=t.hash)
        if len(files) <= 1:
            return
        files_data = [(f.index, f.name, f.size) for f in files]
        keep_idx = find_largest_video_file(files_data)
        if keep_idx is None:
            return
        file_ids = [f.index for f in files if f.index != keep_idx and f.priority != 0]
        if file_ids:
            logger.info("fixing file selection for torrent {}", t.name)
        self.client.torrents_file_priority(
            torrent_hash=t.hash,
            file_ids=file_ids,
            priority=0,
        )
        try:
            data = self.client.get_torrent_debug_info(t.hash)
            raw = json.dumps(data, indent=2, ensure_ascii=False)
        except Exception as e:
            raw = format_exc(e)
        self.db.execute(
            "update job set debug_info = $1 where info_hash = $2 and node_id = $3 and status = $4",
            [raw, t.hash, self.config.node_id, ItemStatus.DOWNLOADING],
        )

    def __process_local_torrent(self, t: Torrent) -> None:
        files = self.client.torrents_files(torrent_hash=t.hash)
        active_files = [(f.index, f.name, f.size) for f in files if f.priority != 0]
        selected_idx = find_largest_video_file(active_files)

        if selected_idx is None:
            self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
            self._delete_torrent_files(t.hash)
            return

        selected_file = next(f for f in files if f.index == selected_idx)
        path = Path(t.save_path, selected_file.name)

        media_info = extract_mediainfo_from_file(self.mediainfo_bin, path)

        hard_code_subtitle = check_hardcode_chinese_subtitle(
            self.ffprobe_bin, self.ffmpeg_bin, path
        )

        with (
            self.db.connection() as conn,
            conn.transaction(),
        ):
            conn.execute(
                """
                    update thread set mediainfo = $1, generated_mediainfo_at = current_timestamp, hard_coded_subtitle = $2 where info_hash = $3
                    """,
                [
                    media_info.replace("\x00", ""),
                    hard_code_subtitle,
                    t.hash,
                ],
            )
            conn.execute(
                """update job set status = $1, failed_reason = '', updated_at = current_timestamp,
                   completed_at = current_timestamp
                   where info_hash = $2 and node_id = $3""",
                [ItemStatus.DONE, t.hash, self.config.node_id],
            )
            conn.execute(
                "delete from job_download_size where info_hash = $1 and node_id = $2",
                [t.hash, self.config.node_id],
            )
        self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
        self._delete_torrent_files(t.hash)

    def __pick_and_add_jobs(self) -> PickContext:
        logger.info("__pick_and_add_jobs")

        torrents = self.client.torrents_info()
        current_total_size = sum(t.size for t in torrents)
        downloading_count = sum(1 for t in torrents if t.state == TorrentState.DOWNLOADING)

        max_count = self.config.max_downloading_count
        if max_count > 0 and downloading_count >= max_count:
            logger.info(
                "at downloading count limit: downloading={} limit={}",
                downloading_count,
                max_count,
            )
            return PickContext(no_space=True)

        left_size = int(self.config.total_process_size) - current_total_size

        if left_size <= 0:
            logger.info(
                "no space left: current={} total_limit={} left={}",
                human_readable_size(current_total_size),
                human_readable_size(self.config.total_process_size),
                human_readable_size(left_size),
            )
            return PickContext(no_space=True)

        picked: list[tuple[int, str]] = []
        has_pending = False
        no_space = False

        with (
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
            params: list[Any] = [
                self.config.single_torrent_size_limit,
                SELECTED_CATEGORY,
                PRIORITY_CATEGORY,
            ]

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(_pick_query(self.config), params)
                rows: list[dict[str, Any]] = cur.fetchall()

            logger.info("fetch {} rows", len(rows))
            if not rows:
                logger.info("skip pick: no pending download threads")
                return PickContext()

            if self.thread_filter_template is not None:
                before_count = len(rows)
                rows = [
                    row
                    for row in rows
                    if self.thread_filter_template.render(thread=row).strip() == "true"
                ]
                if not rows:
                    logger.info(
                        "skip pick: thread filter rejected all {} rows",
                        before_count,
                    )
                    return PickContext()

            has_pending = True

            for row in rows:
                tid = row["tid"]
                info_hash = row["info_hash"]
                selected_size = row["selected_size"]
                if left_size - selected_size <= 0:
                    logger.info(
                        "skip tid={} selected_size={} left_size={}: too large",
                        tid,
                        human_readable_size(selected_size),
                        human_readable_size(left_size),
                    )
                    no_space = True
                    break

                conn.execute(
                    """
                    insert into job (tid, node_id, info_hash, start_download_time, updated_at, status, eta)
                    VALUES ($1, $2, $3, current_timestamp, current_timestamp, $4, $5)
                    """,
                    [
                        tid,
                        self.config.node_id,
                        info_hash,
                        ItemStatus.DOWNLOADING,
                        ETA_INF,
                    ],
                )
                conn.execute(
                    """
                    insert into job_download_size (info_hash, node_id, size)
                    values ($1, $2, 0)
                    """,
                    [info_hash, self.config.node_id],
                )
                left_size -= selected_size
                picked.append((tid, info_hash))

        if not picked:
            logger.info("pick 0 items: left_size={}", human_readable_size(left_size))
        else:
            logger.info("pick {} items", len(picked))

        # add to download client outside the lock to avoid blocking other nodes
        for tid, info_hash in picked:
            try:
                self.__add_torrent(tid, info_hash)
            except TimeoutError:
                logger.warning("timeout adding torrent tid={}, will retry", tid)
                with contextlib.suppress(TorrentNotFoundError):
                    self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                self.db.execute(
                    "delete from job where tid = $1 and node_id = $2",
                    [tid, self.config.node_id],
                )
            except Exception as e:
                logger.exception("failed to add torrent tid={}: {}", tid, e)
                self.__update_job_status(
                    status=ItemStatus.FAILED, tid=tid, failed_reason=format_exc(e)
                )
                with contextlib.suppress(TorrentNotFoundError):
                    self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                self._delete_torrent_files(info_hash)
        return PickContext(picked=len(picked), has_pending=has_pending, no_space=no_space)

    def __maybe_evict_slowest(self) -> None:
        torrents = self.client.torrents_info()
        if not torrents:
            return

        downloading = [
            t
            for t in torrents
            if t.state not in (TorrentState.PAUSED, TorrentState.UPLOADING, TorrentState.ERRORED)
        ]
        if not downloading:
            return

        limit = int(self.config.min_download_speed)
        if sum(t.dlspeed for t in downloading) >= limit:
            return

        now = datetime.now(tz=TZ_SHANGHAI)
        cutoff = now - timedelta(hours=24)
        row = self.db.fetch_one(
            """
            with stats as (
                select
                    info_hash,
                    (max(size) - min(size))::float
                        / greatest(extract(epoch from max(recorded_at) - min(recorded_at)), 1)
                        as avg_speed
                from job_download_size
                where node_id = $1
                  and recorded_at > $2
                group by info_hash
                having count(*) >= 2
            )
            select s.info_hash, s.avg_speed
            from stats s
            join job j on j.info_hash = s.info_hash and j.node_id = $1
            where j.start_download_time < $2
            order by s.avg_speed
            limit 1
            """,
            [self.config.node_id, cutoff],
        )
        if row is None:
            return

        info_hash, avg_speed = row
        logger.info(
            "evicting slowest torrent {} (avg_speed={:.0f} B/s, limit={} B/s)",
            info_hash,
            avg_speed,
            limit,
        )
        self.__update_job_status(
            status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
            info_hash=info_hash,
            removed_reason="slow_download",
        )
        self.db.execute(
            "update thread set torrent_invalid = 'stalled' where info_hash = $1",
            [info_hash],
        )
        self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)

    def __add_torrent(
        self,
        tid: int,
        info_hash: str,
    ) -> None:
        tc = self.store.read(tid)
        if not tc:
            self.__update_job_status(
                status=ItemStatus.FAILED,
                tid=tid,
                failed_reason="torrent content not found",
            )
            return

        tc = set_torrent_comment(tc, f"https://{MTeamDomain}/detail/{tid}")
        logger.info("added torrent tid={} info_hash={}", tid, info_hash)

        r = self.client.torrents_add(
            torrent_files=[tc],
            save_path=os.path.join(self.config.download_path, info_hash),
            use_auto_torrent_management=False,
            tags=[BT_TAG_DOWNLOADING],
            download_limit=1,
            is_sequential_download=True,
        )
        if r != "Ok.":
            self.__update_job_status(
                status=ItemStatus.FAILED, tid=tid, failed_reason="failed to add"
            )
            with contextlib.suppress(TorrentNotFoundError):
                self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
            self._delete_torrent_files(info_hash)
            return
