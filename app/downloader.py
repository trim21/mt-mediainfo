from __future__ import annotations

import contextlib
import dataclasses
import enum
import io
import os.path
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, LiteralString, cast

import jinja2
import psycopg
from psycopg.rows import dict_row
from rich.console import Console
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
    LOCK_KEY_PICK_RSS_JOB,
    PRIORITY_CATEGORY,
    QB_TAG_DOWNLOADING,
    QB_TAG_NEED_SELECT,
    QB_TAG_PROCESS_ERROR,
    QB_TAG_PROCESSING,
    QB_TAG_SELECTING_FILES,
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
from app.utils import must_find_executable, set_torrent_comment


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


def _pick_query(config: DownloaderConfig) -> LiteralString:
    order_clause = pick_order_clause(config.pick_strategy, 3)

    seeder_clause: LiteralString = cast(LiteralString, config.seeder_condition)

    return f"""
    select pending_download_threads.* from pending_download_threads
    left join job on (job.tid = pending_download_threads.tid)
    where
        selected_size < $1 and
        category = any ($2) and
        job.tid is null and
        ({seeder_clause})
    {order_clause}
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
        db = Database(cfg.pg_dsn())
        if cfg.rt_url:
            from rtorrent_rpc import RTorrent

            client: BTClient = RTorrentClient(RTorrent(cfg.rt_url))
        elif cfg.qb_url:
            import qbittorrentapi

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

        thread_filter_template: jinja2.Template | None = None
        if cfg.thread_filter:
            thread_filter_template = jinja2.Environment().from_string(cfg.thread_filter)

        return Downloader(
            config=cfg,
            db=db,
            client=client,
            store=TorrentStore(cfg),
            thread_filter_template=thread_filter_template,
        )

    def __post_init__(self) -> None:
        try:
            self.db.fetch_val("select version()")
        except Exception:
            logger.exception("failed to connect to database")
            sys.exit(1)

        logger.info("successfully connect to database")

        self.db.wait_db_migration()

        version = self.client.app_version()
        logger.info("successfully connect to download client {}", version)

        logger.info("using mediainfo at {}", self.mediainfo_bin)
        logger.info("using ffprobe at {}", self.ffprobe_bin)
        logger.info("using ffmpeg at {}", self.ffmpeg_bin)

    def start(self) -> None:
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
                self.__run_at_interval()
            except Exception:
                logger.exception("failed to run")

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

    def __handle_cmd_delete_torrent(self, payload: DeleteTorrentPayload) -> dict[str, str]:
        self.client.torrents_delete(torrent_hashes=payload.info_hash, delete_files=True)
        self.__update_job_status(
            status=ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
            info_hash=payload.info_hash,
            removed_reason="rpc",
        )
        return {"info_hash": payload.info_hash}

    def __run_at_interval(self) -> None:
        completed = self.__process_qb_torrents()
        ctx = self.__pick_and_add_jobs()
        if not completed and ctx.picked == 0 and ctx.no_space and ctx.has_pending:
            self.__maybe_evict_slowest()

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen, version) values ($1, $2, $3)
            on conflict (id) do update set last_seen = excluded.last_seen, version = excluded.version
            """,
            [self.config.node_id, datetime.now(tz=TZ_SHANGHAI), self.config.version],
        )

    def __set_tags(self, info_hash: str, *, remove: str, add: str) -> None:
        """Swap informational tags on a torrent."""
        self.client.torrents_remove_tags(tags=remove, torrent_hashes=info_hash)
        self.client.torrents_add_tags(tags=add, torrent_hashes=info_hash)

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

    def __process_qb_torrents(self) -> bool:
        """Process all torrents in qBittorrent in a single pass.

        Returns True if any torrent completed (entered UPLOADING state).
        """
        logger.info("__process_qb_torrents")
        torrents = self.client.torrents_info()
        now = datetime.now(tz=TZ_SHANGHAI)
        completed = False
        if not torrents:
            logger.info("qb has no torrents")
            return False
        # Mark jobs as removed-from-client if their torrent is no longer in qb
        qb_hashes = [x.hash for x in torrents]
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
                qb_hashes,
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

        for t in torrents:
            # Torrent not in managed (downloading) jobs — check if it has a job at all
            if t.hash not in managed_hashes:
                self.__handle_unmanaged_torrent(t)
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
                continue

            # Torrent in error state → mark failed and delete
            if t.state == TorrentState.ERRORED:
                logger.info("torrent {} in error state", t.name)
                self.__update_job_status(
                    status=ItemStatus.FAILED,
                    info_hash=t.hash,
                    failed_reason="torrent error",
                )
                self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                continue

            # Skip torrents that failed processing
            if QB_TAG_PROCESS_ERROR in t.tags:
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
                continue

            # Upload complete → process mediainfo
            if t.state == TorrentState.UPLOADING:
                completed = True
                self.__set_tags(t.hash, remove=QB_TAG_DOWNLOADING, add=QB_TAG_PROCESSING)
                try:
                    self.__process_local_torrent(t)
                except Exception as e:
                    self.__update_job_status(
                        status=ItemStatus.FAILED,
                        info_hash=t.hash,
                        failed_reason=format_exc(e),
                    )
                    self.client.torrents_add_tags(tags=QB_TAG_PROCESS_ERROR, torrent_hashes=t.hash)
                    logger.error("failed to process local torrent {}", e)
                continue

            # Newly added torrent → select files, clear limit, remove tag
            if QB_TAG_NEED_SELECT in t.tags:
                self.__fix_file_selection(t)
                self.client.torrents_set_download_limit(limit=0, torrent_hashes=t.hash)
                self.client.torrents_remove_tags(tags=QB_TAG_NEED_SELECT, torrent_hashes=t.hash)
                continue

            # Paused → resume
            if t.state == TorrentState.PAUSED:
                logger.info("resuming stopped torrent {} (tags={})", t.name, t.tags)
                self.__set_tags(t.hash, remove=QB_TAG_SELECTING_FILES, add=QB_TAG_DOWNLOADING)
                self.client.torrents_resume(torrent_hashes=t.hash)
                continue

            # Downloading — update progress
            with self.db.connection() as conn, conn.transaction():
                conn.execute(
                    """
                    update job set
                      progress = $1,
                      dlspeed = $2,
                      eta = $3,
                      updated_at = $4
                    where info_hash = $5 and node_id = $6 and status = $7
                    """,
                    [
                        t.progress,
                        t.dlspeed,
                        t.eta,
                        now,
                        t.hash,
                        self.config.node_id,
                        ItemStatus.DOWNLOADING,
                    ],
                )
                conn.execute(
                    """
                    insert into job_download_size (info_hash, node_id, size)
                    select $1, $2, $3
                    where (
                        select size
                        from job_download_size
                        where info_hash = $1 and node_id = $2
                        order by recorded_at desc
                        limit 1
                    ) is distinct from $3
                    """,
                    [t.hash, self.config.node_id, t.completed],
                )
        return completed

    def __handle_unmanaged_torrent(self, t: Torrent) -> None:
        """Handle a torrent in qB that has no active downloading job.

        Try to reclaim if a job exists with removed-by-client status
        (e.g. was prematurely marked due to async qB add), otherwise delete it.
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

        logger.info("{} not managed, deleting from qb", t.hash)
        self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)

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

    def __process_local_torrent(self, t: Torrent) -> None:
        files = self.client.torrents_files(torrent_hash=t.hash)
        active_files = [(f.index, f.name, f.size) for f in files if f.priority != 0]
        selected_idx = find_largest_video_file(active_files)

        if selected_idx is None:
            self.client.torrents_delete(torrent_hashes=t.hash, delete_files=True)
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

    def __pick_and_add_jobs(self) -> PickContext:
        logger.info("__pick_and_add_jobs")

        current_total_size = sum(t.size for t in self.client.torrents_info())
        left_size = int(self.config.total_process_size) - current_total_size

        picked: list[tuple[int, str]] = []
        has_pending = False
        no_space = False

        logger.info("pick lock")
        with (
            self.db.lock(LOCK_KEY_PICK_RSS_JOB),
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
            logger.info("get lock")
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
                return PickContext()

            if self.thread_filter_template is not None:
                rows = [
                    row
                    for row in rows
                    if self.thread_filter_template.render(thread=row).strip() == "true"
                ]

            if not rows:
                return PickContext()

            has_pending = True

            for row in rows:
                tid = row["tid"]
                info_hash = row["info_hash"]
                selected_size = row["selected_size"]
                if left_size - selected_size <= 0:
                    no_space = True
                    break

                conn.execute(
                    """
                    insert into job (tid, node_id, info_hash, start_download_time, updated_at, status, eta)
                    VALUES ($1, $2, $3, current_timestamp, current_timestamp, $4, $5)
                    """,
                    [tid, self.config.node_id, info_hash, ItemStatus.DOWNLOADING, ETA_INF],
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

        logger.info("pick {} items", len(picked))

        # add to download client outside the lock to avoid blocking other nodes
        for tid, info_hash in picked:
            try:
                self.__add_torrent(tid, info_hash)
            except Exception as e:
                logger.error("failed to add torrent tid={}: {}", tid, e)
                self.__update_job_status(
                    status=ItemStatus.FAILED, tid=tid, failed_reason=format_exc(e)
                )
                with contextlib.suppress(TorrentNotFoundError):
                    self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
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

        r = self.client.torrents_add(
            torrent_files=[tc],
            save_path=os.path.join(self.config.download_path, info_hash),
            use_auto_torrent_management=False,
            tags=[QB_TAG_DOWNLOADING, QB_TAG_NEED_SELECT],
            download_limit=1,
            is_sequential_download=True,
        )
        if r != "Ok.":
            self.__update_job_status(
                status=ItemStatus.FAILED, tid=tid, failed_reason="failed to add"
            )
            with contextlib.suppress(TorrentNotFoundError):
                self.client.torrents_delete(torrent_hashes=info_hash, delete_files=True)
