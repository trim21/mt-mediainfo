from __future__ import annotations

import dataclasses
import enum
import io
import os.path
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console
from sslog import logger

from app.config import Config
from app.const import (
    ITEM_STATUS_DONE,
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
    ITEM_STATUS_SKIPPED,
    LOCK_KEY_PICK_RSS_JOB,
    PRIORITY_CATEGORY,
    QB_TAG_DOWNLOADING,
    QB_TAG_NEED_SELECT,
    QB_TAG_PROCESS_ERROR,
    QB_TAG_PROCESSING,
    QB_TAG_SELECTING_FILES,
    SELECTED_CATEGORY,
)
from app.db import Database
from app.download_client import ClientTorrent, DownloadClient, TorrentState
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_mediainfo_from_file
from app.mt import MTeamDomain
from app.qb import QBittorrentClient
from app.rpc import (
    RPC_DELETE_TORRENT,
    RPC_PING,
    DeleteTorrentPayload,
    PingPayload,
    process_commands,
)
from app.rt import RTorrentClient
from app.torrent import find_largest_video_file
from app.torrent_store import TorrentStore
from app.utils import set_torrent_comment


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


def _create_download_client(cfg: Config) -> DownloadClient:
    if cfg.qb_url:
        return QBittorrentClient(cfg.qb_url)
    if cfg.rt_url:
        return RTorrentClient(cfg.rt_url)
    raise ValueError("no download client configured: set QB_URL or RT_URL")


@dataclasses.dataclass(kw_only=True, frozen=True)
class Node:
    db: Database
    config: Config
    dl: DownloadClient
    store: TorrentStore

    @classmethod
    def new(cls, cfg: Config) -> Node:
        db = Database(cfg.pg_dsn())
        return Node(
            config=cfg,
            db=db,
            dl=_create_download_client(cfg),
            store=TorrentStore(cfg, db),
        )

    def __post_init__(self) -> None:
        try:
            self.db.fetch_val("select version()")
        except Exception:
            logger.exception("failed to connect to database")
            sys.exit(1)

        logger.info("successfully connect to database")

        version = self.dl.connect()
        logger.info("successfully connect to download client {}", version)

    def start(self) -> None:
        interval = 1
        while True:
            self.__heart_beat()
            time.sleep(interval)
            interval = 60
            self.dl.tick()
            try:
                self.__process_commands()
            except Exception as e:
                print("failed to process commands", format_exc(e))
            try:
                self.__run_at_interval()
            except Exception as e:
                print("failed to run", format_exc(e))

    def __process_commands(self) -> None:
        """Poll and execute pending RPC commands for this node."""
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
        self.dl.delete_torrent(payload.info_hash, delete_files=True)
        self.__update_job_status(
            status=ITEM_STATUS_FAILED,
            info_hash=payload.info_hash,
            failed_reason="deleted by user",
        )
        return {"info_hash": payload.info_hash}

    def __run_at_interval(self) -> None:
        self.__process_qb_torrents()
        self.__pick_and_add_jobs()

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen) values ($1, $2)
            on conflict (id) do update set last_seen = excluded.last_seen
            """,
            [self.config.node_id, datetime.now(tz=UTC)],
        )

    def __set_tags(self, info_hash: str, *, remove: str, add: str) -> None:
        """Swap informational tags on a torrent."""
        self.dl.remove_tags(info_hash, [remove])
        self.dl.add_tags(info_hash, [add])

    def __update_job_status(
        self,
        *,
        status: str,
        info_hash: str = "",
        tid: int = 0,
        failed_reason: str = "",
    ) -> None:
        """Update job status. Identify job by info_hash or tid (plus node_id)."""
        if info_hash:
            self.db.execute(
                """update job set status = $1, failed_reason = $2, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where info_hash = $3 and node_id = $4""",
                [status, failed_reason, info_hash, self.config.node_id],
            )
        else:
            self.db.execute(
                """update job set status = $1, failed_reason = $2, updated_at = current_timestamp,
                   completed_at = case when $1 = 'done' then current_timestamp else completed_at end
                   where tid = $3 and node_id = $4""",
                [status, failed_reason, tid, self.config.node_id],
            )

    def __process_qb_torrents(self) -> None:
        """Process all torrents in the download client in a single pass."""
        logger.info("__process_qb_torrents")
        torrents = self.dl.list_torrents()
        now = datetime.now(tz=UTC)
        if not torrents:
            logger.info("downloader has no torrents")
            return

        logger.info("load {} torrents from downloader", len(torrents))
        # Mark jobs as removed-from-client if their torrent is no longer in qb
        qb_hashes = [x.hash for x in torrents]
        self.db.execute(
            """
                update job set
                  status = $1,
                  updated_at = $5
                where (not info_hash = any($2)) and node_id = $3 and status = $4
                """,
            [
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                qb_hashes,
                self.config.node_id,
                ITEM_STATUS_DOWNLOADING,
                now,
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
            [self.config.node_id, ITEM_STATUS_DOWNLOADING, SELECTED_CATEGORY],
        )
        managed_hashes: set[str] = {info_hash for info_hash, _ in job_rows}
        unselected_hashes: set[str] = {
            info_hash for info_hash, unselected in job_rows if unselected
        }

        cutoff = time.time() - 10 * 86400

        for t in torrents:
            # Torrent not in managed (downloading) jobs — check if it has a job at all
            if t.hash not in managed_hashes:
                self.__handle_unmanaged_torrent(t)
                continue

            # Cleanup old torrents (no seeders for 10+ days)
            if 0 < t.seen_complete < cutoff:
                logger.info(
                    "cleanup old torrent {} (last seen complete: {})",
                    t.name,
                    t.seen_complete,
                )
                self.__update_job_status(
                    status=ITEM_STATUS_FAILED,
                    info_hash=t.hash,
                    failed_reason="no seeders",
                )
                self.dl.delete_torrent(t.hash)
                continue

            # Torrent in error state → mark failed and delete
            if t.state is TorrentState.error:
                logger.info("torrent {} in error state: {}", t.name, t.message)
                self.__update_job_status(
                    status=ITEM_STATUS_FAILED,
                    info_hash=t.hash,
                    failed_reason=t.message or "torrent error",
                )
                self.dl.delete_torrent(t.hash)
                continue

            # Skip torrents that failed processing
            if QB_TAG_PROCESS_ERROR in t.tags:
                continue

            # Cleanup torrents whose category is no longer selected
            if t.hash in unselected_hashes:
                logger.info("cleanup unselected category torrent {}", t.hash)
                self.__update_job_status(
                    status=ITEM_STATUS_SKIPPED,
                    info_hash=t.hash,
                    failed_reason="category no longer selected",
                )
                self.dl.delete_torrent(t.hash)
                continue

            # Upload complete → process mediainfo
            if t.state is TorrentState.seeding:
                self.__set_tags(t.hash, remove=QB_TAG_DOWNLOADING, add=QB_TAG_PROCESSING)
                try:
                    self.__process_local_torrent(t)
                except Exception as e:
                    self.__update_job_status(
                        status=ITEM_STATUS_FAILED,
                        info_hash=t.hash,
                        failed_reason=format_exc(e),
                    )
                    self.dl.add_tags(t.hash, [QB_TAG_PROCESS_ERROR])
                    logger.error("failed to process local torrent {}", e)
                continue

            # Newly added torrent → select files, clear limit, remove tag
            if QB_TAG_NEED_SELECT in t.tags:
                self.__fix_file_selection(t)
                self.dl.set_download_limit(t.hash, 0)
                self.dl.remove_tags(t.hash, [QB_TAG_NEED_SELECT])
                continue

            # Paused → resume
            if t.state is TorrentState.paused:
                logger.info("resuming stopped torrent {} (tags={})", t.name, t.tags)
                self.__set_tags(t.hash, remove=QB_TAG_SELECTING_FILES, add=QB_TAG_DOWNLOADING)
                self.dl.resume_torrent(t.hash)
                continue

            # Downloading — update progress
            self.db.execute(
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
                    ITEM_STATUS_DOWNLOADING,
                ],
            )

    def __handle_unmanaged_torrent(self, t: ClientTorrent) -> None:
        """Handle a torrent that has no active downloading job.

        Try to reclaim if a job exists with removed-by-client status
        (e.g. was prematurely marked due to async add), otherwise pause it.
        """
        restored = self.db.fetch_val(
            """
            update job set status = $1, failed_reason = '', updated_at = current_timestamp
            where info_hash = $2 and node_id = $3 and status = $4
            returning info_hash
            """,
            [
                ITEM_STATUS_DOWNLOADING,
                t.hash,
                self.config.node_id,
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
            ],
        )
        if restored:
            logger.info("reclaimed job for torrent {}", t.hash)
            return

        logger.info("{} not managed, deleting", t.hash)
        self.dl.delete_torrent(t.hash, delete_files=True)

    def __fix_file_selection(self, t: ClientTorrent) -> None:
        """Fix file priorities for torrents that are downloading all files."""
        files = self.dl.list_files(t.hash)
        if len(files) <= 1:
            return
        files_data = [(f.index, f.name, f.size) for f in files]
        keep_idx = find_largest_video_file(files_data)
        if keep_idx is None:
            return
        file_ids = [f.index for f in files if f.index != keep_idx and f.priority != 0]
        if file_ids:
            logger.info("fixing file selection for torrent {}", t.name)
            self.dl.set_file_priority(t.hash, file_ids, 0)

    def __process_local_torrent(self, t: ClientTorrent) -> None:
        files = self.dl.list_files(t.hash)
        active_files = [(f.index, f.name, f.size) for f in files if f.priority != 0]
        selected_idx = find_largest_video_file(active_files)

        if selected_idx is None:
            self.dl.delete_torrent(t.hash)
            return

        selected_file = next(f for f in files if f.index == selected_idx)
        path = Path(t.save_path, selected_file.name)

        media_info = extract_mediainfo_from_file(path)

        hard_code_subtitle = check_hardcode_chinese_subtitle(path)

        self.db.execute(
            """
                update thread set mediainfo = $1, hard_coded_subtitle = $2 where info_hash = $3
                """,
            [media_info, hard_code_subtitle, t.hash],
        )
        self.__update_job_status(status=ITEM_STATUS_DONE, info_hash=t.hash)
        self.dl.delete_torrent(t.hash)

    def __pick_and_add_jobs(self) -> None:
        logger.info("__pick_and_add_jobs")

        current_total_size = sum(t.size for t in self.dl.list_torrents())
        left_size = int(self.config.total_process_size) - current_total_size
        if left_size <= 0:
            logger.info("no left size, skipping")
            return

        picked: list[tuple[int, str]] = []

        logger.info("pick lock")
        with (
            self.db.lock(LOCK_KEY_PICK_RSS_JOB),
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
            logger.info("get lock")
            rows: list[tuple[int, str, int]] = conn.fetch_all(
                """
                select thread.tid, thread.info_hash, thread.selected_size from thread
                left join job on (job.tid = thread.tid)
                where
                    mediainfo = '' and
                    thread.info_hash != '' and
                    thread.selected_size > 0 and
                    thread.selected_size < $1 and
                    category = any ($2) and
                    job.tid is null and
                    seeders != 0
                order by (category = any($3)) desc, tid asc
                limit 6
                """,
                [
                    min(int(self.config.single_torrent_size_limit), left_size),
                    SELECTED_CATEGORY,
                    PRIORITY_CATEGORY,
                ],
            )

            logger.info("fetch {} rows", len(rows))
            if not rows:
                return

            for tid, info_hash, selected_size in rows:
                if left_size - selected_size <= 0:
                    continue

                conn.execute(
                    """
                    insert into job (tid, node_id, info_hash, start_download_time, updated_at, status)
                    VALUES ($1, $2, $3, current_timestamp, current_timestamp, $4)
                    """,
                    [tid, self.config.node_id, info_hash, ITEM_STATUS_DOWNLOADING],
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
                    status=ITEM_STATUS_FAILED, tid=tid, failed_reason=format_exc(e)
                )
                self.dl.delete_torrent(info_hash)

    def __add_torrent(
        self,
        tid: int,
        info_hash: str,
    ) -> None:
        tc = self.store.read(tid)
        if not tc:
            self.__update_job_status(
                status=ITEM_STATUS_FAILED,
                tid=tid,
                failed_reason="torrent content not found",
            )
            return

        tc = set_torrent_comment(tc, f"https://{MTeamDomain}/detail/{tid}")

        ok = self.dl.add_torrent(
            tc,
            save_path=os.path.join(self.config.download_path, info_hash),
            tags=[QB_TAG_DOWNLOADING, QB_TAG_NEED_SELECT],
            download_limit=1,
        )
        if not ok:
            self.__update_job_status(
                status=ITEM_STATUS_FAILED, tid=tid, failed_reason="failed to add"
            )
            self.dl.delete_torrent(info_hash, delete_files=True)


@dataclasses.dataclass(kw_only=True, slots=True)
class Pick:
    title: str
    guid: str
    website: str
    link: str
    released_at: datetime
    size: int
    imdb_id: str = ""
    douban_id: str = ""
