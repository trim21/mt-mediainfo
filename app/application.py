from __future__ import annotations

import contextlib
import dataclasses
import enum
import io
import os.path
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import qbittorrentapi
from pydantic import BeforeValidator
from qbittorrentapi import NotFound404Error, TorrentState
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
    QB_TAG_DOWNLOADING,
    QB_TAG_PROCESS_ERROR,
    QB_TAG_PROCESSING,
    QB_TAG_SELECTING_FILES,
    SELECTED_CATEGORY,
)
from app.db import Database
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_mediainfo_from_file
from app.mt import MTeamDomain
from app.torrent import find_largest_video_file, parse_torrent
from app.utils import parse_obj, set_torrent_comment


def format_exc(e: Exception) -> str:
    f = io.StringIO()
    with f:
        f.write(f"{type(e)}: {e}\n")
        Console(legacy_windows=True, width=1000, file=f, no_color=True).print_exception()
        return f.getvalue()


class Status(enum.IntEnum):
    unknown = 0
    downloading = 1
    done = 3


@dataclasses.dataclass(frozen=True, kw_only=True)
class QbFile:
    index: int
    name: str
    size: int
    priority: int
    progress: float


def _parse_str_tags(v: str) -> frozenset[str]:
    if not v:
        return frozenset()
    return frozenset({x.strip() for x in v.split(",")})


@dataclasses.dataclass(kw_only=True, frozen=True)
class QbTorrent:
    name: str
    hash: str
    state: TorrentState

    save_path: str  # final download path
    completed: int

    uploaded: int

    total_size: int  # total file size
    size: int  # select file size
    amount_left: int

    num_seeds: int
    progress: float
    tags: Annotated[frozenset[str], BeforeValidator(_parse_str_tags)]
    seen_complete: int = 0


@dataclasses.dataclass(kw_only=True, frozen=True)
class Application:
    db: Database
    config: Config
    qb: qbittorrentapi.Client

    @classmethod
    def new(cls, cfg: Config) -> Application:
        return Application(
            config=cfg,
            db=Database(cfg.pg_dsn()),
            qb=qbittorrentapi.Client(
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

    def __post_init__(self) -> None:
        try:
            self.db.fetch_val("select version()")
        except Exception:
            logger.exception("failed to connect to database")
            sys.exit(1)

        logger.info("successfully connect to database")

        version = self.qb.app_version()
        logger.info("successfully connect to qBittorrent {}", version)

    def start(self) -> None:
        interval = 1
        while True:
            self.__heart_beat()
            time.sleep(interval)
            interval = 60
            try:
                self.__run_at_interval()
            except Exception as e:
                print("failed to run", format_exc(e))

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
        self.qb.torrents_remove_tags(tags=remove, torrent_hashes=info_hash)
        self.qb.torrents_add_tags(tags=add, torrent_hashes=info_hash)

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
        """Process all torrents in qBittorrent in a single pass."""
        logger.info("__process_qb_torrents")
        torrents = parse_obj(list[QbTorrent], self.qb.torrents_info())
        if not torrents:
            logger.info("qb has no torrents")
            return

        # Mark jobs as removed-from-client if their torrent is no longer in qb
        self.db.execute(
            """
                update job set
                  status = $1,
                  updated_at = current_timestamp
                where (not info_hash = any($2)) and node_id = $3 and status = $4
                """,
            [
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                [x.hash for x in torrents],
                self.config.node_id,
                ITEM_STATUS_DOWNLOADING,
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
            # Skip torrents not managed by us
            if t.hash not in managed_hashes:
                logger.info("{} not managed", t.hash)
                if not t.state.is_paused:
                    self.qb.torrents_pause(torrent_hashes=t.hash)
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
                self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)
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
                self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)
                continue

            # Upload complete → process mediainfo
            if t.state.is_uploading:
                self.__set_tags(t.hash, remove=QB_TAG_DOWNLOADING, add=QB_TAG_PROCESSING)
                try:
                    self.__process_local_torrent(t)
                except Exception as e:
                    self.__update_job_status(
                        status=ITEM_STATUS_FAILED,
                        info_hash=t.hash,
                        failed_reason=format_exc(e),
                    )
                    self.qb.torrents_add_tags(tags=QB_TAG_PROCESS_ERROR, torrent_hashes=t.hash)
                    logger.error("failed to process local torrent {}", e)
                continue

            # Fix file selection for any torrent not yet fully downloaded
            self.__fix_file_selection(t)

            # Paused → resume
            if t.state.is_paused:
                logger.info("resuming stopped torrent {} (tags={})", t.name, t.tags)
                self.__set_tags(t.hash, remove=QB_TAG_SELECTING_FILES, add=QB_TAG_DOWNLOADING)
                self.qb.torrents_resume(torrent_hashes=t.hash)
                continue

            # Downloading — update progress
            self.db.execute(
                """
                update job set
                  progress = $1,
                  updated_at = current_timestamp
                where info_hash = $2 and node_id = $3 and status = $4
                """,
                [t.progress, t.hash, self.config.node_id, ITEM_STATUS_DOWNLOADING],
            )

    def __fix_file_selection(self, t: QbTorrent) -> None:
        """Fix file priorities for torrents that are downloading all files."""
        files = parse_obj(list[QbFile], self.qb.torrents_files(torrent_hash=t.hash))
        if len(files) <= 1:
            return
        files_data = [(f.index, f.name, f.size) for f in files]
        keep_idx = find_largest_video_file(files_data)
        if keep_idx is None:
            return
        file_ids = [f.index for f in files if f.index != keep_idx and f.priority != 0]
        if file_ids:
            logger.info("fixing file selection for torrent {}", t.name)
            self.qb.torrents_file_priority(
                torrent_hash=t.hash,
                file_ids=file_ids,
                priority=0,
            )

    def __process_local_torrent(self, t: QbTorrent) -> None:
        files = parse_obj(list[QbFile], self.qb.torrents_files(torrent_hash=t.hash))
        active_files = [(f.index, f.name, f.size) for f in files if f.priority != 0]
        selected_idx = find_largest_video_file(active_files)

        if selected_idx is None:
            self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)
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
        self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __pick_and_add_jobs(self) -> None:
        logger.info("__pick_and_add_jobs")

        current_total_size = sum(
            t.size for t in parse_obj(list[QbTorrent], self.qb.torrents_info())
        )
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
                order by selected_size desc
                limit 6
                """,
                [
                    min(int(self.config.single_torrent_size_limit), left_size),
                    SELECTED_CATEGORY,
                ],
            )

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

        # add to qBittorrent outside the lock to avoid blocking other nodes
        for tid, info_hash in picked:
            try:
                self.__add_to_qb(tid, info_hash)
            except Exception as e:
                logger.error("failed to add torrent tid={} to qb: {}", tid, e)
                self.__update_job_status(
                    status=ITEM_STATUS_FAILED, tid=tid, failed_reason=format_exc(e)
                )
                with contextlib.suppress(NotFound404Error):
                    self.qb.torrents_delete(torrent_hashes=info_hash, delete_files=True)

    def __add_to_qb(
        self,
        tid: int,
        info_hash: str,
    ) -> None:
        tc = self.db.fetch_val(
            "select content from torrent where tid = $1 limit 1",
            [tid],
        )
        if not tc:
            self.__update_job_status(
                status=ITEM_STATUS_FAILED,
                tid=tid,
                failed_reason="torrent content not found",
            )
            return

        t = parse_torrent(tc)
        tc = set_torrent_comment(tc, f"https://{MTeamDomain}/detail/{tid}")

        # add torrent in stopped state so we can set file priorities before downloading
        r = self.qb.torrents_add(
            torrent_files=[tc],
            save_path=os.path.join(self.config.download_path, info_hash),
            use_auto_torrent_management=False,
            is_paused=True,
            is_stopped=True,
            tags=QB_TAG_SELECTING_FILES,
        )
        if r != "Ok.":
            self.__update_job_status(
                status=ITEM_STATUS_FAILED, tid=tid, failed_reason="failed to add"
            )
            return

        # wait for qBittorrent to register the torrent
        registered = False
        for _ in range(30):
            if self.qb.torrents_info(torrent_hashes=info_hash):
                registered = True
                break
            time.sleep(1)

        if not registered:
            self.__update_job_status(
                status=ITEM_STATUS_FAILED,
                tid=tid,
                failed_reason=f"torrent {info_hash} not found in qBittorrent after 30s waiting (tid={tid}, torrents_add returned Ok but torrent never appeared)",
            )
            return

        # only download largest single video file
        if t.info.files:
            files_data = [(i, f.name, f.length) for i, f in enumerate(t.info.files)]
            keep_idx = find_largest_video_file(files_data)
            if keep_idx is None:
                self.__update_job_status(
                    status=ITEM_STATUS_SKIPPED,
                    tid=tid,
                    failed_reason="no video file in torrent",
                )
                self.qb.torrents_delete(torrent_hashes=info_hash, delete_files=True)
                return
            file_ids = [i for i, _, _ in files_data if i != keep_idx]
            if file_ids:
                self.qb.torrents_file_priority(
                    torrent_hash=info_hash,
                    file_ids=file_ids,
                    priority=0,
                )

        # now start the torrent
        self.__set_tags(info_hash, remove=QB_TAG_SELECTING_FILES, add=QB_TAG_DOWNLOADING)
        self.qb.torrents_resume(torrent_hashes=info_hash)


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
