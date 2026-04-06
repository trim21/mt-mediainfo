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
    QB_TAG_PROCESS_ERROR,
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
        self.__cleanup_old_torrents()
        self.__cleanup_unselected_category()
        self.__process_local_torrents()
        self.__pick_and_add_jobs()

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen) values ($1, $2)
            on conflict (id) do update set last_seen = excluded.last_seen
            """,
            [self.config.node_id, datetime.now(tz=UTC)],
        )

    def __cleanup_old_torrents(self) -> None:
        """Delete torrents where Last Seen Complete is before 10 days ago."""
        torrents = parse_obj(list[QbTorrent], self.qb.torrents_info())
        cutoff = time.time() - 10 * 86400
        for t in torrents:
            if 0 < t.seen_complete < cutoff:
                logger.info(
                    "cleanup old torrent {} (last seen complete: {})", t.name, t.seen_complete
                )
                self.db.execute(
                    "update job set status = $1, failed_reason = $2, updated_at = current_timestamp where info_hash = $3 and node_id = $4",
                    [ITEM_STATUS_FAILED, "no seeders", t.hash, self.config.node_id],
                )
                self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __cleanup_unselected_category(self) -> None:
        """Clean up downloading jobs whose thread category is no longer in SELECTED_CATEGORY."""
        rows: list[tuple[str]] = self.db.fetch_all(
            """
            select job.info_hash from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and job.node_id = $2
              and not (thread.category = any($3))
            """,
            [ITEM_STATUS_DOWNLOADING, self.config.node_id, SELECTED_CATEGORY],
        )
        for (info_hash,) in rows:
            logger.info("cleanup unselected category torrent {}", info_hash)
            self.db.execute(
                "update job set status = $1, failed_reason = $2, updated_at = current_timestamp where info_hash = $3 and node_id = $4",
                [
                    ITEM_STATUS_SKIPPED,
                    "category no longer selected",
                    info_hash,
                    self.config.node_id,
                ],
            )
            with contextlib.suppress(NotFound404Error):
                self.qb.torrents_delete(torrent_hashes=info_hash, delete_files=True)

    def __process_local_torrents(self) -> None:
        torrents = parse_obj(list[QbTorrent], self.qb.torrents_info())
        if torrents:
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

        for t in torrents:
            if not t.state.is_uploading:
                # fix file selection for torrents added before the file filtering logic
                if t.total_size == t.size:
                    self.__fix_file_selection(t)

                self.db.execute(
                    """
                    update job set
                      progress = $1,
                      updated_at = current_timestamp
                    where info_hash = $2 and node_id = $3 and status = $4
                    """,
                    [t.progress, t.hash, self.config.node_id, ITEM_STATUS_DOWNLOADING],
                )
                continue

            if QB_TAG_PROCESS_ERROR in t.tags:
                continue

            try:
                self.__process_local_torrent(t)
            except Exception as e:
                self.db.execute(
                    """
                    update job set
                      status = $1,
                      failed_reason = $2,
                      updated_at = current_timestamp
                    where info_hash = $3 and node_id = $4
                    """,
                    [ITEM_STATUS_FAILED, format_exc(e), t.hash, self.config.node_id],
                )
                self.qb.torrents_add_tags(tags=QB_TAG_PROCESS_ERROR, torrent_hashes=t.hash)
                logger.error("failed to process local torrent {}", e)

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
        self.db.execute(
            "update job set status = $1, completed_at = current_timestamp where info_hash = $2 and node_id = $3",
            [ITEM_STATUS_DONE, t.hash, self.config.node_id],
        )
        self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __pick_and_add_jobs(self) -> None:
        logger.debug("__pick_and_add_jobs")

        current_total_size = sum(
            t.size for t in parse_obj(list[QbTorrent], self.qb.torrents_info())
        )
        left_size = int(self.config.total_process_size) - current_total_size
        if left_size <= 0:
            return

        picked: list[tuple[int, str]] = []

        with (
            self.db.lock(LOCK_KEY_PICK_RSS_JOB),
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
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
            self.__add_to_qb(tid, info_hash)

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
            self.db.execute(
                """
                update job set
                  status = $1,
                  failed_reason = $2,
                  updated_at = current_timestamp
                where tid = $3 and node_id = $4
                """,
                [ITEM_STATUS_FAILED, "torrent content not found", tid, self.config.node_id],
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
        )
        if r != "Ok.":
            self.db.execute(
                """
                update job set
                  status = $1,
                  failed_reason = $2,
                  updated_at = current_timestamp
                where tid = $3 and node_id = $4
                """,
                [ITEM_STATUS_FAILED, "failed to add", tid, self.config.node_id],
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
            self.db.execute(
                """
                update job set
                  status = $1,
                  failed_reason = $2,
                  updated_at = current_timestamp
                where tid = $3 and node_id = $4
                """,
                [
                    ITEM_STATUS_FAILED,
                    "torrent not registered in qBittorrent",
                    tid,
                    self.config.node_id,
                ],
            )
            return

        # only download largest single video file
        if t.info.files:
            files_data = [(i, f.name, f.length) for i, f in enumerate(t.info.files)]
            keep_idx = find_largest_video_file(files_data)
            if keep_idx is None:
                self.db.execute(
                    """
                    update job set
                      status = $1,
                      failed_reason = $2,
                      updated_at = current_timestamp
                    where tid = $3 and node_id = $4
                    """,
                    [ITEM_STATUS_SKIPPED, "no video file in torrent", tid, self.config.node_id],
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
