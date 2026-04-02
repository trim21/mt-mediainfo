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
    ITEM_STATUS_SKIPPED,
    LOCK_KEY_PICK_RSS_JOB,
    QB_TAG_PROCESS_ERROR,
    SELECTED_CATEGORY,
    VIDEO_FILE_EXT,
)
from app.db import Database
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_mediainfo_from_file
from app.mt import MTeamDomain
from app.torrent import parse_torrent
from app.utils import parse_obj_as, set_torrent_comment


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
            db=Database(cfg),
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
        self.__process_local_torrents()
        picked = self.__pick_job()
        self.__add_picked_to_qb(picked)

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
        torrents = parse_obj_as(list[QbTorrent], self.qb.torrents_info())
        cutoff = time.time() - 10 * 86400
        for t in torrents:
            if t.seen_complete > 0 and t.seen_complete < cutoff:
                logger.info(
                    "cleanup old torrent {} (last seen complete: {})", t.name, t.seen_complete
                )
                self.db.execute(
                    "update job set status = $1, failed_reason = $2, updated_at = current_timestamp where info_hash = $3 and node_id = $4",
                    [ITEM_STATUS_FAILED, "no seeders", t.hash, self.config.node_id],
                )
                self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __process_local_torrents(self) -> None:
        torrents = parse_obj_as(list[QbTorrent], self.qb.torrents_info())
        if torrents:
            self.db.execute(
                """
                    update job set
                      status = $1,
                      updated_at = current_timestamp
                    where (not info_hash = any($2)) and node_id = $3 and status = $4
                    """,
                [
                    ITEM_STATUS_SKIPPED,
                    [x.hash for x in torrents],
                    self.config.node_id,
                    ITEM_STATUS_DOWNLOADING,
                ],
            )

        for t in torrents:
            if not t.state.is_uploading:
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

    def __process_local_torrent(self, t: QbTorrent) -> None:
        video_files: list[QbFile] = []

        files = parse_obj_as(list[QbFile], self.qb.torrents_files(torrent_hash=t.hash))
        for file in files:
            if file.priority == 0:
                continue

            if file.name.lower().endswith(VIDEO_FILE_EXT):
                video_files.append(file)

        if not video_files:
            self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)
            return

        video_files.sort(key=lambda x: x.size, reverse=True)
        path = Path(t.save_path, video_files[0].name)

        media_info = extract_mediainfo_from_file(path)

        hard_code_subtitle = check_hardcode_chinese_subtitle(path)

        self.db.execute(
            """
                update thread set mediainfo = $1, hard_coded_subtitle = $2 where info_hash = $3
                """,
            [media_info, hard_code_subtitle, t.hash],
        )
        self.db.execute(
            "update job set status = $1, download_size = $2 where info_hash = $3 and node_id = $4",
            [ITEM_STATUS_DONE, t.size, t.hash, self.config.node_id],
        )
        self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __add_picked_to_qb(self, picked: list[tuple[int, str]]) -> None:
        for tid, info_hash in picked:
            tc = self.db.fetch_val(
                "select content from torrent where tid = $1 limit 1",
                [tid],
            )
            t = parse_torrent(tc)

            video_files = [tf for tf in t.as_files() if tf.name.lower().endswith(VIDEO_FILE_EXT)]
            if not video_files:
                self.db.execute(
                    """
                    update job set
                      status = $1,
                      updated_at = current_timestamp
                    where
                      tid = $2 and node_id = $3
                    """,
                    [ITEM_STATUS_SKIPPED, tid, self.config.node_id],
                )
                continue

            tc = set_torrent_comment(tc, f"https://{MTeamDomain}/detail/{tid}")

            r = self.qb.torrents_add(
                torrent_files=[tc],
                save_path=os.path.join(self.config.download_path, info_hash),
                use_auto_torrent_management=False,
                is_paused=False,
                is_stopped=False,
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
                continue

            # only download lartest single video file
            if t.info.files:
                files = list(enumerate(list(t.info.files)))
                file_ids: set[int] = set()
                find_video_file = False

                for index, file in sorted(
                    [(index, file) for index, file in files],
                    key=lambda y: y[1].length,
                    reverse=True,
                ):
                    if file.name.lower().endswith(VIDEO_FILE_EXT) and not find_video_file:
                        find_video_file = True
                        continue
                    file_ids.add(index)

                if file_ids:
                    # give qbittorrent some time to process the torrent
                    time.sleep(10)
                    with contextlib.suppress(NotFound404Error):
                        self.qb.torrents_file_priority(
                            torrent_hash=info_hash,
                            file_ids=list(file_ids),
                            priority=0,
                        )

    def __pick_job(self) -> list[tuple[int, str]]:
        logger.debug("__pick_job")

        current_total_size = sum(
            t.size for t in parse_obj_as(list[QbTorrent], self.qb.torrents_info())
        )
        left_size = int(self.config.total_process_size) - current_total_size
        if left_size <= 0:
            return []

        picked: list[tuple[int, str]] = []

        with (
            self.db.lock(LOCK_KEY_PICK_RSS_JOB),
            self.db.connection() as conn,
            conn.transaction() as _,
        ):
            rows: list[tuple[int, str, int]] = conn.fetch_all(
                """
                select thread.tid, thread.info_hash, thread.size from thread
                left join job on (job.tid = thread.tid)
                where
                    mediainfo = '' and
                    size < $1 and
                    thread.info_hash != '' and
                    category = any ($2) and
                    job.tid is null and
                    seeders != 0
                order by size desc
                limit 6
                """,
                [
                    min(int(self.config.single_torrent_size_limit), left_size),
                    SELECTED_CATEGORY,
                ],
            )

            if not rows:
                return []

            logger.info("pick {} new jobs", len(rows))

            for tid, info_hash, size in rows:
                if left_size - size <= 0:
                    continue
                left_size = left_size - size

                conn.execute(
                    """
                insert into job (tid, node_id, info_hash, start_download_time, updated_at, status)
                VALUES ($1, $2, $3, current_timestamp, current_timestamp, $4)
                    """,
                    [tid, self.config.node_id, info_hash, ITEM_STATUS_DOWNLOADING],
                )
                picked.append((tid, info_hash))

        return picked


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


def backfill_download_size(cfg: Config) -> None:
    """Backfill download_size for old done jobs using torrent file content."""
    db = Database(cfg)

    rows: list[tuple[int, ...]] = db.fetch_all(
        """
        select job.tid, job.node_id from job
        where job.status = $1 and job.download_size = 0
        """,
        [ITEM_STATUS_DONE],
    )

    logger.info("found {} jobs to backfill", len(rows))

    for tid, node_id in rows:
        tc = db.fetch_val("select content from torrent where tid = $1 limit 1", [tid])
        if tc is None:
            logger.warning("no torrent content for tid={}", tid)
            continue

        t = parse_torrent(tc)

        # same logic as __add_picked_to_qb: keep only the largest video file
        if t.info.files:
            video_files = [f for f in t.info.files if f.name.lower().endswith(VIDEO_FILE_EXT)]
            if video_files:
                video_files.sort(key=lambda x: x.length, reverse=True)
                download_size = video_files[0].length
            else:
                download_size = t.total_length
        else:
            download_size = t.total_length

        db.execute(
            "update job set download_size = $1 where tid = $2 and node_id = $3",
            [download_size, tid, node_id],
        )
        logger.info("backfilled tid={} download_size={}", tid, download_size)

    logger.info("backfill complete")
