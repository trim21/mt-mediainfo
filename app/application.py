from __future__ import annotations

import dataclasses
import enum
import io
import os.path
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import qbittorrentapi
from qbittorrentapi import TorrentState
from rich.console import Console
from sslog import logger

from app.config import Config, video_ext
from app.const import ITEM_STATUS_DONE, ITEM_STATUS_DOWNLOADING, ITEM_STATUS_SKIPPED
from app.db import Database
from app.hardcode_subtitle import check_hardcode_chinese_subtitle
from app.mediainfo import extract_mediainfo_from_file
from app.torrent import parse_torrent
from app.utils import parse_obj_as


def format_exc(e: Exception) -> str:
    f = io.StringIO()
    with f:
        f.write(f"{type(e)}: {e}\n")
        Console(legacy_windows=True, width=1000, file=f, no_color=True).print_exception()
        return f.getvalue()


class Skip(Exception):
    def __init__(self, guid: str, website: str, reason: str = ""):
        super().__init__()
        self.guid: str = guid
        self.website: str = website
        self.reason: str = reason


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


@dataclasses.dataclass(kw_only=True, frozen=True)
class QbTorrent:
    name: str
    hash: str
    state: TorrentState

    save_path: str  # final download path
    completed: int

    uploaded: int

    total_size: int
    size: int
    amount_left: int

    num_seeds: int


console = Console(emoji=False, force_terminal=True, no_color=False, legacy_windows=True)


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
        except Exception as e:
            print("failed to connect to database", e)
            sys.exit(1)

        print("successfully connect to database")

        version = self.qb.app_version()
        print("successfully connect to qBittorrent", version)

    def start(self) -> None:
        interval = 1
        while True:
            self.__heart_beat()
            time.sleep(interval)
            interval = 60
            try:
                self.__run_at_interval()
            except Exception as e:
                console.print_exception()
                print("failed to run", e)

    def __run_at_interval(self) -> None:
        self.__process_local_torrents()
        picked = self.__pick_job()
        self.__add_picked_to_qb(picked)

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen) values ($1, $2)
            on conflict (id) do update set last_seen = excluded.last_seen
            """,
            [self.config.node_id, datetime.now(tz=timezone.utc)],
        )

    def __process_local_torrents(self) -> None:
        for t in parse_obj_as(list[QbTorrent], self.qb.torrents_info()):
            if not t.state.is_uploading:
                continue

            video_files: list[QbFile] = []

            files = parse_obj_as(list[QbFile], self.qb.torrents_files(torrent_hash=t.hash))
            for file in files:
                if file.name.lower().endswith(video_ext):
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
                "update job set status = $1 where info_hash = $2 and node_id = $3",
                [ITEM_STATUS_DONE, t.hash, self.config.node_id],
            )
            self.qb.torrents_delete(torrent_hashes=t.hash, delete_files=True)

    def __add_picked_to_qb(self, picked: list[tuple[int, str]]) -> None:
        for tid, info_hash in picked:
            tc = self.db.fetch_val("select content from torrent where tid = $1 limit 1", [tid])
            t = parse_torrent(tc)

            video_files = [tf for tf in t.as_files() if tf.name.lower().endswith(video_ext)]
            if not video_files:
                self.db.execute(
                    "update job set status = $1 where tid = $2 and node_id = $3",
                    [ITEM_STATUS_SKIPPED, tid, self.config.node_id],
                )
                continue

            self.qb.torrents_add(
                torrent_files=[tc],
                save_path=os.path.join(self.config.download_path, info_hash),
                use_auto_torrent_management=False,
                is_paused=False,
                is_stopped=False,
            )

    def __pick_job(self) -> list[tuple[int, str]]:
        logger.debug("__pick_job")

        current_total_size = sum(
            t.total_size for t in parse_obj_as(list[QbTorrent], self.qb.torrents_info())
        )
        left_size = int(self.config.total_process_size) - current_total_size
        if left_size < 0:
            return []

        picked: list[tuple[int, str]] = []

        with self.db.lock("pick-job"), self.db.connection() as conn:
            with conn.transaction():
                rows: list[tuple[int, str, int]] = conn.fetch_all(
                    """
                    select thread.tid, thread.info_hash, thread.size from thread
                    left join job on (job.tid = thread.tid)
                    where
                        mediainfo = '' and
                        size < $1 and
                        thread.info_hash != '' and
                        category = any ($2) and
                        job.tid is null
                    order by size asc
                    """,
                    [
                        min(int(self.config.single_torrent_size_limit), left_size),
                        [419, 407, 405, 402, 404],
                    ],
                )
                # all threads already have mediainfo
                if not rows:
                    logger.info("no new job to pick")
                    return []

                for tid, info_hash, size in rows:
                    if left_size - size <= 0:
                        continue
                    left_size = left_size - size

                    conn.execute(
                        """
                    insert into job (tid, node_id, info_hash, start_download_time, status)
                    VALUES ($1, $2, $3, current_timestamp, $4)
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
