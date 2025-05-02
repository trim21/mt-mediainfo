from __future__ import annotations

import dataclasses
import enum
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar

import packaging.version
import qbittorrentapi
from qbittorrentapi import TorrentState
from rich.console import Console
from sslog import logger

from app.config import Config
from app.const import (
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_PENDING,
    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
)
from app.db import Database
from app.mt import MTeamAPI
from app.utils import parse_obj_as

QB_CATEGORY = "mt-mediainfo"


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

    mteam_client: MTeamAPI

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
            mteam_client=MTeamAPI(cfg),
        )

    def __post_init__(self) -> None:
        try:
            self.db.fetch_val("select version()")
        except Exception as e:
            print("failed to connect to database", e)
            sys.exit(1)

        print("successfully connect to database")

        for sql_file in Path(__file__, "../sql/").resolve().iterdir():
            print("executing {}".format(sql_file.name))
            self.db.execute(sql_file.read_text(encoding="utf-8"))

        try:
            version = packaging.version.parse(self.qb.app_version())
        except Exception as e:
            print("failed to connect to qBittorrent", e)
            sys.exit(1)

        print("successfully connect to qBittorrent")
        if version < packaging.version.parse("v4.5.0"):
            print("qb版本太旧，请升级到 >=4.5.0")
            sys.exit(1)

    def start(self) -> None:
        interval = 1
        while True:
            self.__heart_beat()
            time.sleep(interval)
            interval = 60
            try:
                self.__run_at_interval()
            except Exception as e:
                print("failed to run", e)

    def __run_at_interval(self) -> None:
        self.__process_downloading()
        self.__pick_job()

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen) values ($1, $2)
            on conflict (id) do update set last_seen = excluded.last_seen
            """,
            [self.config.node_id, datetime.now(tz=timezone.utc)],
        )

    def __process_downloading(self) -> None:
        torrents = self.qb.torrents_info()
        for t in torrents:
            print(t)

    def __pick_job(self) -> None:
        logger.debug("__pick_job")

        current_total_size = sum(
            t.total_size for t in parse_obj_as(list[QbTorrent], self.qb.torrents_info())
        )
        left_size = self.config.total_process_size - current_total_size
        if left_size < 0:
            return

        picked: list[tuple[int, str]] = []

        # add distributed lock here when we have multiple nodes.
        with self.db.connection() as conn:
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

                row: tuple[int, str] | None = conn.fetch_one(
                    """
                    select thread.tid, thread.info_hash from thread
                    left join job on (job.tid = thread.tid and job.node_id = $1)
                    where
                        mediainfo = '' and
                        size < $2 and
                        thread.info_hash != ''
                    order by size desc
                    limit 1
                    """,
                    [self.config.node_id, left_size],
                )
                # all threads already have mediainfo
                if not row:
                    logger.info("no new job to pick")
                    return

                print(row)

                tid, info_hash = row

                conn.execute(
                    """
                insert into job (tid, node_id, info_hash, start_download_time)
                VALUES ($1, $2, $3, current_timestamp)
                """,
                    [tid, self.config.node_id, info_hash],
                )
                picked.append(row)

        print(picked)

    def __process_local_downloading(self) -> None:
        """
        may move torrent from download status to uploading status
        """
        self.db.execute(
            """
            update thread
             set status = $1
             where status = $2 and info_hash = $3 and pick_node = $4
            """,
            [
                ITEM_STATUS_PENDING,
                ITEM_STATUS_DOWNLOADING,
                "",
                self.config.node_id,
            ],
        )
        downloading = {
            row[0]
            for row in self.db.fetch_all(
                """select info_hash from thread where status = $1""",
                [ITEM_STATUS_DOWNLOADING],
            )
        }

        local_torrents = parse_obj_as(list[QbTorrent], self.qb.torrents_info(category=QB_CATEGORY))
        local_hashes = {t.hash for t in local_torrents}

        missing_in_local_downloads = {h for h in downloading if h not in local_hashes}
        if missing_in_local_downloads:
            self.db.execute(
                """
                update thread
                 set status = $1, updated_at = current_timestamp
                where info_hash = any($2)
                """,
                [
                    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                    list(missing_in_local_downloads),
                ],
            )

        for t in local_torrents:
            if t.hash not in downloading:
                continue
            if not t.state.is_uploading:
                try:
                    self.db.execute(
                        "update job set progress = $1 where info_hash = $2",
                        [t.completed / t.total_size, t.hash],
                    )
                except Exception as e:
                    logger.warning("failed to update torrent progress {}", e)
                continue

            try:
                self.process_task(t=t)
            except Exception as e:
                logger.warning("failed to process task {!r} {}", e, e)
                self.db.execute(
                    """
                    update thread set status = $1,
                     failed_reason = $2,
                     updated_at = current_timestamp
                    where info_hash = $3
                    """,
                    [
                        ITEM_STATUS_FAILED,  # 1
                        format_exc(e),  # 2
                        t.hash,
                    ],
                )

    def process_task(self, t: QbTorrent) -> None:
        pass

    def export_torrent(self, info_hash: str) -> bytes:
        return self.qb.torrents_export(info_hash)


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


T = TypeVar("T")


def first(s: list[T], default: T) -> T:
    if s:
        return s[0]

    return default
