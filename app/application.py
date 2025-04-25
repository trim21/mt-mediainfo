from __future__ import annotations

import dataclasses
import enum
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, TypeVar

import annotated_types
import packaging.version
import qbittorrentapi
from pydantic import Field
from qbittorrentapi import TorrentState
from rich.console import Console

from app.config import Config
from app.db import Database
from app.mt import MTeamAPI


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
        self.__fetch_new_torrents()

    def __heart_beat(self) -> None:
        self.db.execute(
            """
            insert into node (id, last_seen) values ($1, $2)
            on conflict (id) do update set last_seen = excluded.last_seen
            """,
            [self.config.node_id, datetime.now(tz=timezone.utc)],
        )

    def __fetch_new_torrents(self):
        torrents = self.db.fetch_all(
            """
            select * from torrent where mediainfo = '' and  pick_node is null order by tid asc
            """
        )

        print(len(torrents))

    def export_torrent(self, info_hash: str) -> bytes:
        return self.qb.torrents_export(info_hash)


def _transform_info(obj: dict[bytes, Any]) -> dict[str, Any]:
    d = {}
    for key, value in obj.items():
        if key == b"pieces":
            d[key.decode()] = value
        else:
            d[key.decode()] = _transform_value(value)
    return d


def _transform_dict(obj: dict[bytes, Any]) -> dict[str, Any]:
    return {key.decode(): _transform_value(value) for key, value in obj.items()}


def _transform_value(v: Any) -> Any:
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return v
    if isinstance(v, dict):
        return _transform_dict(v)
    if isinstance(v, list):
        return [_transform_value(o) for o in v]
    return v


@dataclasses.dataclass(kw_only=True, slots=True)
class File:
    length: int
    path: Annotated[tuple[str, ...], annotated_types.MinLen(1)]

    @property
    def name(self) -> str:
        return self.path[-1]


@dataclasses.dataclass(kw_only=True, slots=False, frozen=True)
class TorrentInfo:
    name: Annotated[str, annotated_types.MinLen(1)]
    pieces: bytes
    length: int | None = None
    private: bool = False
    files: Annotated[tuple[File, ...], Field(default_factory=tuple)]
    piece_length: Annotated[int, Field(alias="piece length")]
    # common used field for private tracker
    source: str | None = None


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
