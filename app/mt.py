from __future__ import annotations

import dataclasses
from contextvars import ContextVar
from typing import Any, Final

import httpcore
import httpx
from pydantic import TypeAdapter
from typing_extensions import Self

from app.config import Config
from app.utils import parse_obj_as

MTeamDomain: Final[str] = "kp.m-team.cc"
MTeamApiDomain: Final[str] = "api.m-team.cc"


@dataclasses.dataclass(slots=True, kw_only=True, frozen=True)
class Torrent:
    createdDate: str
    id: int
    infoHash: str | None
    name: str
    size: int


TT = TypeAdapter[list[tuple[Any, Torrent]]](list[tuple[Any, Torrent]])

nested_depth = ContextVar("nested_depth", default=0)


class MTeamRequestError(Exception):
    def __init__(self, code: str, message: str, op: str | None = None):
        super().__init__(code, message)
        self.code = code
        self.message = message
        self.op = op

    @classmethod
    def from_req(cls, data: dict[str, str], op: str | None = None) -> Self:
        return cls(data["code"], data["message"], op=op)


network_errors: Final[tuple[type[Exception], ...]] = (
    ConnectionError,
    TimeoutError,
)

httpx_network_errors: Final[tuple[type[Exception], ...]] = (
    *network_errors,
    httpcore.TimeoutException,
    httpx.NetworkError,
    httpx.TransportError,
    httpx.TimeoutException,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


class TorrentFileError(Exception):
    pass


class MTeamAPI:
    _httpx: httpx.Client

    __slots__ = ("_httpx",)

    def __init__(self, c: Config) -> None:
        self._httpx = httpx.Client(
            timeout=10,
            proxy=c.http_proxy or None,
            headers={"x-api-key": c.mt_token},
        )

    def get_download_url(self, tid: str) -> str:
        try:
            r = self._httpx.post(
                f"https://{MTeamApiDomain}/api/torrent/genDlToken",
                data={"id": tid},
            ).raise_for_status()
        except httpx.HTTPStatusError:
            raise

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="genDlToken")

        return data["data"]

    def download_torrent(self, tid: int) -> bytes:
        url = self.get_download_url(str(tid))

        for _ in range(4):
            rr = self._httpx.get(url, follow_redirects=True).raise_for_status()
            if rr.content == b"file error.":
                raise TorrentFileError
            if rr.content[0] == 123:  # "{"
                data = rr.json()
                if data["message"] == "種子未找到" or data["message"] == "檔案缺失，請聯系管理員":
                    raise TorrentFileError
                raise MTeamRequestError.from_req(data, "download torrent")
            return rr.content

        raise Exception("too much retry")

    def torrent_detail(self, tid: int) -> TorrentDetail:
        r = self._httpx.post(
            "https://api.m-team.cc/api/torrent/detail",
            data={"id": tid, "origin": f"https://kp.m-team.cc/detail/{tid}"},
        )

        r.raise_for_status()

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="getTorrentDetail")

        return parse_obj_as(TorrentDetail, data["data"])


@dataclasses.dataclass
class TorrentStatus:
    seeders: int
    leechers: int


@dataclasses.dataclass
class TorrentDetail:
    id: str
    createdDate: str
    lastModifiedDate: str
    status: TorrentStatus
    name: str
    category: str
    standard: str
    size: int
    labels: str
    msUp: int
    anonymous: bool
    # info_hash: Annotated[str, pydantic.Field(alias="infoHash")]
    collection: bool
    inRss: bool
    canVote: bool
    originFileName: str
    descr: str
    mediainfo: str | None
