from __future__ import annotations

import dataclasses
from typing import Any, Final, Self

import httpcore
import httpx

from app.config import ScrapeConfig, load_scrape_config
from app.utils import parse_obj

MTeamDomain: Final[str] = "kp.m-team.cc"
MTeamApiDomain: Final[str] = "api.m-team.cc"


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

    def __init__(self, c: ScrapeConfig) -> None:
        self._httpx = httpx.Client(
            timeout=60,
            proxy=c.http_proxy or None,
            headers={"x-api-key": c.mt_token},
        )

    def get_download_url(self, tid: str) -> str:
        r = self._httpx.post(
            f"https://{MTeamApiDomain}/api/torrent/genDlToken",
            data={"id": tid},
        ).raise_for_status()

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="genDlToken")

        return data["data"]

    def download_torrent(self, tid: int) -> bytes:
        url = self.get_download_url(str(tid))

        for _ in range(4):
            try:
                rr = self._httpx.get(url, follow_redirects=True).raise_for_status()
            except httpx.TooManyRedirects:
                raise TorrentFileError
            if rr.content == b"file error.":
                raise TorrentFileError
            if rr.content[0] == 123:  # "{"
                data = rr.json()
                if data["message"] == "種子未找到" or data["message"] == "檔案缺失，請聯系管理員":
                    raise TorrentFileError
                raise MTeamRequestError.from_req(data, "download torrent")
            return rr.content

        raise TorrentFileError("too many retries")

    def torrent_detail(self, tid: int) -> TorrentDetail:
        r = self._httpx.post(
            "https://api.m-team.cc/api/torrent/detail",
            data={"id": tid, "origin": f"https://kp.m-team.cc/detail/{tid}"},
        )

        r.raise_for_status()

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="getTorrentDetail")

        return parse_obj(TorrentDetail, data["data"])

    def search(
        self,
        *,
        page_number: int = 1,
        page_size: int = 100,
        mode: str = "normal",
        upload_date_start: str | None = None,
        upload_date_end: str | None = None,
        categories: list[int] | None = None,
        sort_field: str | None = None,
        sort_direction: str | None = None,
    ) -> SearchResult:
        payload: dict[str, Any] = {
            "pageNumber": page_number,
            "pageSize": page_size,
            "mode": mode,
        }
        if upload_date_start is not None:
            payload["uploadDateStart"] = upload_date_start
        if upload_date_end is not None:
            payload["uploadDateEnd"] = upload_date_end
        if categories is not None:
            payload["categories"] = categories
        if sort_field is not None:
            payload["sortField"] = sort_field
        if sort_direction is not None:
            payload["sortDirection"] = sort_direction

        r = self._httpx.post(
            f"https://{MTeamApiDomain}/api/torrent/search",
            json=payload,
        )
        r.raise_for_status()

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="search")

        return parse_obj(SearchResult, data["data"])

    def torrent_mediainfo(self, tid: int) -> str | None:
        """Fetch mediainfo text for a torrent via /torrent/mediaInfo endpoint."""
        r = self._httpx.post(
            f"https://{MTeamApiDomain}/api/torrent/mediaInfo",
            data={"id": tid},
        )
        r.raise_for_status()

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data, op="mediaInfo")

        return data["data"] or None


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


@dataclasses.dataclass
class SearchItemStatus:
    seeders: int
    toppingLevel: str = "0"


@dataclasses.dataclass
class SearchItem:
    id: str
    createdDate: str
    category: str
    size: int
    status: SearchItemStatus


@dataclasses.dataclass
class SearchResult:
    pageNumber: int
    pageSize: int
    total: int
    totalPages: int
    data: list[SearchItem]


if __name__ == "__main__":
    print(MTeamAPI(load_scrape_config()).download_torrent(203098))
