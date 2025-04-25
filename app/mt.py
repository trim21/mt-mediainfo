import dataclasses
import time
from contextvars import ContextVar
from typing import Any, Final

import httpcore
import httpx
import tenacity
from pydantic import TypeAdapter

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
    def __init__(self, code: str, message: str):
        super().__init__(code, message)
        self.code = code
        self.message = message

    @classmethod
    def from_req(cls, data):
        return cls(data["code"], data["message"])


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

    def __init__(self, c: Config):
        self._httpx = httpx.Client(
            timeout=10,
            proxy=c.http_proxy or None,
            headers={"x-api-key": c.mt_token},
        )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(2),
        retry=tenacity.retry_if_exception_type(httpx_network_errors),
        reraise=True,
    )
    def search(
        self,
        *,
        mode: str,
        discount: str | None = None,
        page: int = 1,
        page_size: int = 200,
    ) -> list[tuple[Any, Torrent]]:
        if nested_depth.get() > 5:
            nested_depth.set(0)
            raise RecursionError("too many recursive call")
        data = {
            "mode": mode,
            "categories": [],
            "visible": 1,
            "pageNumber": page,
            "pageSize": page_size,
        }

        if discount is not None:
            data["discount"] = discount

        r = self._httpx.post(
            f"https://{MTeamApiDomain}/api/torrent/search",
            json=data,
        )

        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                send_critical_message("[pt.rolling]: mteam api token 失效")
            elif e.response.status_code == 400:
                data = r.json()
                if data["message"] == "未知錯誤":
                    send_critical_message(
                        "[pt.rolling]: api search 未知错误 (cf-ray: {!r})".format(
                            r.headers.get("cf-ray"),
                        ),
                    )
                    time.sleep(5)
                    nested_depth.set(nested_depth.get() + 1)
                    return self.search(mode=mode, discount=discount, page=page, page_size=page_size)
            raise

        data = r.json()

        if data["code"] != "0":
            raise MTeamRequestError(data["code"], data["message"])
            # if data["message"] == _Too_Many_Request_Msg:
            #     time.sleep(10)
            #     return self.search(mode=mode, discount=discount, page=page, page_size=page_size)

        return TT.validate_python([(x, x) for x in data["data"]["data"]])

    def get_download_url(self, id: str) -> str:
        try:
            r = self._httpx.post(
                f"https://{MTeamApiDomain}/api/torrent/genDlToken",
                data={"id": id},
            ).raise_for_status()
        except httpx.HTTPStatusError as e:
            raise

        data = r.json()
        if data["code"] != "0":
            raise MTeamRequestError.from_req(data)

        return data["data"]

    def download_torrent(self, tid: str):
        url = self.get_download_url(tid)

        for _ in range(4):
            rr = self._httpx.get(url, follow_redirects=True).raise_for_status()
            if rr.content == b"file error.":
                raise TorrentFileError
            if rr.content[0] == 123:  # "{"
                data = rr.json()
                if data["message"] == "種子未找到" or data["message"] == "檔案缺失，請聯系管理員":
                    raise TorrentFileError
                raise MTeamRequestError.from_req(data)
            return rr.content

        raise Exception("too much retry")

    def torrent_detail(self, tid: int):
        r = self._httpx.post(
            "https://api.m-team.cc/api/torrent/detail",
            data={"id": tid, "origin": f"https://kp.m-team.cc/detail/{tid}"},
        )

        r.raise_for_status()

        return parse_obj_as(TorrentDetail, r.json()["data"])


@dataclasses.dataclass
class TorrentDetail:
    id: str
    createdDate: str
    lastModifiedDate: str
    name: str
    smallDescr: str
    imdb: str
    imdbRating: Any
    douban: str
    doubanRating: str
    dmmCode: Any
    author: Any
    category: str
    source: str
    standard: str
    videoCodec: str
    audioCodec: Any
    team: str
    numfiles: str
    size: str
    labels: str
    msUp: int
    anonymous: bool
    infoHash: str
    editedBy: Any
    editDate: Any
    collection: bool
    inRss: bool
    canVote: bool
    imageList: Any
    originFileName: str
    descr: str
    mediainfo: str | None
