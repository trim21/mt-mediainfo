import dataclasses
from collections.abc import Sequence
from typing import Annotated, Any

import qbittorrentapi
from pydantic import BeforeValidator
from qbittorrentapi import NotFound404Error

from app.utils import parse_obj

from ._base import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)


def _normalize_eta(v: Any) -> int:
    n = int(v)
    if n < 0 or n >= ETA_INF:
        return ETA_INF
    return n


def _normalize_state(v: Any) -> TorrentState:
    s = str(v).lower()
    if "uploading" in s or ("up" in s and "paused" not in s):
        return TorrentState.UPLOADING
    if "paused" in s:
        return TorrentState.PAUSED
    if s in ("error", "missingfiles", "unknown"):
        return TorrentState.ERRORED
    return TorrentState.DOWNLOADING


def _parse_str_tags(v: str) -> frozenset[str]:
    if not v:
        return frozenset()
    return frozenset({x.strip() for x in v.split(",")})


@dataclasses.dataclass(kw_only=True, frozen=True)
class QbTorrent(Torrent):
    eta: Annotated[int, BeforeValidator(_normalize_eta)]
    state: Annotated[TorrentState, BeforeValidator(_normalize_state)]
    tags: Annotated[frozenset[str], BeforeValidator(_parse_str_tags)]


class QBittorrentClient(BTClient):
    def __init__(self, client: qbittorrentapi.Client) -> None:
        self._client = client

    def app_version(self) -> str:
        return self._client.app_version()

    def torrents_info(self) -> Sequence[Torrent]:
        return parse_obj(list[QbTorrent], self._client.torrents_info())

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        return parse_obj(list[TorrentFile], self._client.torrents_files(torrent_hash=torrent_hash))

    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None:
        try:
            self._client.torrents_delete(torrent_hashes=torrent_hashes, delete_files=delete_files)
        except NotFound404Error:
            raise TorrentNotFoundError(torrent_hashes) from None

    def torrents_add(
        self,
        torrent_files: list[bytes],
        save_path: str,
        *,
        use_auto_torrent_management: bool = False,
        tags: list[str] | None = None,
        download_limit: int = 0,
        is_sequential_download: bool = False,
    ) -> str:
        r = self._client.torrents_add(
            torrent_files=torrent_files,
            save_path=save_path,
            use_auto_torrent_management=use_auto_torrent_management,
            tags=tags,
            download_limit=download_limit,
            is_sequential_download=is_sequential_download,
        )
        return str(r)

    def torrents_remove_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._client.torrents_remove_tags(tags=tags, torrent_hashes=torrent_hashes)

    def torrents_add_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._client.torrents_add_tags(tags=tags, torrent_hashes=torrent_hashes)

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        self._client.torrents_set_download_limit(limit=limit, torrent_hashes=torrent_hashes)

    def torrents_resume(self, torrent_hashes: str) -> None:
        self._client.torrents_resume(torrent_hashes=torrent_hashes)

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        self._client.torrents_file_priority(
            torrent_hash=torrent_hash, file_ids=file_ids, priority=priority
        )
