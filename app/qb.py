from __future__ import annotations

import contextlib
from typing import Any

import qbittorrentapi
from pydantic import HttpUrl
from qbittorrentapi import NotFound404Error

from app.download_client import ClientFile, ClientTorrent, TorrentState


def _map_state(state: qbittorrentapi.TorrentState) -> TorrentState:
    if state.is_uploading:
        return TorrentState.seeding
    if state.is_paused:
        return TorrentState.paused
    return TorrentState.downloading


def _parse_tags(raw: str) -> frozenset[str]:
    if not raw:
        return frozenset()
    return frozenset(x.strip() for x in raw.split(","))


def _to_client_torrent(t: dict[str, Any]) -> ClientTorrent:
    return ClientTorrent(
        name=str(t["name"]),
        hash=str(t["hash"]),
        state=_map_state(qbittorrentapi.TorrentState(t["state"])),
        save_path=str(t["save_path"]),
        completed=int(t["completed"]),
        uploaded=int(t["uploaded"]),
        total_size=int(t["total_size"]),
        size=int(t["size"]),
        amount_left=int(t["amount_left"]),
        num_seeds=int(t["num_seeds"]),
        progress=float(t["progress"]),
        dlspeed=int(t["dlspeed"]),
        eta=int(t["eta"]),
        tags=_parse_tags(str(t.get("tags", ""))),
        seen_complete=int(t.get("seen_complete", 0)),
    )


def _to_client_file(f: dict[str, Any]) -> ClientFile:
    return ClientFile(
        index=int(f["index"]),
        name=str(f["name"]),
        size=int(f["size"]),
        priority=int(f["priority"]),
        progress=float(f["progress"]),
    )


class QBittorrentClient:
    """DownloadClient implementation backed by qBittorrent Web API."""

    def __init__(self, url: HttpUrl) -> None:
        self._qb = qbittorrentapi.Client(
            host=str(url),
            password=url.password,
            username=url.username,
            SIMPLE_RESPONSES=True,
            FORCE_SCHEME_FROM_HOST=True,
            VERBOSE_RESPONSE_LOGGING=False,
            RAISE_NOTIMPLEMENTEDERROR_FOR_UNIMPLEMENTED_API_ENDPOINTS=True,
            REQUESTS_ARGS={"timeout": 10},
        )

    # -- connection ----------------------------------------------------------

    def connect(self) -> str:
        return self._qb.app_version()  # type: ignore[return-value]

    # -- tick ----------------------------------------------------------------

    def tick(self) -> None:
        pass  # qBittorrent natively tracks seen_complete

    # -- queries -------------------------------------------------------------

    def list_torrents(self) -> list[ClientTorrent]:
        raw: list[dict[str, object]] = self._qb.torrents_info()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        return [_to_client_torrent(t) for t in raw]

    def list_files(self, info_hash: str) -> list[ClientFile]:
        raw: list[dict[str, object]] = self._qb.torrents_files(torrent_hash=info_hash)  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        return [_to_client_file(f) for f in raw]

    # -- mutations -----------------------------------------------------------

    def add_torrent(
        self,
        torrent_data: bytes,
        *,
        save_path: str,
        tags: list[str],
        download_limit: int = 0,
    ) -> bool:
        r = self._qb.torrents_add(
            torrent_files=[torrent_data],
            save_path=save_path,
            use_auto_torrent_management=False,
            tags=tags,
            download_limit=download_limit,
            is_sequential_download=True,
        )
        return r == "Ok."

    def delete_torrent(self, info_hash: str, *, delete_files: bool = True) -> None:
        with contextlib.suppress(NotFound404Error):
            self._qb.torrents_delete(torrent_hashes=info_hash, delete_files=delete_files)

    def pause_torrent(self, info_hash: str) -> None:
        self._qb.torrents_pause(torrent_hashes=info_hash)

    def resume_torrent(self, info_hash: str) -> None:
        self._qb.torrents_resume(torrent_hashes=info_hash)

    def set_download_limit(self, info_hash: str, limit: int) -> None:
        self._qb.torrents_set_download_limit(limit=limit, torrent_hashes=info_hash)

    def add_tags(self, info_hash: str, tags: list[str]) -> None:
        self._qb.torrents_add_tags(tags=tags, torrent_hashes=info_hash)

    def remove_tags(self, info_hash: str, tags: list[str]) -> None:
        self._qb.torrents_remove_tags(tags=tags, torrent_hashes=info_hash)

    def set_file_priority(self, info_hash: str, file_ids: list[int], priority: int) -> None:
        self._qb.torrents_file_priority(
            torrent_hash=info_hash,
            file_ids=file_ids,
            priority=priority,
        )
