from __future__ import annotations

from typing import Any

import qbittorrentapi
from qbittorrentapi import NotFound404Error

from app.bt_client import BTClient, Torrent, TorrentFile, TorrentNotFoundError, TorrentState


def _convert_state(state: Any) -> TorrentState:
    if state.is_uploading:
        return TorrentState.UPLOADING
    if state.is_paused:
        return TorrentState.PAUSED
    if state.is_errored:
        return TorrentState.ERRORED
    return TorrentState.DOWNLOADING


def _convert_torrent(t: Any) -> Torrent:
    return Torrent(
        name=t.name,
        hash=t.hash,
        state=_convert_state(t.state),
        save_path=t.save_path,
        completed=t.completed,
        uploaded=t.uploaded,
        total_size=t.total_size,
        size=t.size,
        amount_left=t.amount_left,
        num_seeds=t.num_seeds,
        progress=t.progress,
        dlspeed=t.dlspeed,
        eta=t.eta,
        tags=t.tags,
        seen_complete=t.seen_complete or 0,
    )


def _convert_file(f: Any) -> TorrentFile:
    return TorrentFile(
        index=f.index,
        name=f.name,
        size=f.size,
        priority=f.priority,
        progress=f.progress,
    )


class QBittorrentClient(BTClient):
    def __init__(self, client: qbittorrentapi.Client) -> None:
        self._client = client

    def app_version(self) -> str:
        return self._client.app_version()

    def torrents_info(self) -> list[Torrent]:
        return [_convert_torrent(t) for t in self._client.torrents_info()]

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        return [_convert_file(f) for f in self._client.torrents_files(torrent_hash=torrent_hash)]

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

    def torrents_remove_tags(self, tags: str, torrent_hashes: str) -> None:
        self._client.torrents_remove_tags(tags=tags, torrent_hashes=torrent_hashes)

    def torrents_add_tags(self, tags: str, torrent_hashes: str) -> None:
        self._client.torrents_add_tags(tags=tags, torrent_hashes=torrent_hashes)

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        self._client.torrents_set_download_limit(limit=limit, torrent_hashes=torrent_hashes)

    def torrents_resume(self, torrent_hashes: str) -> None:
        self._client.torrents_resume(torrent_hashes=torrent_hashes)

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        self._client.torrents_file_priority(
            torrent_hash=torrent_hash, file_ids=file_ids, priority=priority
        )
