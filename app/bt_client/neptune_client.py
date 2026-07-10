import contextlib
import json

from neptune_sdk import NeptuneClient as SDKClient
from neptune_sdk.exceptions import NeptuneRPCError
from neptune_sdk.models import AddTorrentRequest
from neptune_sdk.models import MainDataTorrent as SDKTorrent
from neptune_sdk.models import TorrentFile as SDKFile

from app.utils import human_readable_byte_rate, human_readable_size

from .base import (
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)

_STATE_MAP: dict[str, TorrentState] = {
    "Stopped": TorrentState.PAUSED,
    "Downloading": TorrentState.DOWNLOADING,
    "Seeding": TorrentState.UPLOADING,
    "Error": TorrentState.ERRORED,
}


def _convert_torrent(t: SDKTorrent) -> Torrent:
    state = _STATE_MAP.get(t.state, TorrentState.DOWNLOADING)
    size = t.selected_size if t.selected_size > 0 else t.total_length

    return Torrent(
        name=t.name,
        hash=t.hash,
        state=state,
        save_path=t.directory_base,
        completed=t.completed,
        total_size=t.total_length,
        size=size,
        tags=frozenset(t.tags),
        dlspeed=t.download_rate,
        error_message=t.message,
    )


def _convert_file(f: SDKFile) -> TorrentFile:
    return TorrentFile(
        index=f.index,
        name="/".join(f.path),
        size=f.size,
        priority=1,  # Neptune doesn't expose per-file priority; assume normal
        progress=f.progress,
    )


class NeptuneClient(BTClient):
    """BTClient implementation for the Neptune BitTorrent client."""

    def __init__(self, base_url: str, *, token: str, timeout: float = 30.0) -> None:
        self._client = SDKClient(base_url, token=token, timeout=timeout)
        self._raw_torrents: dict[str, SDKTorrent] = {}

    def app_version(self) -> str:
        with contextlib.suppress(Exception):
            self._client.ping()
        return "neptune"

    def torrents_info(self) -> list[Torrent]:
        raw = self._client.torrent_list().torrents
        self._raw_torrents = {t.hash: t for t in raw}
        return [_convert_torrent(t) for t in raw]

    def torrent_debug_info(self, info_hash: str) -> str:
        raw = self._raw_torrents.get(info_hash)
        if raw is None:
            return ""
        return json.dumps(
            {
                "name": raw.name,
                "hash": raw.hash,
                "state": raw.state,
                "download_rate": raw.download_rate,
                "download_rate_fmt": human_readable_byte_rate(raw.download_rate),
                "upload_rate": raw.upload_rate,
                "upload_rate_fmt": human_readable_byte_rate(raw.upload_rate),
                "download_total": raw.download_total,
                "download_total_fmt": human_readable_size(raw.download_total),
                "upload_total": raw.upload_total,
                "upload_total_fmt": human_readable_size(raw.upload_total),
                "completed": raw.completed,
                "completed_fmt": human_readable_size(raw.completed),
                "total_length": raw.total_length,
                "total_length_fmt": human_readable_size(raw.total_length),
                "selected_size": raw.selected_size,
                "selected_size_fmt": human_readable_size(raw.selected_size),
                "corrupted": raw.corrupted,
                "corrupted_fmt": human_readable_size(raw.corrupted),
                "connection_count": raw.connection_count,
                "total_seeding": raw.total_seeding,
                "total_downloading": raw.total_downloading,
                "connected_seeding": raw.connected_seeding,
                "connected_downloading": raw.connected_downloading,
                "message": raw.message,
                "tracker_errors": raw.tracker_errors,
            },
            indent=2,
            ensure_ascii=False,
        )

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        try:
            return [_convert_file(f) for f in self._client.torrent_files(torrent_hash).files]
        except NeptuneRPCError as e:
            raise TorrentNotFoundError(torrent_hash) from e

    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None:
        try:
            self._client.torrent_remove(torrent_hashes, delete_data=delete_files)
        except NeptuneRPCError as e:
            if "not exists" in str(e).lower():
                raise TorrentNotFoundError(torrent_hashes) from e
            raise

    def torrents_add(
        self,
        torrent_files: list[bytes],
        save_path: str,
        *,
        use_auto_torrent_management: bool = False,
        tags: list[str] | None = None,
        download_limit: int = 0,
        is_sequential_download: bool = False,
        selected_files: list[int] | None = None,
    ) -> str:
        for content in torrent_files:
            self._client.torrent_add(
                AddTorrentRequest(
                    torrent_file=content,
                    download_dir=save_path,
                    tags=tags or [],
                    is_base_dir=True,
                    selected_files=selected_files,
                )
            )
        return "Ok."

    def torrents_remove_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._client.torrent_remove_tags(torrent_hashes, tags)

    def torrents_add_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._client.torrent_add_tags(torrent_hashes, tags)

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        self._client.torrent_set_download_limit(torrent_hashes, limit)

    def torrents_resume(self, torrent_hashes: str) -> None:
        self._client.torrent_start(torrent_hashes)

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        self._client.torrent_set_file_priority(torrent_hash, file_ids, priority)
