import contextlib

from neptune_sdk import NeptuneClient as SDKClient
from neptune_sdk.exceptions import NeptuneRPCError
from neptune_sdk.models import AddTorrentRequest
from neptune_sdk.models import MainDataTorrent as SDKTorrent
from neptune_sdk.models import TorrentFile as SDKFile

from .base import (
    ETA_INF,
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
    amount_left = max(0, size - t.completed)
    download_rate = t.download_rate

    if amount_left > 0 and download_rate > 0:
        eta = int(amount_left / download_rate)
    elif amount_left > 0:
        eta = ETA_INF
    else:
        eta = 0

    return Torrent(
        name=t.name,
        hash=t.hash,
        state=state,
        save_path=t.directory_base,
        completed=t.completed,
        uploaded=t.upload_total,
        total_size=t.total_length,
        size=size,
        amount_left=amount_left,
        num_seeds=t.total_seeding,
        progress=t.completed / size if size > 0 else 0.0,
        dlspeed=download_rate,
        eta=min(ETA_INF, eta),
        tags=frozenset(t.tags),
        seen_complete=0,
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

    def app_version(self) -> str:
        with contextlib.suppress(Exception):
            self._client.ping()
        return "neptune"

    def torrents_info(self) -> list[Torrent]:
        return [_convert_torrent(t) for t in self._client.torrent_list().torrents]

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
    ) -> str:
        for content in torrent_files:
            self._client.torrent_add(
                AddTorrentRequest(
                    torrent_file=content,
                    download_dir=save_path,
                    tags=tags or [],
                    is_base_dir=True,
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
