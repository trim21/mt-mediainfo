from __future__ import annotations

import dataclasses
import enum
from typing import Protocol


class TorrentState(enum.Enum):
    downloading = "downloading"
    seeding = "seeding"
    paused = "paused"
    error = "error"
    other = "other"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ClientTorrent:
    name: str
    hash: str
    state: TorrentState

    save_path: str
    completed: int

    uploaded: int

    total_size: int  # total file size
    size: int  # selected file size
    amount_left: int

    num_seeds: int
    progress: float
    dlspeed: int  # bytes/s
    eta: int  # seconds, 8640000 = infinity
    tags: frozenset[str]
    seen_complete: int = 0
    message: str = ""


@dataclasses.dataclass(frozen=True, kw_only=True)
class ClientFile:
    index: int
    name: str
    size: int
    priority: int
    progress: float


class DownloadClient(Protocol):
    def connect(self) -> str:
        """Verify connection and return the client version string."""
        ...

    def list_torrents(self) -> list[ClientTorrent]:
        """Return all torrents managed by the client."""
        ...

    def list_files(self, info_hash: str) -> list[ClientFile]:
        """Return files belonging to a torrent."""
        ...

    def add_torrent(
        self,
        torrent_data: bytes,
        *,
        save_path: str,
        tags: list[str],
    ) -> bool:
        """Add a torrent. Return True on success."""
        ...

    def delete_torrent(self, info_hash: str, *, delete_files: bool = True) -> None:
        """Delete a torrent. Silently ignore if the torrent does not exist."""
        ...

    def pause_torrent(self, info_hash: str) -> None: ...

    def resume_torrent(self, info_hash: str) -> None: ...

    def add_tags(self, info_hash: str, tags: list[str]) -> None: ...

    def remove_tags(self, info_hash: str, tags: list[str]) -> None: ...

    def set_file_priority(self, info_hash: str, file_ids: list[int], priority: int) -> None:
        """Set download priority for specific files. 0 = do not download."""
        ...
