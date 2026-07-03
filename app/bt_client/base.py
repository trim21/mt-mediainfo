import abc
import dataclasses
import enum
from collections.abc import Sequence

ETA_INF = (1 << 31) - 1


class TorrentState(enum.StrEnum):
    UPLOADING = "uploading"
    PAUSED = "paused"
    DOWNLOADING = "downloading"
    ERRORED = "errored"


@dataclasses.dataclass(frozen=True, kw_only=True)
class TorrentFile:
    index: int
    name: str
    size: int
    priority: int
    progress: float


@dataclasses.dataclass(kw_only=True, frozen=True)
class Torrent:
    name: str
    hash: str
    state: TorrentState

    save_path: str
    completed: int

    uploaded: int

    total_size: int
    size: int
    amount_left: int

    num_seeds: int
    progress: float
    dlspeed: int
    eta: int
    tags: frozenset[str]
    seen_complete: int = 0
    error_message: str = ""
    queue_join_ts: int = 0


class TorrentNotFoundError(Exception):
    pass


class BTClient(abc.ABC):
    @abc.abstractmethod
    def app_version(self) -> str: ...

    @abc.abstractmethod
    def torrents_info(self) -> Sequence[Torrent]: ...

    @abc.abstractmethod
    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]: ...

    @abc.abstractmethod
    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None: ...

    @abc.abstractmethod
    def torrents_add(
        self,
        torrent_files: list[bytes],
        save_path: str,
        *,
        use_auto_torrent_management: bool = False,
        tags: list[str] | None = None,
        download_limit: int = 0,
        is_sequential_download: bool = False,
    ) -> str: ...

    @abc.abstractmethod
    def torrents_remove_tags(self, tags: list[str], torrent_hashes: str) -> None: ...

    @abc.abstractmethod
    def torrents_add_tags(self, tags: list[str], torrent_hashes: str) -> None: ...

    @abc.abstractmethod
    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None: ...

    @abc.abstractmethod
    def torrents_resume(self, torrent_hashes: str) -> None: ...

    @abc.abstractmethod
    def torrents_file_priority(
        self, torrent_hash: str, file_ids: list[int], priority: int
    ) -> None: ...

    def tick(self) -> None:
        """Called periodically (≈every minute) for internal maintenance.

        RTorrentClient uses this to enforce the active-download queue via
        per-torrent speed limits.  qBittorrent handles queue natively, so
        the default implementation is a no-op.
        """
