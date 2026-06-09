from __future__ import annotations

import base64
import dataclasses
from collections.abc import Sequence
from pathlib import PurePosixPath
from typing import Any

import httpx

from app.bt_client import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)
from app.utils import parse_obj


def _map_state(state: str) -> TorrentState:
    s = state.lower()
    if s == "stopped":
        return TorrentState.PAUSED
    if s == "downloading":
        return TorrentState.DOWNLOADING
    if s == "seeding":
        return TorrentState.UPLOADING
    if s == "checking":
        return TorrentState.DOWNLOADING
    if s == "moving":
        return TorrentState.PAUSED
    if s == "error":
        return TorrentState.ERRORED
    return TorrentState.ERRORED


def _ensure_jsonrpc_error(resp: dict[str, Any]) -> None:
    if "error" in resp:
        err = resp["error"]
        code = err.get("code", 0)
        message = err.get("message", "unknown error")
        raise RuntimeError(f"JSON-RPC error {code}: {message}")


@dataclasses.dataclass(frozen=True, kw_only=True)
class NeptuneTorrent:
    hash: str
    name: str
    state: str
    comment: str = ""
    directory_base: str = ""
    message: str = ""
    tags: list[str] = dataclasses.field(default_factory=list)
    download_rate: int = 0
    download_total: int = 0
    upload_rate: int = 0
    upload_total: int = 0
    connection_count: int = 0
    completed: int = 0
    total_length: int = 0
    selected_size: int = 0
    add_at: int = 0
    private: bool = False


@dataclasses.dataclass(frozen=True, kw_only=True)
class TorrentListResult:
    torrents: list[NeptuneTorrent] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(frozen=True, kw_only=True)
class NeptuneFile:
    path: list[str] = dataclasses.field(default_factory=list)
    index: int = 0
    progress: float = 0.0
    size: int = 0


@dataclasses.dataclass(frozen=True, kw_only=True)
class TorrentFilesResult:
    files: list[NeptuneFile] = dataclasses.field(default_factory=list)


class NeptuneClient(BTClient):
    def __init__(self, url: str, token: str) -> None:
        self._url = url
        self._client = httpx.Client(
            headers={"Authorization": token},
            timeout=httpx.Timeout(30),
        )
        self._id = 0

    def _call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._id += 1
        body: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "id": self._id,
        }
        if params is not None:
            body["params"] = params

        r = self._client.post(self._url, json=body)
        r.raise_for_status()
        resp = r.json()
        _ensure_jsonrpc_error(resp)
        return resp.get("result")

    def app_version(self) -> str:
        return "neptune"

    def torrents_info(self) -> Sequence[Torrent]:
        r = parse_obj(TorrentListResult, self._call("torrent.list"))
        torrents: list[Torrent] = []
        for item in r.torrents:
            info_hash = item.hash.lower()
            total_size = item.selected_size or item.total_length
            completed = item.completed
            left = total_size - completed
            dl_rate = item.download_rate

            if left > 0 and dl_rate > 0:
                eta = int(left / dl_rate)
            elif left > 0:
                eta = ETA_INF
            else:
                eta = 0
            eta = min(ETA_INF, eta)

            torrents.append(
                Torrent(
                    name=item.name,
                    hash=info_hash,
                    state=_map_state(item.state),
                    save_path=item.directory_base,
                    completed=completed,
                    uploaded=item.upload_total,
                    total_size=total_size,
                    size=total_size,
                    amount_left=left,
                    num_seeds=item.connection_count,
                    progress=float(completed / total_size) if total_size > 0 else 0.0,
                    dlspeed=dl_rate,
                    eta=eta,
                    tags=frozenset(item.tags),
                    seen_complete=item.add_at,
                )
            )
        return torrents

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        r = parse_obj(TorrentFilesResult, self._call("torrent.files", {"info_hash": torrent_hash}))
        files: list[TorrentFile] = []
        for f in r.files:
            name = str(PurePosixPath(*f.path))
            files.append(
                TorrentFile(
                    index=f.index,
                    name=name,
                    size=f.size,
                    priority=1,
                    progress=f.progress,
                )
            )
        return files

    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None:
        result = self._call(
            "torrent.remove",
            {
                "info_hash": torrent_hashes,
                "delete_data": delete_files,
            },
        )
        if result is None:
            raise TorrentNotFoundError(torrent_hashes)

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
            encoded = base64.b64encode(content).decode("ascii")
            params: dict[str, Any] = {
                "torrent_file": encoded,
                "download_dir": save_path,
                "is_base_dir": True,
                "tags": tags or [],
                "selected_files": None,
            }
            self._call("torrent.add", params)

        return "Ok."

    def torrents_remove_tags(self, tags: str, torrent_hashes: str) -> None:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        self._call(
            "torrent.remove_tags",
            {
                "info_hash": torrent_hashes,
                "tags": tag_list,
            },
        )

    def torrents_add_tags(self, tags: str, torrent_hashes: str) -> None:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        self._call(
            "torrent.add_tags",
            {
                "info_hash": torrent_hashes,
                "tags": tag_list,
            },
        )

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        self._call(
            "torrent.set_download_limit",
            {
                "info_hash": torrent_hashes,
                "limit": limit,
            },
        )

    def torrents_resume(self, torrent_hashes: str) -> None:
        self._call("torrent.resume", {"info_hash": torrent_hashes})

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        self._call(
            "torrent.set_file_priority",
            {
                "info_hash": torrent_hash,
                "file_ids": file_ids,
                "priority": priority,
            },
        )
