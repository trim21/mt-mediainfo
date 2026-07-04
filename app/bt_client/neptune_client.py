import base64
import contextlib
import json
import random
from typing import Any

import httpx

from .base import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)


def _neptune_state_to_torrent_state(state: str) -> TorrentState:
    match state:
        case "Stopped":
            return TorrentState.PAUSED
        case "Downloading":
            return TorrentState.DOWNLOADING
        case "Seeding":
            return TorrentState.UPLOADING
        case "Error":
            return TorrentState.ERRORED
        case _:  # Checking, Moving, or unknown
            return TorrentState.DOWNLOADING


class NeptuneClient(BTClient):
    """BTClient implementation for the Neptune BitTorrent client via JSON-RPC."""

    def __init__(self, base_url: str, *, token: str, timeout: float = 30.0) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": token},
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def _call(self, method: str, params: dict[str, object] | None = None) -> Any:
        """Send a JSON-RPC request and return the result field."""
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "id": random.randint(1, 2**31),
        }
        if params is not None:
            payload["params"] = params

        body_bytes = json.dumps(payload, default=self._json_default).encode()

        resp = self._client.post(
            "/json_rpc",
            content=body_bytes,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

        body = resp.json()
        if "error" in body and body["error"] is not None:
            err = body["error"]
            raise NeptuneRPCError(
                code=err.get("code", -1),
                message=err.get("message", ""),
                data=err.get("data"),
            )

        return body.get("result")

    @staticmethod
    def _json_default(obj: Any) -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return base64.b64encode(obj).decode()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    # ── BTClient interface ──────────────────────────────────────────

    def app_version(self) -> str:
        with contextlib.suppress(Exception):
            self._call("system.ping")
        return "neptune"

    def torrents_info(self) -> list[Torrent]:
        result = self._call("torrent.list")
        torrents_data: list[dict[str, Any]] = result.get("torrents", [])

        torrents: list[Torrent] = []
        for t in torrents_data:
            state = _neptune_state_to_torrent_state(t.get("state", ""))
            completed = t.get("completed", 0)
            selected_size = t.get("selected_size", 0)
            total_length = t.get("total_length", 0)
            size = selected_size if selected_size > 0 else total_length
            amount_left = max(0, size - completed)
            download_rate = t.get("download_rate", 0)

            if amount_left > 0 and download_rate > 0:
                eta = int(amount_left / download_rate)
            elif amount_left > 0:
                eta = ETA_INF
            else:
                eta = 0

            if size > 0:
                progress = completed / size
            else:
                progress = 0.0

            torrents.append(
                Torrent(
                    name=t.get("name", ""),
                    hash=t.get("hash", ""),
                    state=state,
                    save_path=t.get("directory_base", ""),
                    completed=completed,
                    uploaded=t.get("upload_total", 0),
                    total_size=total_length,
                    size=size,
                    amount_left=amount_left,
                    num_seeds=t.get("total_seeding", 0),
                    progress=progress,
                    dlspeed=download_rate,
                    eta=min(ETA_INF, eta),
                    tags=frozenset(t.get("tags", [])),
                    seen_complete=0,
                    error_message=t.get("message", ""),
                )
            )

        return torrents

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        try:
            result = self._call("torrent.files", {"info_hash": torrent_hash})
        except NeptuneRPCError as e:
            raise TorrentNotFoundError(torrent_hash) from e

        files_data: list[dict[str, Any]] = result.get("files", [])

        files: list[TorrentFile] = []
        for f in files_data:
            path_parts: list[str] = f.get("path", [])
            files.append(
                TorrentFile(
                    index=f.get("index", 0),
                    name="/".join(path_parts),
                    size=f.get("size", 0),
                    priority=1,  # Neptune doesn't expose priority; assume normal
                    progress=f.get("progress", 0.0),
                )
            )

        return files

    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None:
        try:
            self._call(
                "torrent.remove",
                {"info_hash": torrent_hashes, "delete_data": delete_files},
            )
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
            self._call(
                "torrent.add",
                {
                    "torrent_file": content,
                    "download_dir": save_path,
                    "tags": tags or [],
                    "is_base_dir": True,
                },
            )
        return "Ok."

    def torrents_remove_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._call(
            "torrent.remove_tags",
            {"info_hash": torrent_hashes, "tags": tags},
        )

    def torrents_add_tags(self, tags: list[str], torrent_hashes: str) -> None:
        self._call(
            "torrent.add_tags",
            {"info_hash": torrent_hashes, "tags": tags},
        )

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        self._call(
            "torrent.set_download_limit",
            {"info_hash": torrent_hashes, "limit": limit},
        )

    def torrents_resume(self, torrent_hashes: str) -> None:
        self._call("torrent.start", {"info_hash": torrent_hashes})

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        self._call(
            "torrent.set_file_priority",
            {"info_hash": torrent_hash, "file_ids": file_ids, "priority": priority},
        )


class NeptuneRPCError(Exception):
    """Raised when the Neptune JSON-RPC server returns an error."""

    def __init__(self, code: int, message: str, data: object = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"Neptune RPC error {code}: {message}")
