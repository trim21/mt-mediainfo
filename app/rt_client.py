from __future__ import annotations

import json
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import quote

import bencode2
from rtorrent_rpc import RTorrent
from rtorrent_rpc.helper import get_torrent_info_hash, parse_tags

from app.bt_client import (
    ETA_INF,
    BTClient,
    Torrent,
    TorrentFile,
    TorrentNotFoundError,
    TorrentState,
)


def _encode_rt_tags(tags: Iterable[str] | None) -> str:
    if not tags:
        return ""
    if isinstance(tags, str):
        return quote(tags.strip())
    return ",".join(quote(t) for t in sorted({x.strip() for x in tags}) if t)


class RTorrentClient(BTClient):
    def __init__(self, client: RTorrent) -> None:
        self._client = client

    def _call(self, method: str, params: list[Any] | None = None) -> Any:
        return self._client.jsonrpc.call(method, params or [])

    def app_version(self) -> str:
        return str(self._call("system.client_version"))

    def torrents_info(self) -> list[Torrent]:
        rows: list[list[Any]] = self._call(  # type: ignore[assignment]
            "d.multicall2",
            [
                "",
                "",
                "d.name=",
                "d.hash=",
                "d.directory_base=",
                "d.custom1=",
                "d.is_open=",
                "d.size_bytes=",
                "d.state=",
                "d.complete=",
                "d.hashing=",
                "d.bytes_done=",
                "d.down.rate=",
                "d.left_bytes=",
                "d.peers_complete=",
                "d.up.total=",
                "d.timestamp.finished=",
                "d.message=",
                "d.hashing_failed=",
                "d.custom=selected_size=",
            ],
        )

        torrents = []
        for r in rows:
            name = str(r[0])
            info_hash = str(r[1]).lower()
            directory_base = str(r[2])
            custom1 = str(r[3])
            is_open = int(r[4])
            size_bytes = int(r[5])
            state = int(r[6])
            complete = int(r[7])
            bytes_done = int(r[9])
            down_rate = int(r[10])
            left_bytes = int(r[11])
            peers_complete = int(r[12])
            up_total = int(r[13])
            timestamp_finished = int(r[14])
            message = str(r[15])
            hashing_failed = int(r[16])
            selected_size_raw = str(r[17])

            tags = frozenset(parse_tags(custom1))

            selected_size = int(selected_size_raw) if selected_size_raw else 0
            size = selected_size if selected_size > 0 else size_bytes

            if hashing_failed or (message and message != "" and "hash" in message.lower()):
                torrent_state = TorrentState.ERRORED
            elif state == 0 or not is_open:
                torrent_state = TorrentState.PAUSED
            elif complete:
                torrent_state = TorrentState.UPLOADING
            else:
                torrent_state = TorrentState.DOWNLOADING

            if hashing_failed:
                if message and message != "":
                    error_message = f"Hashing failed: {message}"
                else:
                    error_message = f"Hashing failed:\n completed={bytes_done}/{size_bytes} left={left_bytes} state={state} complete={complete} is_open={is_open}"
            elif message and message != "":
                error_message = message
            else:
                error_message = ""

            progress = (bytes_done / size_bytes) if size_bytes > 0 else 0.0

            if left_bytes > 0 and down_rate > 0:
                eta = int(left_bytes / down_rate)
            elif left_bytes > 0:
                eta = ETA_INF
            else:
                eta = 0

            eta = min(ETA_INF, eta)

            torrents.append(
                Torrent(
                    name=name,
                    hash=info_hash,
                    state=torrent_state,
                    save_path=directory_base,
                    completed=bytes_done,
                    uploaded=up_total,
                    total_size=size_bytes,
                    size=size,
                    amount_left=left_bytes,
                    num_seeds=peers_complete,
                    progress=progress,
                    dlspeed=down_rate,
                    eta=eta,
                    tags=tags,
                    seen_complete=timestamp_finished or 0,
                    error_message=error_message,
                )
            )

        return torrents

    def torrents_files(self, torrent_hash: str) -> list[TorrentFile]:
        info_hash = torrent_hash.upper()
        rows: list[list[Any]] = self._call(  # type: ignore[assignment]
            "f.multicall",
            [
                info_hash,
                "",
                "f.path=",
                "f.size_bytes=",
                "f.priority=",
                "f.completed_chunks=",
                "f.size_chunks=",
            ],
        )

        files = []
        for i, r in enumerate(rows):
            path = str(r[0])
            size = int(r[1])
            priority = int(r[2])
            completed_chunks = int(r[3])
            size_chunks = int(r[4])

            if size_chunks > 0:
                file_progress = completed_chunks / size_chunks
            else:
                file_progress = 1.0 if size == 0 else 0.0

            files.append(
                TorrentFile(
                    index=i,
                    name=path,
                    size=size,
                    priority=priority,
                    progress=file_progress,
                )
            )

        return files

    def torrents_delete(self, torrent_hashes: str, *, delete_files: bool = True) -> None:
        info_hash = torrent_hashes.upper()
        try:
            self._call("d.stop", [info_hash])
            self._call("d.close", [info_hash])
            self._call("d.erase", [info_hash])
        except Exception:
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
        for content in torrent_files:
            Path(save_path).mkdir(parents=True, exist_ok=True)

            params: list[str | bytes] = [
                "",
                content,
                'd.tied_to_file.set=""',
                f'd.directory.set="{save_path}"',
            ]

            custom: dict[str, Any] = {"addtime": int(time.time())}

            if tags:
                params.append(f'd.custom1.set="{_encode_rt_tags(tags)}"')

            t = bencode2.bdecode(content)
            if b"comment" in t:
                params.append(f'd.custom2.set="VRS24mrker{quote(t[b"comment"].decode().strip())}"')

            info = t[b"info"]
            if b"files" in info:
                total_size = sum(f[b"length"] for f in info[b"files"])
            else:
                total_size = info[b"length"]
            custom["selected_size"] = total_size

            for key, value in custom.items():
                params.append(f"d.custom.set={key},{json.dumps(value)}")

            self._call("load.raw", params)

            if download_limit > 0:
                self.torrents_set_download_limit(
                    download_limit, self._get_hash_from_content(content)
                )

        return "Ok."

    def torrents_remove_tags(self, tags: str, torrent_hashes: str) -> None:
        info_hash = torrent_hashes.upper()
        raw = str(self._call("d.custom1", [info_hash]))
        current_tags = parse_tags(raw)
        remove_set = {t.strip() for t in tags.split(",")}
        new_tags = current_tags - remove_set
        self._call("d.custom1.set", [info_hash, _encode_rt_tags(new_tags)])

    def torrents_add_tags(self, tags: str, torrent_hashes: str) -> None:
        info_hash = torrent_hashes.upper()
        raw = str(self._call("d.custom1", [info_hash]))
        current_tags = parse_tags(raw)
        add_set = {t.strip() for t in tags.split(",")}
        new_tags = current_tags | add_set
        self._call("d.custom1.set", [info_hash, _encode_rt_tags(new_tags)])

    def torrents_set_download_limit(self, limit: int, torrent_hashes: str) -> None:
        pass

    def torrents_resume(self, torrent_hashes: str) -> None:
        info_hash = torrent_hashes.upper()
        self._call("d.open", [info_hash])
        self._call("d.start", [info_hash])

    def torrents_file_priority(self, torrent_hash: str, file_ids: list[int], priority: int) -> None:
        info_hash = torrent_hash.upper()
        for file_id in file_ids:
            self._call("f.priority.set", [f"{info_hash}:f{file_id}", priority])

        self._compute_and_store_selected_size(info_hash)
        self._call("session.save")

    def _compute_and_store_selected_size(self, info_hash: str) -> int:
        rows: list[tuple[int, int]] = self._call(  # type: ignore[assignment]
            "f.multicall",
            [info_hash, "", "f.size_bytes=", "f.priority="],
        )
        selected_size = sum(size for size, priority in rows if priority != 0)
        if selected_size > 0:
            self._call("d.custom.set", [info_hash, "selected_size", str(selected_size)])
            self._call("d.save_resume", [info_hash])
        return selected_size

    @staticmethod
    def _get_hash_from_content(content: bytes) -> str:
        return get_torrent_info_hash(content).upper()
