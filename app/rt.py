from __future__ import annotations

import contextlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

from rtorrent_rpc import RTorrent
from rtorrent_rpc.helper import parse_tags

from app.download_client import ClientFile, ClientTorrent, TorrentState


def _map_state(*, is_complete: bool, is_open: bool, state: int, message: str) -> TorrentState:
    """Map rTorrent state fields to our generic TorrentState.

    rTorrent semantics:
      - ``d.complete`` = 1  → all selected data downloaded
      - ``d.is_open``  = 1  → torrent handle is open (active)
      - ``d.state``    = 1  → started (leeching or seeding)
      - ``d.message``  ≠ "" → torrent has an error

    A torrent with a non-empty message is in error state.
    A torrent that is complete **and** started is seeding.
    A torrent that is **not** started (state=0) or not open is paused/stopped.
    Otherwise it is downloading.
    """
    if message:
        return TorrentState.error
    if is_complete and state == 1:
        return TorrentState.seeding
    if not is_open or state == 0:
        return TorrentState.paused
    return TorrentState.downloading


class _RTorrent(RTorrent):
    def __init__(
        self,
        address: str,
        rutorrent_compatibility: bool = True,
        timeout: float | None = 180,
    ):
        super().__init__(address, rutorrent_compatibility=rutorrent_compatibility, timeout=timeout)
        self._support_json: None | bool = None

    def xml_call(self, method_name: str, /, params: Any = ()) -> Any:
        return self.rpc.call(method_name, params)  # type: ignore[union-attr]

    def json_call(self, method_name: str, /, params: Any = ()) -> Any:
        return self.jsonrpc.call(method_name, params)

    def call(self, method_name: str, /, params: Any = ()) -> Any:
        if self._support_json is None:
            try:
                result = self.jsonrpc.call(method_name, params)
                self._support_json = True
                return result
            except json.JSONDecodeError:
                self._support_json = False

        if self._support_json:
            return self.jsonrpc.call(method_name, params)
        return self.xml_call(method_name, params)


class RTorrentClient:
    """DownloadClient implementation backed by rTorrent (XMLRPC/JSONRPC via scgi)."""

    def __init__(self, address: str) -> None:
        self._rt = _RTorrent(address, rutorrent_compatibility=True, timeout=30)

    # -- connection ----------------------------------------------------------

    def connect(self) -> str:
        methods = self._rt.system_list_methods()
        return f"rTorrent ({len(methods)} methods)"

    # -- tick ----------------------------------------------------------------

    _SEEN_COMPLETE_KEY = "pt_repost_seen_complete"

    def tick(self) -> None:
        """Maintain ``seen_complete`` timestamps for torrents with active seeders.

        rTorrent has no native ``seen_complete`` field.  We synthesise it by
        recording the current unix timestamp into a per-torrent custom key
        whenever ``peers_complete > 0``.
        """
        rows: list[list[Any]] = self._rt.call(
            "d.multicall2",
            [
                "",
                "default",
                "d.hash=",
                "d.peers_complete=",
                f"d.custom={self._SEEN_COMPLETE_KEY}",
            ],
        )
        now_str = str(int(time.time()))
        for row in rows:
            peers_complete = int(row[1])
            if peers_complete > 0:
                info_hash = str(row[0])
                self._rt.call(
                    "d.custom.set",
                    [info_hash, self._SEEN_COMPLETE_KEY, now_str],
                )

    # -- queries -------------------------------------------------------------

    def list_torrents(self) -> list[ClientTorrent]:
        raw: list[list[Any]] = self._rt.call(
            "d.multicall2",
            [
                "",
                "default",
                "d.name=",  # 0
                "d.hash=",  # 1
                "d.directory=",  # 2  full content path (includes torrent subfolder for multi-file)
                "d.custom1=",  # 3  tags (ruTorrent compat)
                "d.size_bytes=",  # 4
                "d.completed_bytes=",  # 5
                "d.up.total=",  # 6
                "d.bytes_done=",  # 7
                "d.left_bytes=",  # 8
                "d.peers_complete=",  # 9  seeders
                "d.is_open=",  # 10
                "d.state=",  # 11
                "d.complete=",  # 12
                "d.down.rate=",  # 13  bytes/s
                f"d.custom={self._SEEN_COMPLETE_KEY}",  # 14  synthesised seen_complete
                "d.message=",  # 15  error message (empty = no error)
            ],
        )

        result: list[ClientTorrent] = []
        for x in raw:
            total_size = int(x[4])
            completed_bytes = int(x[5])
            amount_left = int(x[8])
            is_open = bool(x[10])
            state_int = int(x[11])
            is_complete = bool(x[12])
            message = str(x[15])

            progress = completed_bytes / total_size if total_size > 0 else 0.0
            dlspeed = int(x[13])
            eta = int(amount_left / dlspeed) if dlspeed > 0 else 8640000

            result.append(
                ClientTorrent(
                    name=str(x[0]),
                    hash=str(x[1]).lower(),
                    state=_map_state(
                        is_complete=is_complete,
                        is_open=is_open,
                        state=state_int,
                        message=message,
                    ),
                    save_path=str(x[2]),
                    completed=completed_bytes,
                    uploaded=int(x[6]),
                    total_size=total_size,
                    size=total_size,  # rTorrent has no per-torrent "selected size"; use total
                    amount_left=amount_left,
                    num_seeds=int(x[9]),
                    progress=progress,
                    dlspeed=dlspeed,
                    eta=eta,
                    tags=frozenset(parse_tags(str(x[3]))),
                    seen_complete=int(x[14] or 0),
                    message=message,
                )
            )
        return result

    def list_files(self, info_hash: str) -> list[ClientFile]:
        raw: list[list[Any]] = self._rt.call(
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

        result: list[ClientFile] = []
        for i, x in enumerate(raw):
            size_chunks = int(x[4])
            progress = int(x[3]) / size_chunks if size_chunks > 0 else 0.0
            result.append(
                ClientFile(
                    index=i,
                    name=str(x[0]),
                    size=int(x[1]),
                    priority=int(x[2]),
                    progress=progress,
                )
            )
        return result

    # -- mutations -----------------------------------------------------------

    def add_torrent(
        self,
        torrent_data: bytes,
        *,
        save_path: str,
        tags: list[str],
    ) -> bool:
        self._rt.add_torrent_by_file(
            torrent_data,
            directory_base=save_path,
            tags=tags,
        )
        return True

    def delete_torrent(self, info_hash: str, *, delete_files: bool = True) -> None:
        save_path: str | None = None
        if delete_files:
            with contextlib.suppress(Exception):
                save_path = str(self._rt.call("d.directory_base", [info_hash]))

        with contextlib.suppress(Exception):
            self._rt.stop_torrent(info_hash)
        with contextlib.suppress(Exception):
            self._rt.call("d.erase", [info_hash])

        if delete_files and save_path:
            shutil.rmtree(Path(save_path), ignore_errors=True)

    def pause_torrent(self, info_hash: str) -> None:
        self._rt.stop_torrent(info_hash)

    def resume_torrent(self, info_hash: str) -> None:
        self._rt.start_torrent(info_hash)

    def add_tags(self, info_hash: str, tags: list[str]) -> None:
        current = self._get_tags(info_hash)
        merged = current | set(tags)
        self._rt.d_set_tags(info_hash, merged)

    def remove_tags(self, info_hash: str, tags: list[str]) -> None:
        current = self._get_tags(info_hash)
        remaining = current - set(tags)
        self._rt.d_set_tags(info_hash, remaining)

    def set_file_priority(self, info_hash: str, file_ids: list[int], priority: int) -> None:
        for fid in file_ids:
            self._rt.call("f.priority.set", [f"{info_hash}:f{fid}", priority])

    # -- internal helpers ----------------------------------------------------

    def _get_tags(self, info_hash: str) -> set[str]:
        raw: str = self._rt.d_get_custom(info_hash, "1")
        return set(parse_tags(raw))
