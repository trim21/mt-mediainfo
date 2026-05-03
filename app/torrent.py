import dataclasses
from functools import cached_property
from typing import Annotated, Any, cast

import annotated_types
import bencode2
import pydantic
from pydantic import Field

from app.const import VIDEO_FILE_EXT


def _transform_info(obj: dict[bytes, Any]) -> dict[str, Any]:
    d = {}
    for key, value in obj.items():
        if key == b"pieces":
            d[key.decode()] = value
        else:
            d[key.decode()] = _transform_value(value)
    return d


def _transform_dict(obj: dict[bytes, Any]) -> dict[str, Any]:
    return {key.decode(): _transform_value(value) for key, value in obj.items()}


def _transform_value(v: Any) -> Any:
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return v
    if isinstance(v, dict):
        return _transform_dict(v)
    if isinstance(v, list):
        return [_transform_value(o) for o in v]
    return v


def _transform_torrent(obj: dict[bytes, Any]) -> dict[str, Any]:
    d = {}

    for key, value in obj.items():
        if key == b"info":
            d[key.decode()] = _transform_info(value)
        elif key in {b"created rd", b"piece layers"}:
            d[key.decode()] = value
        else:
            d[key.decode()] = _transform_value(value)

    return d


@dataclasses.dataclass(kw_only=True, slots=True)
class File:
    length: int
    path: Annotated[tuple[str, ...], annotated_types.MinLen(1)]

    @property
    def name(self) -> str:
        return self.path[-1]


@dataclasses.dataclass(kw_only=True, slots=False, frozen=True)
class TorrentInfo:
    name: Annotated[str, annotated_types.MinLen(1)]
    pieces: bytes
    length: int | None = None
    private: bool = False
    files: Annotated[tuple[File, ...], Field(default_factory=tuple)]
    piece_length: Annotated[int, Field(alias="piece length")]
    # commonly used field for private tracker
    source: str | None = None


@dataclasses.dataclass(kw_only=True, slots=False, frozen=True)
class Torrent:
    info: TorrentInfo

    @cached_property
    def total_length(self) -> int:
        return self.info.length or sum(x.length for x in self.info.files)

    def as_files(self) -> list[File]:
        if self.info.files:
            return list(self.info.files)
        return [File(length=cast(int, self.info.length), path=(self.info.name,))]


__t = pydantic.TypeAdapter(Torrent)


def parse_torrent(tc: bytes) -> Torrent:
    return __t.validate_python(_transform_torrent(bencode2.bdecode(tc)))


def find_largest_video_file(files: list[tuple[int, str, int]]) -> int | None:
    """Return the index of the largest video file, or None if no video file found.

    Args:
        files: list of (index, name, size) tuples.
    """
    best: tuple[int, int] | None = None  # (index, size)
    for index, name, size in files:
        if not name.lower().endswith(VIDEO_FILE_EXT):
            continue
        if best is None or size > best[1]:
            best = (index, size)
    return best[0] if best is not None else None
