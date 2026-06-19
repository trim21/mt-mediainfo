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


def find_largest_video_file(files: list[File]) -> int | None:
    """Return the index of the largest video file, or None if no video file found."""
    best: tuple[int, int] | None = None  # (index, size)
    for i, f in enumerate(files):
        if not f.name.lower().endswith(VIDEO_FILE_EXT):
            continue
        if best is None or f.length > best[1]:
            best = (i, f.length)
    return best[0] if best is not None else None


BDMV_MARKERS = {"index.bdmv", "movieobject.bdmv"}


def is_bdmv(torrent: Torrent) -> bool:
    for f in torrent.as_files():
        if f.name.lower() in BDMV_MARKERS:
            return True
    return False


def is_bdmv_from_files(files: list[File]) -> bool:
    """Return True if any file matches BDMV structure markers."""
    return any(f.name.lower() in BDMV_MARKERS for f in files)


def _bdmv_parent(path: tuple[str, ...]) -> tuple[str, ...]:
    """Return the disc parent directory for a BDMV marker file path.

    Finds the BDMV component and returns everything before it, which is
    the actual disc root (e.g. 'Disc2') or () for a root-level BDMV.
    """
    for i, part in enumerate(path):
        if part.lower() == "bdmv":
            return path[:i]
    return path[:-2]


def _group_by_bdmv_dir(files: list[File]) -> dict[tuple[str, ...], list[int]]:
    """Group file indices by their BDMV disc parent directory."""
    parents: set[tuple[str, ...]] = set()
    for f in files:
        if f.name.lower() in BDMV_MARKERS:
            parents.add(_bdmv_parent(f.path))

    if not parents:
        return {}

    groups: dict[tuple[str, ...], list[int]] = {}
    for i, f in enumerate(files):
        for parent in parents:
            if f.path[: len(parent)] == parent:
                groups.setdefault(parent, []).append(i)
                break
    return groups


def _pick_best_bdmv_disc(
    files: list[File],
    groups: dict[tuple[str, ...], list[int]],
) -> tuple[str, ...]:
    """Return the parent directory of the largest BDMV disc, or () if none."""
    best_parent: tuple[str, ...] = ()
    best_size = -1
    for parent, indices in groups.items():
        total = sum(files[i].length for i in indices)
        if total > best_size:
            best_size = total
            best_parent = parent
    return best_parent


def pick_bdmv_selection(files: list[File]) -> tuple[int, list[int]]:
    """Pick the largest BDMV disc from a multi-disc torrent.

    Returns (selected_size, selected_index). If no BDMV disc found, returns (-1, []).
    """
    groups = _group_by_bdmv_dir(files)
    if not groups:
        return -1, []

    best_parent = _pick_best_bdmv_disc(files, groups)
    selected = groups[best_parent]
    total = sum(files[i].length for i in selected)
    return total, sorted(selected)


def bdmv_disc_path(files: list[File], save_path: str) -> str:
    """Return the filesystem path to the best (largest) BDMV disc directory."""
    groups = _group_by_bdmv_dir(files)
    best_parent = _pick_best_bdmv_disc(files, groups)
    if best_parent:
        return save_path + "/" + "/".join(best_parent)
    return save_path


def compute_selection(files: list[File]) -> tuple[int, list[int], int]:
    """Compute selected_size, selected_index, and priority for a normal torrent.

    Picks the largest video file. Returns (selected_size, selected_index, priority).
    """
    keep_idx = find_largest_video_file(files)
    selected_size = files[keep_idx].length if keep_idx is not None else -1
    selected_index = [keep_idx] if keep_idx is not None else []
    priority = 99 if len(files) == 1 else 0
    return selected_size, selected_index, priority


def compute_bdmv_selection(files: list[File]) -> tuple[int, list[int], int]:
    """Compute selected_size, selected_index, and priority for a BDMV torrent.

    Picks the largest disc. Returns (selected_size, selected_index, priority).
    """
    selected_size, selected_index = pick_bdmv_selection(files)
    priority = 99 if len(files) == 1 else 0
    return selected_size, selected_index, priority
