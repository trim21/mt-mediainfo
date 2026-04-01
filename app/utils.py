import functools
import hashlib
import shlex
import subprocess
from collections.abc import Hashable
from pathlib import Path
from shutil import which
from typing import Any

import orjson
from bencode2 import bdecode, bencode
from pydantic import TypeAdapter
from sslog import logger


def must_find_executable(executable: str) -> str:
    tool = which(executable)
    if tool is None:
        raise RuntimeError("can't find {e}")
    return tool


def must_run_command(
    executable: str,
    command: list[str],
    cwd: str | Path | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    logger.trace("executing command {!r}", shlex.join([executable, *command]))
    return subprocess.run([executable, *command], **kwargs, cwd=cwd)


def human_readable_size(size: float, decimal_places: int = 2) -> str:
    size = float(size)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def human_readable_byte_rate(bytes_per_second: float, decimal_places: int = 2) -> str:
    return human_readable_size(bytes_per_second, decimal_places) + "/s"


@functools.cache
def get_type_adapter[T](t: type[T]) -> TypeAdapter[T]:
    return TypeAdapter(t)


def parse_obj_as[K](typ: type[K], value: Any, *, strict: bool | None = None) -> K:
    t: TypeAdapter[K] = get_type_adapter(typ)  # type: ignore[arg-type]
    return t.validate_python(value, strict=strict)


def parse_json_as[T](typ: type[T], value: str | bytes, *, strict: bool | None = None) -> T:
    t: TypeAdapter[T] = get_type_adapter(typ)  # type: ignore[arg-type]
    return t.validate_python(orjson.loads(value), strict=strict)


def get_info_hash_v1_from_content(content: bytes) -> str:
    data = bdecode(content)
    enc = bencode(data[b"info"])
    return hashlib.sha1(enc).hexdigest()


def set_torrent_comment(content: bytes, comment: str) -> bytes:
    data = bdecode(content)
    data[b"comment"] = comment.encode()
    return bencode(data)


def dedupe[J: Hashable](seq: list[J]) -> list[J]:
    seen: set[J] = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
