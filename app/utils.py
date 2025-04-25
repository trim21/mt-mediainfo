import functools
import hashlib
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Hashable
from pathlib import Path
from shutil import which
from typing import Any, TypeVar

import orjson
from bencode2 import bdecode, bencode
from loguru import logger
from pydantic import TypeAdapter


def must_find_executable(e: str) -> str:
    tool = which(e)
    if tool is None:
        raise Exception("can't find {e}")
    return tool


def run_command(
    command: list[str],
    cwd: str | Path | None = None,
    check: bool = False,
    stdout: int | None = None,
    stderr: int | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    logger.debug("executing command {!r}", shlex.join(command))
    return subprocess.run(command, **kwargs, cwd=cwd, stdout=stdout, stderr=stderr, check=check)


def must_run_command(
    executable: str,
    command: list[str],
    cwd: str | Path | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    cmd = which(executable)
    if cmd is None:
        logger.error("can't find {!r}", executable)
        sys.exit(1)
    logger.trace("executing command {!r}", shlex.join([cmd, *command]))
    return subprocess.run([cmd, *command], **kwargs, cwd=cwd)


def human_readable_size(size: float, decimal_places: int = 2) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


_T = TypeVar("_T")


@functools.cache
def get_type_adapter(t: type[_T]) -> TypeAdapter[_T]:
    return TypeAdapter(t)


_K = TypeVar("_K")


def parse_obj_as(typ: type[_K], value: Any, *, strict: bool | None = None) -> _K:
    t: TypeAdapter[_K] = get_type_adapter(typ)  # type: ignore[arg-type]
    return t.validate_python(value, strict=strict)


def parse_json_as(typ: type[_K], value: str | bytes, *, strict: bool | None = None) -> _K:
    t: TypeAdapter[_K] = get_type_adapter(typ)  # type: ignore[arg-type]
    return t.validate_python(orjson.loads(value), strict=strict)


def get_info_hash_v1_from_content(content: bytes) -> str:
    data = bdecode(content)
    enc = bencode(data[b"info"])
    return hashlib.sha1(enc).hexdigest()


_J = TypeVar("_J", bound=Hashable)


def dedupe(seq: list[_J]) -> list[_J]:
    seen: set[_J] = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def an2cn(i: int) -> str:
    match i:
        case 1:
            return "一"
        case 2:
            return "二"
        case 3:
            return "三"
        case 4:
            return "四"
        case 5:
            return "五"
        case 6:
            return "六"
        case 7:
            return "七"
        case 8:
            return "八"
        case 9:
            return "九"
        case 10:
            return "十"

    if i >= 100:
        raise NotImplementedError(f"an2cn({i!r})")

    if i < 20:
        return "十" + an2cn(i // 10)

    if i % 10 == 0:
        return an2cn(i // 10) + "十"

    return an2cn(i // 10) + "十" + an2cn(i % 10)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pt-repost") as d:
        generate_images(Path(sys.argv[1]), Path(d))
