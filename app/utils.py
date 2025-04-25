import functools
import hashlib
import shlex
import subprocess
import sys
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
