import functools
import hashlib
import shlex
import subprocess
from collections.abc import Hashable
from pathlib import Path
from shutil import which
from typing import IO, Any, Self

import orjson
from bencode2 import bdecode, bencode
from pydantic import TypeAdapter
from sslog import logger


def _format_subprocess_command(command: object) -> str:
    if isinstance(command, bytes):
        return command.decode("utf-8", errors="replace")
    if isinstance(command, str):
        return command
    if isinstance(command, (list, tuple)):
        return shlex.join([str(part) for part in command])
    return str(command)


def _format_subprocess_output(output: bytes | str | None) -> str:
    if output is None:
        return "<empty>"
    if isinstance(output, bytes):
        text = output.decode("utf-8", errors="replace")
    else:
        text = output
    if not text.strip():
        return "<empty>"
    return text.rstrip()


class CommandExecutionError(RuntimeError):
    command: str
    returncode: int
    stdout: str
    stderr: str

    def __init__(
        self,
        message: str,
        *,
        command: str,
        returncode: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            "\n".join([
                message,
                f"command: {command}",
                f"return code: {returncode}",
                f"stdout:\n{stdout}",
                f"stderr:\n{stderr}",
            ])
        )

    @classmethod
    def from_called_process_error(
        cls,
        message: str,
        error: subprocess.CalledProcessError,
    ) -> Self:
        return cls(
            message,
            command=_format_subprocess_command(error.cmd),
            returncode=error.returncode,
            stdout=_format_subprocess_output(error.stdout),
            stderr=_format_subprocess_output(error.stderr),
        )


def must_find_executable(executable: str) -> str:
    tool = which(executable)
    if tool is None:
        raise RuntimeError("can't find {e}")
    return tool


def must_run_command(
    executable: str,
    command: list[str],
    *,
    cwd: str | Path | None = None,
    capture_output: bool = False,
    stdout: int | IO[bytes] | IO[str] | None = None,
    stderr: int | IO[bytes] | IO[str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    logger.trace("executing command {!r}", shlex.join([executable, *command]))
    return subprocess.run(
        [executable, *command],
        check=True,
        cwd=cwd,
        capture_output=capture_output,
        stdout=stdout,
        stderr=stderr,
    )


def human_readable_size(size: float, decimal_places: int = 2) -> str:
    size = float(size)
    unit = "B"
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


def parse_obj[K](typ: type[K], value: Any, *, strict: bool | None = None) -> K:
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
