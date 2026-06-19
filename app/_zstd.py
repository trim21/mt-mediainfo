"""Thin wrapper around ``zstandard`` with a simpler streaming API."""

from typing import IO, Protocol, Self, cast

import zstandard

ZSTD_LEVEL = 3


class _Writable(Protocol):
    def write(self, data: bytes, /) -> object: ...


class _Readable(Protocol):
    def read(self, size: int = ...) -> bytes: ...


def writer(dst: _Writable, level: int = ZSTD_LEVEL) -> _ZstdWriter:
    """Return a write-compress wrapper object."""
    return _ZstdWriter(dst, level)


def reader(src: _Readable) -> _ZstdReader:
    """Return a read-decompress wrapper object."""
    return _ZstdReader(src)


class _ZstdWriter:
    def __init__(self, dst: _Writable, level: int = ZSTD_LEVEL) -> None:
        self._dst = dst
        self._cctx = zstandard.ZstdCompressor(level=level)
        self._comp = self._cctx.compressobj()

    def write(self, data: bytes) -> int:
        self._dst.write(self._comp.compress(data))
        return len(data)

    def flush(self) -> None:
        remaining = self._comp.flush()
        if remaining:
            self._dst.write(remaining)

    def close(self) -> None:
        self.flush()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class _ZstdReader:
    def __init__(self, src: _Readable) -> None:
        self._reader = zstandard.ZstdDecompressor().stream_reader(cast(IO[bytes], src))

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return self._reader.read()
        return self._reader.read(size)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass
