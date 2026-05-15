from __future__ import annotations

from datetime import date
from pathlib import Path

import opendal
import orjson
import zstandard
from tqdm import tqdm

from app.config import load_s3_config
from app.torrent_store import create_operator
from app.utils import human_readable_size


def find_latest_backup_date(op: opendal.Operator) -> date:
    entries = op.scan("backups/")
    dates: list[date] = []
    for entry in entries:
        parts = entry.path.rstrip("/").split("/")
        if len(parts) >= 2:
            try:
                dates.append(date.fromisoformat(parts[1]))
            except ValueError:
                continue
    if not dates:
        raise SystemExit("no backups found in S3")
    return max(dates)


CACHE_DIR = Path("data/backups")


def download_backup(op: opendal.Operator, backup_date: date) -> bytes:
    key = f"backups/{backup_date}/thread.jsonl.zst"
    total_size = op.stat(key).content_length

    cache_path = CACHE_DIR / f"{backup_date}.jsonl.zst"
    if cache_path.exists():
        data = cache_path.read_bytes()
        if len(data) == total_size:
            print(f"using cached {cache_path} ({human_readable_size(len(data))})")
            return data

    print(f"downloading {key} ...")
    with tqdm(total=total_size, unit_scale=True, unit_divisor=1024, ascii=True) as bar:
        chunk_size = 1024 * 1024
        chunks = []
        with op.open(key, "rb") as f:
            while chunk := f.read(chunk_size):
                chunks.append(chunk)
                bar.update(len(chunk))
    data = b"".join(chunks)
    print(f"downloaded {human_readable_size(len(data))}")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(data)
    print(f"cached to {cache_path}")
    return data


def iter_jsonl_lines(compressed: bytes):
    dctx = zstandard.ZstdDecompressor()
    reader = dctx.stream_reader(compressed)
    buf = b""
    while True:
        chunk = reader.read(1024 * 1024)
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line_bytes, buf = buf.split(b"\n", 1)
            yield line_bytes
    if buf:
        yield buf


def main() -> None:
    op = create_operator(load_s3_config())

    backup_date = find_latest_backup_date(op)
    print(f"latest backup: {backup_date}")

    compressed = download_backup(op, backup_date)
    print("streaming decompression...")

    output_path = Path(f"data/mediainfo_export-{backup_date}.jsonl.zst")
    count = 0

    cctx = zstandard.ZstdCompressor(level=3)
    with output_path.open("wb") as f_out, cctx.stream_writer(f_out) as writer:
        for line in tqdm(iter_jsonl_lines(compressed), ascii=True):
            if not line.strip():
                continue
            row = orjson.loads(line)
            mediainfo = row["mediainfo"]
            if not mediainfo:
                continue
            entry = {
                "id": row["tid"],
                "mediainfo": mediainfo,
                "hardcoded_subtitle": row["hard_coded_subtitle"],
            }
            writer.write(orjson.dumps(entry) + b"\n")
            count += 1

    print(f"exported {count} threads to {output_path}")


if __name__ == "__main__":
    main()
