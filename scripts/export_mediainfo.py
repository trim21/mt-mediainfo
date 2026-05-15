from __future__ import annotations

import json
import math
import zipfile
from datetime import date
from pathlib import Path

import opendal
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


def decompress(data: bytes) -> bytes:
    dctx = zstandard.ZstdDecompressor()
    reader = dctx.stream_reader(data)
    chunks: list[bytes] = []
    while True:
        chunk = reader.read(1024 * 1024)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def bucket_dir(tid: int) -> str:
    up = math.ceil(tid / 1000) * 1000
    lower = up - 999
    return f"{lower}-{up}"


def main() -> None:
    op = create_operator(load_s3_config())

    backup_date = find_latest_backup_date(op)
    print(f"latest backup: {backup_date}")

    compressed = download_backup(op, backup_date)
    raw = decompress(compressed)
    print(f"decompressed {human_readable_size(len(raw))} bytes")

    output_path = Path("data/mediainfo_export.zip")
    count = 0

    with zipfile.ZipFile(
        output_path,
        "w",
        zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as zf:
        for line in raw.splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            mediainfo = row.get("mediainfo", "")
            if not mediainfo:
                continue
            tid = row["tid"]
            entry = {
                "id": tid,
                "mediainfo": mediainfo,
                "hardcoded_subtitle": bool(row.get("hard_coded_subtitle", False)),
            }
            arcname = f"{bucket_dir(tid)}/{tid}.json"
            zf.writestr(arcname, json.dumps(entry, ensure_ascii=False))
            count += 1

    print(f"exported {count} threads to {output_path}")


if __name__ == "__main__":
    main()
