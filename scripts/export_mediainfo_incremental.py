from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import opendal
import orjson
from tqdm import tqdm

from app._zstd import reader as zstd_reader
from app._zstd import writer as zstd_writer
from app.config import load_s3_config
from app.torrent_store import create_operator
from app.utils import human_readable_size

CACHE_DIR = Path("data/backups")
EXPORT_DIR = Path("data")


def find_all_backup_dates(op: opendal.Operator) -> list[date]:
    dates: set[date] = set()
    for entry in op.scan("backups/"):
        parts = entry.path.rstrip("/").split("/")
        if len(parts) >= 2:
            try:
                dates.add(date.fromisoformat(parts[1]))
            except ValueError:
                continue
    return sorted(dates, reverse=True)


def find_monthly_backup_dates(dates: list[date]) -> list[date]:
    return [d for d in dates if d.day == 1]


def find_baseline_date(
    all_dates: list[date],
    current: date,
) -> date | None:
    candidates = [d for d in all_dates if d < current]
    if not candidates:
        return None
    return candidates[0]


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
        chunks: list[bytes] = []
        with op.open(key, "rb") as f:  # type: ignore
            while chunk := f.read(chunk_size):
                chunks.append(chunk)
                bar.update(len(chunk))
    data = b"".join(chunks)
    print(f"downloaded {human_readable_size(len(data))}")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(data)
    print(f"cached to {cache_path}")
    return data


def iter_jsonl_lines(compressed: bytes) -> Iterator[bytes]:
    reader = zstd_reader(io.BytesIO(compressed))
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


def build_tid_index(compressed: bytes) -> dict[int, dict[str, Any]]:
    index: dict[int, dict[str, Any]] = {}
    for line in tqdm(iter_jsonl_lines(compressed), ascii=True):
        if not line.strip():
            continue
        row = orjson.loads(line)
        mediainfo = _get_mediainfo(row)
        if not mediainfo:
            continue
        if _is_api_mediainfo(row, mediainfo):
            continue
        index[row["tid"]] = {
            "id": row["tid"],
            "mediainfo": mediainfo,
            "hardcoded_subtitle": row["hard_coded_subtitle"],
        }
    return index


def find_incremental_entries(
    current: dict[int, dict[str, Any]],
    previous: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for tid, entry in current.items():
        if tid not in previous:
            result.append(entry)
        else:
            prev_entry = previous[tid]
            if (
                entry["mediainfo"] != prev_entry["mediainfo"]
                or entry["hardcoded_subtitle"] != prev_entry["hardcoded_subtitle"]
            ):
                result.append(entry)
    return result


def _get_mediainfo(row: dict[str, Any]) -> str:
    return row.get("mediainfo", "")


def _is_api_mediainfo(row: dict[str, Any], mediainfo: str) -> bool:
    api = row.get("api_mediainfo")
    if not api:
        return False
    return mediainfo == api


def write_export(entries: list[dict[str, Any]], output_path: Path) -> int:
    count = 0
    with output_path.open("wb") as f_out, zstd_writer(f_out) as w:
        for entry in entries:
            w.write(orjson.dumps(entry) + b"\n")
            count += 1
    return count


def main() -> None:
    op = create_operator(load_s3_config())

    all_dates = find_all_backup_dates(op)
    print(f"found {len(all_dates)} backup(s) in S3")

    monthly_dates = find_monthly_backup_dates(all_dates)
    if not monthly_dates:
        raise SystemExit("no monthly backups found in S3")
    print(
        f"found {len(monthly_dates)} monthly backup(s): "
        f"{', '.join(str(d) for d in monthly_dates[:3])}"
    )

    current = monthly_dates[0]

    baseline = find_baseline_date(all_dates, current)
    if baseline is None:
        entries = list(build_tid_index(download_backup(op, current)).values())
        output_path = EXPORT_DIR / f"mediainfo_export-{current}.jsonl.zst"
        count = write_export(entries, output_path)
        print(f"no prior backup found; exported full baseline ({count} entries) to {output_path}")
        return

    current_data = download_backup(op, current)
    print("indexing current backup...")
    current_index = build_tid_index(current_data)
    print(f"current backup ({current}) has {len(current_index)} entries with mediainfo")

    previous_data = download_backup(op, baseline)
    print("indexing baseline backup...")
    previous_index = build_tid_index(previous_data)
    print(f"baseline backup ({baseline}) has {len(previous_index)} entries with mediainfo")

    incremental = find_incremental_entries(current_index, previous_index)
    print(f"found {len(incremental)} new or changed entries (out of {len(current_index)} total)")

    output_path = EXPORT_DIR / f"mediainfo_incremental-{baseline}-to-{current}.jsonl.zst"
    count = write_export(incremental, output_path)
    print(f"exported {count} entries to {output_path}")


if __name__ == "__main__":
    main()
