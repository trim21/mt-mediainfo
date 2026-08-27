from datetime import UTC, datetime, timedelta

import pytest

from app.bin.server import (
    DownloadRemaining,
    DownloadThroughput,
    _build_download_eta,
    _build_download_eta_kind,
    _next_shanghai_midnight,
)
from app.const import TZ_SHANGHAI
from app.db.kv import kv_expires_at

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=TZ_SHANGHAI)


def test_kv_expires_at_absolute_and_relative() -> None:
    assert kv_expires_at() is None
    assert kv_expires_at(ex=NOW) == NOW
    assert kv_expires_at(ex=timedelta(hours=1), now=NOW) == NOW + timedelta(hours=1)
    assert kv_expires_at(ttl=timedelta(hours=1), now=NOW) == NOW + timedelta(hours=1)


def test_kv_expires_at_rejects_ttl_and_ex() -> None:
    with pytest.raises(TypeError, match="mutually exclusive"):
        kv_expires_at(ttl=timedelta(seconds=1), ex=timedelta(seconds=1))


def test_next_shanghai_midnight() -> None:
    utc = datetime(2026, 8, 26, 16, 0, tzinfo=UTC)
    assert _next_shanghai_midnight(utc) == datetime(2026, 8, 28, tzinfo=TZ_SHANGHAI)
    assert _next_shanghai_midnight(NOW) == datetime(2026, 8, 28, tzinfo=TZ_SHANGHAI)


def test_eta_done_when_nothing_remaining() -> None:
    row = _build_download_eta_kind(
        label="BDMV",
        remaining=DownloadRemaining(),
        throughput=DownloadThroughput(bytes=100, count=1),
        window_seconds=10,
        now=NOW,
    )
    assert row.eta_fmt == "done"
    assert row.finish_at_fmt == "-"
    assert row.byte_rate == 10.0


def test_eta_infinite_when_no_speed() -> None:
    row = _build_download_eta_kind(
        label="BDMV",
        remaining=DownloadRemaining(count=1, size=100),
        throughput=DownloadThroughput(),
        window_seconds=10,
        now=NOW,
    )
    assert row.eta_fmt == "∞"
    assert row.finish_at_fmt == "∞"


def test_eta_beyond_one_year_still_shows_finish_time() -> None:
    row = _build_download_eta_kind(
        label="BDMV",
        remaining=DownloadRemaining(count=1, size=400 * 86400),
        throughput=DownloadThroughput(bytes=1, count=1),
        window_seconds=1,
        now=NOW,
    )
    assert row.eta_fmt == "9600h"
    assert row.finish_at_fmt == "2027-10-01 12:00:00"


def test_eta_from_selected_size_byte_rate() -> None:
    row = _build_download_eta_kind(
        label="Non-BDMV",
        remaining=DownloadRemaining(count=2, size=50),
        throughput=DownloadThroughput(bytes=100, count=4),
        window_seconds=10,
        now=NOW,
    )
    assert row.byte_rate == 10.0
    assert row.eta_fmt == "5s"
    assert row.finish_at_fmt == "2026-08-27 12:00:05"


def test_build_download_eta_splits_bdmv() -> None:
    rows = _build_download_eta(
        bdmv_remaining=DownloadRemaining(count=2, size=50),
        other_remaining=DownloadRemaining(count=3, size=150),
        bdmv_throughput=DownloadThroughput(bytes=100, count=2),
        other_throughput=DownloadThroughput(bytes=300, count=6),
        window_seconds=10,
        now=NOW,
    )
    assert [r.label for r in rows] == ["BDMV", "Non-BDMV"]
    assert rows.bdmv.remaining_count == 2
    assert rows.bdmv.downloaded_bytes == 100
    assert rows.bdmv.downloaded_count == 2
    assert rows.other.remaining_count == 3
    assert rows.other.remaining_size == 150
    assert rows.other.downloaded_bytes == 300
    assert rows.other.downloaded_count == 6
