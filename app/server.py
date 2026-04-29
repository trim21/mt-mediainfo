from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from operator import itemgetter
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol, cast
from zoneinfo import ZoneInfo

import asyncpg
import durationpy
import fastapi
import orjson
from fastapi import Depends, Query, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, JSONResponse

from app.config import ServerConfig, load_server_config
from app.const import (
    PRIORITY_CATEGORY,
    SELECTED_CATEGORY,
    ItemStatus,
    PickStrategy,
)
from app.db import Database
from app.rpc import PAYLOAD_TYPES, RpcRequest, enqueue_command
from app.utils import human_readable_byte_rate, human_readable_size, parse_obj


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_INDENT_2, default=str)


_tz_shanghai = ZoneInfo("Asia/Shanghai")

templates = Jinja2Templates(directory=str(Path(__file__).parent.joinpath("templates").resolve()))


def _fmt_eta(seconds: float) -> str:
    if seconds <= 0 or seconds > 365 * 24 * 3600:
        return "∞"
    return durationpy.to_str(timedelta(seconds=int(seconds)))


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.astimezone(_tz_shanghai).strftime("%Y-%m-%d %H:%M:%S")


def _timeago(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    delta = datetime.now(tz=_tz_shanghai) - dt.astimezone(_tz_shanghai)
    seconds = delta.total_seconds()
    if seconds < 0:
        return "-"
    return _fmt_eta(seconds) + " ago"


templates.env.filters["fmt_dt"] = _fmt_dt
templates.env.filters["timeago"] = _timeago


@dataclass(slots=True, frozen=True)
class NodeDownloadStat:
    downloaded_bytes: int
    count: int


@dataclass(slots=True, frozen=True)
class DailyStatsSnapshot:
    day: date
    downloaded_bytes: int
    downloaded_count: int
    fetched_bytes: int
    fetched_count: int
    thread_count: int
    torrent_count: int
    mediainfo_count: int
    node_downloaded: dict[str, NodeDownloadStat]

    @classmethod
    def from_record(cls, record: asyncpg.Record) -> DailyStatsSnapshot:
        node_downloaded_raw = cast(dict[str, dict[str, Any]], record["node_downloaded"] or {})
        return cls(
            day=record["day"],
            downloaded_bytes=int(record["downloaded_bytes"]),
            downloaded_count=int(record["downloaded_count"]),
            fetched_bytes=int(record["fetched_bytes"]),
            fetched_count=int(record["fetched_count"]),
            thread_count=int(record["thread_count"]),
            torrent_count=int(record["torrent_count"]),
            mediainfo_count=int(record["mediainfo_count"]),
            node_downloaded={
                node_id: NodeDownloadStat(
                    downloaded_bytes=int(node_stats.get("bytes", 0)),
                    count=int(node_stats.get("count", 0)),
                )
                for node_id, node_stats in node_downloaded_raw.items()
            },
        )


@dataclass(slots=True, frozen=True)
class DailyStat:
    day: date
    downloaded_bytes: int
    downloaded_count: int
    downloaded_byte_rate: float
    fetched_bytes: int
    fetched_count: int
    fetched_byte_rate: float
    thread_count: int
    torrent_count: int
    mediainfo_count: int
    node_downloaded: dict[str, NodeDownloadStat]
    node_downloaded_byte_rate: dict[str, float]

    @classmethod
    def from_snapshot(
        cls,
        snapshot: DailyStatsSnapshot,
        *,
        period_seconds: float = 86400.0,
        fetched_byte_rate: float | None = None,
    ) -> DailyStat:
        if fetched_byte_rate is None:
            fetched_byte_rate = snapshot.fetched_bytes / 86400.0 if snapshot.fetched_bytes else 0.0

        return cls(
            day=snapshot.day,
            downloaded_bytes=snapshot.downloaded_bytes,
            downloaded_count=snapshot.downloaded_count,
            downloaded_byte_rate=snapshot.downloaded_bytes / period_seconds,
            fetched_bytes=snapshot.fetched_bytes,
            fetched_count=snapshot.fetched_count,
            fetched_byte_rate=fetched_byte_rate,
            thread_count=snapshot.thread_count,
            torrent_count=snapshot.torrent_count,
            mediainfo_count=snapshot.mediainfo_count,
            node_downloaded=snapshot.node_downloaded,
            node_downloaded_byte_rate={
                node_id: node_stats.downloaded_bytes / period_seconds
                for node_id, node_stats in snapshot.node_downloaded.items()
            },
        )


def _project_today_fetched_byte_rate(
    snapshot: DailyStatsSnapshot,
    *,
    projected_fetches_per_day: int = 900,
) -> float:
    if snapshot.fetched_count == 0:
        return 0.0
    return (snapshot.fetched_bytes / snapshot.fetched_count) * projected_fetches_per_day / 86400.0


@dataclass(slots=True, frozen=True)
class ByteRateTotal:
    byte_rate: float
    byte_rate_fmt: str
    total_size: int
    total_size_fmt: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "byte_rate": self.byte_rate,
            "byte_rate_fmt": self.byte_rate_fmt,
            "total_size": self.total_size,
            "total_size_fmt": self.total_size_fmt,
        }


@dataclass(slots=True, frozen=True)
class LabeledByteRate:
    label: str
    byte_rate: float

    def to_dict(self, *, label_key: str) -> dict[str, Any]:
        return {label_key: self.label, "byte_rate": self.byte_rate}


@dataclass(slots=True, frozen=True)
class LabeledCount:
    label: str
    count: int

    def to_dict(self, *, label_key: str) -> dict[str, Any]:
        return {label_key: self.label, "count": self.count}


@dataclass(slots=True, frozen=True)
class ByteRateChart:
    labels: list[str]
    totals: list[ByteRateTotal]
    per_node: dict[str, list[float]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "labels": self.labels,
            "totals": [point.to_dict() for point in self.totals],
            "per_node": self.per_node,
        }


@dataclass(slots=True, frozen=True)
class DoneCountChart:
    labels: list[str]
    totals: list[int]
    per_node: dict[str, list[int]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "labels": self.labels,
            "totals": self.totals,
            "per_node": self.per_node,
        }


@dataclass(slots=True, frozen=True)
class DailyCharts:
    daily_byte_rate: ByteRateChart
    daily_fetched_size: list[LabeledByteRate]
    daily_thread_count: list[LabeledCount]
    daily_torrent_count: list[LabeledCount]
    daily_done_count: DoneCountChart
    daily_mediainfo_count: list[LabeledCount]

    def to_context(self) -> dict[str, Any]:
        return {
            "daily_byte_rate": self.daily_byte_rate.to_dict(),
            "daily_fetched_size": [
                point.to_dict(label_key="day") for point in self.daily_fetched_size
            ],
            "daily_thread_count": [
                point.to_dict(label_key="day") for point in self.daily_thread_count
            ],
            "daily_torrent_count": [
                point.to_dict(label_key="day") for point in self.daily_torrent_count
            ],
            "daily_done_count": self.daily_done_count.to_dict(),
            "daily_mediainfo_count": [
                point.to_dict(label_key="day") for point in self.daily_mediainfo_count
            ],
        }


@dataclass(slots=True, frozen=True)
class WeeklyCharts:
    weekly_byte_rate: ByteRateChart
    weekly_fetched_size: list[LabeledByteRate]
    weekly_thread_count: list[LabeledCount]
    weekly_torrent_count: list[LabeledCount]
    weekly_done_count: DoneCountChart
    weekly_mediainfo_count: list[LabeledCount]

    def to_payload(self) -> dict[str, Any]:
        return {
            "weekly_byte_rate": self.weekly_byte_rate.to_dict(),
            "weekly_fetched_size": [
                point.to_dict(label_key="week") for point in self.weekly_fetched_size
            ],
            "weekly_thread_count": [
                point.to_dict(label_key="week") for point in self.weekly_thread_count
            ],
            "weekly_torrent_count": [
                point.to_dict(label_key="week") for point in self.weekly_torrent_count
            ],
            "weekly_done_count": self.weekly_done_count.to_dict(),
            "weekly_mediainfo_count": [
                point.to_dict(label_key="week") for point in self.weekly_mediainfo_count
            ],
        }


class _Render(Protocol):
    def __call__(
        self,
        name: str,
        ctx: dict[str, Any] | None = ...,
        status_code: int = ...,
        headers: Mapping[str, str] | None = ...,
        media_type: str | None = ...,
    ) -> HTMLResponse: ...


async def __render(request: Request) -> _Render:
    def render(
        name: str,
        ctx: dict[str, Any] | None = None,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
    ) -> HTMLResponse:
        return templates.TemplateResponse(
            name=name,
            request=request,
            context=ctx,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
        )

    return render


type Render = Annotated[_Render, Depends(__render)]


PAGE_SIZE = 100


@dataclasses.dataclass(frozen=True, kw_only=True)
class ConfigUpsertRequest:
    key: str
    value: str


def _pagination(page: int, total_count: int) -> dict[str, int | bool | None]:
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    current_page = min(max(page, 1), total_pages)
    return {
        "page": current_page,
        "page_size": PAGE_SIZE,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": current_page > 1,
        "has_next": current_page < total_pages,
        "prev_page": current_page - 1 if current_page > 1 else None,
        "next_page": current_page + 1 if current_page < total_pages else None,
        "offset": (current_page - 1) * PAGE_SIZE,
    }


def _thread_row(r: asyncpg.Record, *, show_failed_reason: bool) -> dict[str, Any]:
    row = dict(r) | {
        "size_fmt": human_readable_size(r["size"]),
        "selected_size_fmt": human_readable_size(r["selected_size"])
        if r["selected_size"] > 0
        else "-",
    }
    if show_failed_reason:
        failed_reason: str = r["failed_reason"]
        has_details = "\n" in failed_reason
        row["failed_reason_preview"] = (
            failed_reason.partition("\n")[0] if has_details else failed_reason
        ) or "-"
        row["failed_reason_has_details"] = has_details
    return row


def _today_start() -> datetime:
    now = datetime.now(_tz_shanghai)
    return datetime(now.year, now.month, now.day, tzinfo=_tz_shanghai)


def _week_range() -> tuple[int, int]:
    return -12, -1


def _week_label(today: datetime, week_num: int) -> str:
    ref = today + timedelta(days=1)
    start = ref + timedelta(weeks=int(week_num))
    return start.strftime("%Y-%m-%d")


def _build_history_daily_stats(history_stats: list[DailyStatsSnapshot]) -> list[DailyStat]:
    return [DailyStat.from_snapshot(stats) for stats in history_stats]


def _build_today_daily_stat(today_stats: DailyStatsSnapshot) -> DailyStat:
    elapsed_today = max((datetime.now(_tz_shanghai) - _today_start()).total_seconds(), 1.0)
    return DailyStat.from_snapshot(
        today_stats,
        period_seconds=elapsed_today,
        fetched_byte_rate=_project_today_fetched_byte_rate(today_stats),
    )


def _combine_daily_stats(
    history_daily_stats: list[DailyStat],
    today_daily_stat: DailyStat,
) -> list[DailyStat]:
    daily_stats = {stats.day: stats for stats in history_daily_stats}
    daily_stats[today_daily_stat.day] = today_daily_stat
    return [daily_stats[day] for day in sorted(daily_stats)]


def _build_weekly_charts(
    daily_stats: list[DailyStat],
) -> WeeklyCharts:
    days_per_week = 7.0
    today_date = max((s.day for s in daily_stats), default=_today_start().date())
    today = datetime(today_date.year, today_date.month, today_date.day, tzinfo=_tz_shanghai)
    ref_date = today_date + timedelta(days=1)
    min_week, max_week = _week_range()

    week_days: dict[int, list[DailyStat]] = {w: [] for w in range(min_week, max_week + 1)}
    for s in daily_stats:
        d = s.day
        wn = (d - ref_date).days // 7
        if min_week <= wn <= max_week:
            week_days[wn].append(s)

    all_node_ids: set[str] = set()
    for s in daily_stats:
        all_node_ids.update(s.node_downloaded.keys())
    sorted_node_ids = sorted(all_node_ids)

    labels: list[str] = []
    byte_rate_totals: list[ByteRateTotal] = []
    done_count_totals: list[int] = []
    fetched_data: list[LabeledByteRate] = []
    thread_count_data: list[LabeledCount] = []
    torrent_count_data: list[LabeledCount] = []
    mediainfo_count_data: list[LabeledCount] = []

    byte_rate_per_node: dict[str, list[float]] = {nid: [] for nid in sorted_node_ids}
    done_count_per_node: dict[str, list[int]] = {nid: [] for nid in sorted_node_ids}

    for wn in range(min_week, max_week + 1):
        label = _week_label(today, wn)
        labels.append(label)
        days = week_days[wn]

        total_dl_bytes = sum(s.downloaded_bytes for s in days)
        total_dl_count = sum(s.downloaded_count for s in days)
        byte_rate = sum(s.downloaded_byte_rate for s in days) / days_per_week
        fetched_rate = sum(s.fetched_byte_rate for s in days) / days_per_week

        byte_rate_totals.append(
            ByteRateTotal(
                byte_rate=byte_rate,
                byte_rate_fmt=human_readable_byte_rate(byte_rate),
                total_size=int(total_dl_bytes),
                total_size_fmt=human_readable_size(total_dl_bytes),
            )
        )
        done_count_totals.append(total_dl_count)

        fetched_data.append(LabeledByteRate(label=label, byte_rate=fetched_rate))

        thread_count_data.append(LabeledCount(label=label, count=sum(s.thread_count for s in days)))
        torrent_count_data.append(
            LabeledCount(label=label, count=sum(s.torrent_count for s in days))
        )
        mediainfo_count_data.append(
            LabeledCount(label=label, count=sum(s.mediainfo_count for s in days))
        )

        node_totals: dict[str, dict[str, int]] = {}
        for s in days:
            for nid, nd in s.node_downloaded.items():
                if nid not in node_totals:
                    node_totals[nid] = {"bytes": 0, "count": 0}
                node_totals[nid]["bytes"] += nd.downloaded_bytes
                node_totals[nid]["count"] += nd.count

        for nid in sorted_node_ids:
            nt = node_totals.get(nid, {"bytes": 0, "count": 0})
            byte_rate_per_node[nid].append(
                sum(s.node_downloaded_byte_rate.get(nid, 0.0) for s in days) / days_per_week
            )
            done_count_per_node[nid].append(nt["count"])

    return WeeklyCharts(
        weekly_byte_rate=ByteRateChart(
            labels=labels,
            totals=byte_rate_totals,
            per_node=byte_rate_per_node,
        ),
        weekly_fetched_size=fetched_data,
        weekly_thread_count=thread_count_data,
        weekly_torrent_count=torrent_count_data,
        weekly_done_count=DoneCountChart(
            labels=labels,
            totals=done_count_totals,
            per_node=done_count_per_node,
        ),
        weekly_mediainfo_count=mediainfo_count_data,
    )


def _build_daily_charts(
    daily_stats: list[DailyStat],
    start_date: date,
    end_date: date,
    known_node_ids: set[str] | None = None,
) -> DailyCharts:
    by_day: dict[date, DailyStat] = {s.day: s for s in daily_stats}

    all_node_ids: set[str] = set(known_node_ids) if known_node_ids else set()
    for v in daily_stats:
        all_node_ids.update(v.node_downloaded.keys())
    sorted_node_ids = sorted(all_node_ids)

    labels: list[str] = []
    byte_rate_totals: list[ByteRateTotal] = []
    done_count_totals: list[int] = []
    fetched_data: list[LabeledByteRate] = []
    thread_count_data: list[LabeledCount] = []
    torrent_count_data: list[LabeledCount] = []
    mediainfo_count_data: list[LabeledCount] = []

    byte_rate_per_node: dict[str, list[float]] = {nid: [] for nid in sorted_node_ids}
    done_count_per_node: dict[str, list[int]] = {nid: [] for nid in sorted_node_ids}

    for i in range((end_date - start_date).days + 1):
        d = start_date + timedelta(days=i)
        label = d.strftime("%Y-%m-%d")
        labels.append(label)
        s = by_day.get(d)

        if s is None:
            byte_rate_totals.append(
                ByteRateTotal(
                    byte_rate=0.0,
                    byte_rate_fmt=human_readable_byte_rate(0),
                    total_size=0,
                    total_size_fmt=human_readable_size(0),
                )
            )
            done_count_totals.append(0)
            fetched_data.append(LabeledByteRate(label=label, byte_rate=0.0))
            thread_count_data.append(LabeledCount(label=label, count=0))
            torrent_count_data.append(LabeledCount(label=label, count=0))
            mediainfo_count_data.append(LabeledCount(label=label, count=0))
            for nid in sorted_node_ids:
                byte_rate_per_node[nid].append(0.0)
                done_count_per_node[nid].append(0)
            continue

        dl_bytes = s.downloaded_bytes
        byte_rate = s.downloaded_byte_rate
        byte_rate_totals.append(
            ByteRateTotal(
                byte_rate=byte_rate,
                byte_rate_fmt=human_readable_byte_rate(byte_rate),
                total_size=int(dl_bytes),
                total_size_fmt=human_readable_size(dl_bytes),
            )
        )
        done_count_totals.append(s.downloaded_count)

        fetched_data.append(LabeledByteRate(label=label, byte_rate=s.fetched_byte_rate))

        thread_count_data.append(LabeledCount(label=label, count=s.thread_count))
        torrent_count_data.append(LabeledCount(label=label, count=s.torrent_count))
        mediainfo_count_data.append(LabeledCount(label=label, count=s.mediainfo_count))

        node_byte_rates = s.node_downloaded_byte_rate
        for nid in sorted_node_ids:
            byte_rate_per_node[nid].append(node_byte_rates.get(nid, 0.0))
            n = s.node_downloaded.get(nid)
            done_count_per_node[nid].append(0 if n is None else n.count)

    return DailyCharts(
        daily_byte_rate=ByteRateChart(
            labels=labels,
            totals=byte_rate_totals,
            per_node=byte_rate_per_node,
        ),
        daily_fetched_size=fetched_data,
        daily_thread_count=thread_count_data,
        daily_torrent_count=torrent_count_data,
        daily_done_count=DoneCountChart(
            labels=labels,
            totals=done_count_totals,
            per_node=done_count_per_node,
        ),
        daily_mediainfo_count=mediainfo_count_data,
    )


def create_app() -> fastapi.FastAPI:
    cfg: ServerConfig = load_server_config()

    async def _init_connection(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb",
            encoder=lambda v: orjson.dumps(v).decode(),
            decoder=orjson.loads,
            schema="pg_catalog",
        )

    pool = asyncpg.create_pool(cfg.pg_dsn(), init=_init_connection)

    @asynccontextmanager
    async def lifespan(_app: fastapi.FastAPI) -> AsyncGenerator[None, None]:
        await pool
        with Database(cfg.pg_dsn()) as migration_db:
            await asyncio.to_thread(migration_db.run_migrations)
        yield
        await pool.close()

    app = fastapi.FastAPI(debug=True, lifespan=lifespan)

    async def _render_thread_list(
        render: Render,
        *,
        title: str,
        count_sql: str,
        rows_sql: str,
        params: list[Any],
        page: int,
        show_progress: bool,
        show_failed_reason: bool,
        show_reset: bool = False,
        show_reset_all: bool = False,
        extra_ctx: dict[str, Any] | None = None,
        count_params: list[Any] | None = None,
    ) -> HTMLResponse:
        total_count = cast(
            int,
            await pool.fetchval(count_sql, *(count_params if count_params is not None else params))
            or 0,
        )
        pager = _pagination(page, total_count)
        rows = await pool.fetch(rows_sql, *params, pager["page_size"], pager["offset"])
        ctx = {
            "title": title,
            "show_progress": show_progress,
            "show_failed_reason": show_failed_reason,
            "show_reset": show_reset,
            "show_reset_all": show_reset_all,
            "threads": [_thread_row(r, show_failed_reason=show_failed_reason) for r in rows],
            "page": pager["page"],
            "page_size": pager["page_size"],
            "total_count": pager["total_count"],
            "total_pages": pager["total_pages"],
            "has_prev": pager["has_prev"],
            "has_next": pager["has_next"],
            "prev_page": pager["prev_page"],
            "next_page": pager["next_page"],
        }
        if extra_ctx:
            ctx.update(extra_ctx)
        return render("threads.html.j2", ctx=ctx)

    _backfill_lock = asyncio.Lock()

    async def _backfill_daily_stats(since: date) -> None:
        async with _backfill_lock:
            today_date = _today_start().date()
            yesterday = today_date - timedelta(days=1)
            start_date = since if since < today_date else yesterday

            if start_date > yesterday:
                return

            target_days = [
                start_date + timedelta(days=i) for i in range((yesterday - start_date).days + 1)
            ]
            if not target_days:
                return

            first_missing = target_days[0]
            last_missing = target_days[-1]
            start_ts = datetime(
                first_missing.year, first_missing.month, first_missing.day, tzinfo=_tz_shanghai
            )
            end_ts = datetime(
                last_missing.year, last_missing.month, last_missing.day, tzinfo=_tz_shanghai
            ) + timedelta(days=1)

            (
                downloaded_rows,
                fetched_rows,
                thread_rows,
                mediainfo_rows,
            ) = await asyncio.gather(
                pool.fetch(
                    """
                    select (coalesce(job.completed_at, job.updated_at)
                            at time zone 'Asia/Shanghai')::date as day,
                           job.node_id::text as node_id,
                           count(1)::int as count,
                           coalesce(sum(thread.selected_size), 0)::int8 as bytes
                    from job
                    join thread on (thread.tid = job.tid)
                    where job.status = $1 and thread.selected_size > 0
                      and coalesce(job.completed_at, job.updated_at) >= $2
                      and coalesce(job.completed_at, job.updated_at) < $3
                    group by day, job.node_id
                    """,
                    ItemStatus.DONE,
                    start_ts,
                    end_ts,
                ),
                pool.fetch(
                    """
                    select (torrent_fetched_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count,
                           coalesce(sum(selected_size), 0)::int8 as bytes
                    from thread
                    where torrent_fetched_at >= $1 and torrent_fetched_at < $2
                      and selected_size > 0 and category = any($3)
                    group by day
                    """,
                    start_ts,
                    end_ts,
                    SELECTED_CATEGORY,
                ),
                pool.fetch(
                    """
                    select (created_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count
                    from thread
                    where created_at >= $1 and created_at < $2
                    group by day
                    """,
                    start_ts,
                    end_ts,
                ),
                pool.fetch(
                    """
                    select (mediainfo_at at time zone 'Asia/Shanghai')::date as day,
                           count(1)::int as count
                    from thread
                    where mediainfo_at >= $1 and mediainfo_at < $2
                    group by day
                    """,
                    start_ts,
                    end_ts,
                ),
            )

            days_data: dict[date, dict[str, Any]] = {}

            def _ensure_day(d: date) -> dict[str, Any]:
                if d not in days_data:
                    days_data[d] = {
                        "downloaded_bytes": 0,
                        "downloaded_count": 0,
                        "fetched_bytes": 0,
                        "fetched_count": 0,
                        "thread_count": 0,
                        "torrent_count": 0,
                        "mediainfo_count": 0,
                        "node_downloaded": {},
                    }
                return days_data[d]

            for r in downloaded_rows:
                d = _ensure_day(r["day"])
                d["downloaded_bytes"] += r["bytes"]
                d["downloaded_count"] += r["count"]
                d["node_downloaded"][r["node_id"]] = {
                    "bytes": int(r["bytes"]),
                    "count": int(r["count"]),
                }

            for r in fetched_rows:
                d = _ensure_day(r["day"])
                d["fetched_bytes"] = int(r["bytes"])
                d["fetched_count"] = int(r["count"])
                d["torrent_count"] = int(r["count"])

            for r in thread_rows:
                _ensure_day(r["day"])["thread_count"] = int(r["count"])

            for r in mediainfo_rows:
                _ensure_day(r["day"])["mediainfo_count"] = int(r["count"])

            rows_to_insert: list[tuple[Any, ...]] = []
            for current in target_days:
                data = days_data.get(current)
                if data:
                    rows_to_insert.append((
                        current,
                        data["downloaded_bytes"],
                        data["downloaded_count"],
                        data["fetched_bytes"],
                        data["fetched_count"],
                        data["thread_count"],
                        data["torrent_count"],
                        data["mediainfo_count"],
                        data["node_downloaded"],
                    ))
                else:
                    rows_to_insert.append((current, 0, 0, 0, 0, 0, 0, 0, {}))

            if rows_to_insert:
                await pool.executemany(
                    """
                    insert into daily_stats
                        (day, downloaded_bytes, downloaded_count,
                         fetched_bytes, fetched_count, thread_count,
                         torrent_count, mediainfo_count, node_downloaded)
                    values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                    on conflict (day) do update set
                        downloaded_bytes = excluded.downloaded_bytes,
                        downloaded_count = excluded.downloaded_count,
                        fetched_bytes = excluded.fetched_bytes,
                        fetched_count = excluded.fetched_count,
                        thread_count = excluded.thread_count,
                        torrent_count = excluded.torrent_count,
                        mediainfo_count = excluded.mediainfo_count,
                        node_downloaded = excluded.node_downloaded
                    """,
                    rows_to_insert,
                )

    async def _compute_today_stats() -> DailyStatsSnapshot:
        today = _today_start()
        tomorrow = today + timedelta(days=1)

        (
            downloaded_rows,
            fetched_row,
            thread_count,
            mediainfo_count,
        ) = await asyncio.gather(
            pool.fetch(
                """
                select job.node_id::text as node_id,
                       count(1)::int as count,
                       coalesce(sum(thread.selected_size), 0)::int8 as bytes
                from job
                join thread on (thread.tid = job.tid)
                where job.status = $1 and thread.selected_size > 0
                  and coalesce(job.completed_at, job.updated_at) >= $2
                  and coalesce(job.completed_at, job.updated_at) < $3
                group by job.node_id
                """,
                ItemStatus.DONE,
                today,
                tomorrow,
            ),
            pool.fetchrow(
                """
                select count(1)::int as count,
                       coalesce(sum(selected_size), 0)::int8 as bytes
                from thread
                where torrent_fetched_at >= $1 and torrent_fetched_at < $2
                  and selected_size > 0 and category = any($3)
                """,
                today,
                tomorrow,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                "select count(1)::int from thread where created_at >= $1 and created_at < $2",
                today,
                tomorrow,
            ),
            pool.fetchval(
                "select count(1)::int from thread where mediainfo_at >= $1 and mediainfo_at < $2",
                today,
                tomorrow,
            ),
        )

        node_downloaded: dict[str, NodeDownloadStat] = {}
        total_downloaded_bytes = 0
        total_downloaded_count = 0
        for r in downloaded_rows:
            b = int(r["bytes"])
            c = int(r["count"])
            total_downloaded_bytes += b
            total_downloaded_count += c
            node_downloaded[r["node_id"]] = NodeDownloadStat(downloaded_bytes=b, count=c)

        fetched_row = cast(asyncpg.Record, fetched_row)

        return DailyStatsSnapshot(
            day=today.date(),
            downloaded_bytes=total_downloaded_bytes,
            downloaded_count=total_downloaded_count,
            fetched_bytes=int(fetched_row["bytes"]),
            fetched_count=int(fetched_row["count"]),
            thread_count=int(thread_count),
            torrent_count=int(fetched_row["count"]),
            mediainfo_count=int(mediainfo_count),
            node_downloaded=node_downloaded,
        )

    @app.get("/")
    async def progress(render: Render) -> HTMLResponse:
        (
            thread_stats,
            search_cursor,
            pending_download_stats,
            job_status_rows,
            downloading_node_rows,
            done_node_rows,
        ) = await asyncio.gather(
            pool.fetchrow(
                """
            select
              count(1) as scraped_total,
              count(1) filter (where category = any($1)) as total,
              coalesce(sum(selected_size) filter (where category = any($1)), 0) as total_size,
              count(1) filter (where deleted = false and mediainfo_at is null
                                and seeders != 0 and upload_at >= '2024-01-01' and category = any($1)) as pending_fetch_mediainfo,
              count(1) filter (where deleted = false and mediainfo_at is not null
                                and mediainfo = '' and info_hash = '' and torrent_invalid = '' and seeders != 0 and category = any($1)) as pending_fetch_torrent,
              count(1) filter (where mediainfo != '' and info_hash != ''
                and category = any($1)) as done,
              coalesce(sum(selected_size) filter (where mediainfo != '' and info_hash != ''
                and category = any($1)), 0) as done_size
            from thread
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval("select value from config where key = 'search_cursor'"),
            pool.fetchrow(
                """
            select count(1) as count, coalesce(sum(thread.selected_size), 0) as size
            from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetch(
                """
            select job.status,
                   count(1)::int as count,
                   coalesce(sum(thread.selected_size), 0)::int8 as size
            from job
            join thread on (thread.tid = job.tid)
            where thread.category = any($1)
              and job.status = any($2)
            group by job.status
            """,
                SELECTED_CATEGORY,
                [
                    ItemStatus.DOWNLOADING,
                    ItemStatus.DONE,
                    ItemStatus.FAILED,
                    ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
                ],
            ),
            pool.fetch(
                """
            select job.node_id,
                   count(1)::int as count,
                   coalesce(sum(thread.selected_size), 0)::int8 as size
            from job
            join thread on (thread.tid = job.tid)
            where thread.category = any($1)
              and job.status = $2
            group by job.node_id
            order by job.node_id
            """,
                SELECTED_CATEGORY,
                ItemStatus.DOWNLOADING,
            ),
            pool.fetch(
                """
            select job.node_id,
                   count(1)::int as count,
                   coalesce(sum(thread.selected_size), 0)::int8 as size
            from job
            join thread on (thread.tid = job.tid)
            where thread.category = any($1)
              and job.status = $2
            group by job.node_id
            order by job.node_id
            """,
                SELECTED_CATEGORY,
                ItemStatus.DONE,
            ),
        )

        thread_stats = cast(asyncpg.Record, thread_stats)
        scraped_total = cast(int, thread_stats["scraped_total"])
        total = cast(int, thread_stats["total"])
        total_size = cast(int, thread_stats["total_size"])
        pending_fetch_mediainfo = cast(int, thread_stats["pending_fetch_mediainfo"])
        pending_fetch_torrent = cast(int, thread_stats["pending_fetch_torrent"])
        done = cast(int, thread_stats["done"])
        done_size = cast(int, thread_stats["done_size"])

        pending_download_stats = cast(asyncpg.Record, pending_download_stats)
        pending_to_download = cast(int, pending_download_stats["count"])
        pending_to_download_size = cast(int, pending_download_stats["size"])

        status_stats = {
            str(r["status"]): {"count": int(r["count"]), "size": int(r["size"])}
            for r in job_status_rows
        }

        downloading = status_stats.get(ItemStatus.DOWNLOADING, {}).get("count", 0)
        downloading_size = status_stats.get(ItemStatus.DOWNLOADING, {}).get("size", 0)
        failed = status_stats.get(ItemStatus.FAILED, {}).get("count", 0)
        failed_size = status_stats.get(ItemStatus.FAILED, {}).get("size", 0)
        removed_by_client = status_stats.get(ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT, {}).get(
            "count", 0
        )
        removed_by_client_size = status_stats.get(ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT, {}).get(
            "size", 0
        )

        node_aliases = {
            str(r["id"]): str(r["alias"])
            for r in await pool.fetch("select id, alias from node where alias != ''")
        }

        def _node_name(nid: str) -> str:
            return node_aliases.get(nid, nid[:8])

        downloading_nodes = [
            {
                "node_id": str(r["node_id"]),
                "node_name": _node_name(str(r["node_id"])),
                "count": int(r["count"]),
                "size_fmt": human_readable_size(int(r["size"])),
            }
            for r in downloading_node_rows
        ]

        done_nodes = [
            {
                "node_id": str(r["node_id"]),
                "node_name": _node_name(str(r["node_id"])),
                "count": int(r["count"]),
                "size_fmt": human_readable_size(int(r["size"])),
            }
            for r in done_node_rows
        ]

        skipped = (
            total
            - done
            - downloading
            - failed
            - removed_by_client
            - pending_fetch_mediainfo
            - pending_fetch_torrent
            - pending_to_download
        )

        skipped_size = (
            total_size - done_size - downloading_size - failed_size - removed_by_client_size
        )

        def pct(n: int) -> str:
            if total == 0:
                return "0.0"
            return f"{n / total * 100:.1f}"

        return render(
            "index.html.j2",
            ctx={
                "scraped_total": scraped_total,
                "search_cursor": search_cursor or "N/A",
                "total": total,
                "total_size": human_readable_size(total_size),
                "done": done,
                "done_size": human_readable_size(done_size),
                "done_pct": pct(done),
                "done_nodes": done_nodes,
                "pending_fetch_mediainfo": pending_fetch_mediainfo,
                "pending_fetch_mediainfo_pct": pct(pending_fetch_mediainfo),
                "pending_fetch_torrent": pending_fetch_torrent,
                "pending_fetch_torrent_pct": pct(pending_fetch_torrent),
                "pending_to_download": pending_to_download,
                "pending_to_download_size": human_readable_size(pending_to_download_size),
                "pending_to_download_pct": pct(pending_to_download),
                "downloading": downloading,
                "downloading_size": human_readable_size(downloading_size),
                "downloading_pct": pct(downloading),
                "downloading_nodes": downloading_nodes,
                "failed": failed,
                "failed_size": human_readable_size(failed_size),
                "failed_pct": pct(failed),
                "removed_by_client": removed_by_client,
                "removed_by_client_size": human_readable_size(removed_by_client_size),
                "removed_by_client_pct": pct(removed_by_client),
                "skipped": skipped,
                "skipped_size": human_readable_size(skipped_size),
                "skipped_pct": pct(skipped),
                "node_aliases": node_aliases,
            },
        )

    @app.get("/api/weekly-charts")
    async def weekly_charts() -> ORJSONResponse:
        since = _today_start().date() - timedelta(days=13 * 7)
        await _backfill_daily_stats(since)
        history_rows, today_stats = await asyncio.gather(
            pool.fetch(
                "select * from daily_stats where day >= $1 order by day",
                since,
            ),
            _compute_today_stats(),
        )
        history_stats = [DailyStatsSnapshot.from_record(row) for row in history_rows]
        history_daily_stats = _build_history_daily_stats(history_stats)
        today_daily_stat = _build_today_daily_stat(today_stats)
        daily_stats = _combine_daily_stats(history_daily_stats, today_daily_stat)
        return ORJSONResponse(_build_weekly_charts(daily_stats).to_payload())

    @app.get("/detail")
    async def detail(render: Render, start: Annotated[str | None, Query()] = None) -> HTMLResponse:
        today = _today_start()
        default_start = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        start_value = start if start is not None else default_start

        start_dt: datetime | None = None
        if start_value:
            start_dt = datetime.strptime(start_value, "%Y-%m-%d").replace(tzinfo=_tz_shanghai)

        if start_dt is None:
            start_dt = today - timedelta(days=364)
        start_dt = start_dt.replace(tzinfo=_tz_shanghai) if start_dt.tzinfo is None else start_dt

        await _backfill_daily_stats(start_dt.date())
        history_rows, today_stats, alias_rows, all_node_rows = await asyncio.gather(
            pool.fetch(
                "select * from daily_stats where day >= $1 order by day",
                start_dt.date(),
            ),
            _compute_today_stats(),
            pool.fetch("select id, alias from node where alias != ''"),
            pool.fetch("select id from node"),
        )
        history_stats = [DailyStatsSnapshot.from_record(row) for row in history_rows]
        history_daily_stats = _build_history_daily_stats(history_stats)
        today_daily_stat = _build_today_daily_stat(today_stats)
        daily_stats = _combine_daily_stats(history_daily_stats, today_daily_stat)
        all_node_ids = {str(r["id"]) for r in all_node_rows}
        charts = _build_daily_charts(
            daily_stats, start_dt.date(), today.date(), all_node_ids
        ).to_context()
        node_aliases = {str(r["id"]): str(r["alias"]) for r in alias_rows}

        return render(
            "detail.html.j2",
            ctx={"start": start_value, "node_aliases": node_aliases} | charts,
        )

    @app.get("/threads/pending-mediainfo")
    async def threads_pending_mediainfo(
        render: Render, page: Annotated[int, Query()] = 1
    ) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Pending Fetch Mediainfo",
            count_sql="""
            select count(1)::int from thread
                        where deleted = false and mediainfo_at is null and seeders != 0
              and upload_at >= '2024-01-01' and category = any($1)
            """,
            rows_sql="""
            select tid, category, size, selected_size, seeders, created_at from thread
                        where deleted = false and mediainfo_at is null and seeders != 0
              and upload_at >= '2024-01-01' and category = any($1)
            order by tid desc
            limit $2 offset $3
            """,
            params=[SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=False,
        )

    @app.get("/threads/pending-torrent")
    async def threads_pending_torrent(
        render: Render, page: Annotated[int, Query()] = 1
    ) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Pending Fetch Torrent",
            count_sql="""
            select count(1)::int from thread
            where deleted = false and mediainfo_at is not null
                            and mediainfo = '' and info_hash = '' and torrent_invalid = '' and seeders != 0 and category = any($1)
            """,
            rows_sql="""
            select tid, category, size, selected_size, seeders, created_at from thread
            where deleted = false and mediainfo_at is not null
                            and mediainfo = '' and info_hash = '' and torrent_invalid = '' and seeders != 0 and category = any($1)
            order by tid desc
            limit $2 offset $3
            """,
            params=[SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=False,
        )

    @app.get("/threads/pending-download")
    async def threads_pending_download(
        render: Render,
        page: Annotated[int, Query()] = 1,
        strategy: Annotated[PickStrategy, Query()] = PickStrategy.seeders,
    ) -> HTMLResponse:
        if strategy == PickStrategy.seeders:
            order = "order by seeders desc, (category = any($2)) desc, tid asc"
        else:
            order = "order by (category = any($2)) desc, tid asc"
        return await _render_thread_list(
            render,
            title="Pending to Download",
            count_sql="""
            select count(1)::int
            from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
            rows_sql=f"""
            select thread.tid, category, size, selected_size, seeders, thread.created_at from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            {order}
            limit $3 offset $4
            """,
            params=[SELECTED_CATEGORY, PRIORITY_CATEGORY],
            count_params=[SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=False,
            extra_ctx={
                "pick_strategies": [s.value for s in PickStrategy],
                "current_strategy": strategy.value,
            },
        )

    @app.get("/threads/downloading")
    async def threads_downloading(render: Render, page: int = 1) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Downloading",
            count_sql="""
            select count(1)::int
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
            rows_sql="""
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.progress, job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            limit $3 offset $4
            """,
            params=[ItemStatus.DOWNLOADING, SELECTED_CATEGORY],
            page=page,
            show_progress=True,
            show_failed_reason=False,
        )

    @app.get("/threads/done")
    async def threads_done(render: Render, page: int = 1) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Done",
            count_sql="""
            select count(1)::int from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            """,
            rows_sql="""
            select thread.tid, category, size, selected_size, seeders, thread.created_at
            from thread
            join job on (job.tid = thread.tid)
            where mediainfo != '' and thread.info_hash != '' and category = any($1)
              and job.status = 'done'
            order by job.completed_at desc
            limit $2 offset $3
            """,
            params=[SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=False,
        )

    @app.get("/threads/failed")
    async def threads_failed(render: Render, page: int = 1) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Failed",
            count_sql="""
            select count(1)::int
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
            rows_sql="""
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.failed_reason
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            limit $3 offset $4
            """,
            params=[ItemStatus.FAILED, SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=True,
            show_reset=True,
        )

    @app.get("/threads/removed")
    async def threads_removed(render: Render, page: int = 1) -> HTMLResponse:
        return await _render_thread_list(
            render,
            title="Removed",
            count_sql="""
            select count(1)::int
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
            rows_sql="""
            select thread.tid, category, size, selected_size, seeders, thread.created_at
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            limit $3 offset $4
            """,
            params=[ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT, SELECTED_CATEGORY],
            page=page,
            show_progress=False,
            show_failed_reason=False,
            show_reset=True,
            show_reset_all=True,
        )

    @app.post("/api/thread/{tid}/reset")
    async def reset_thread(tid: int) -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where tid = $1 and status = any($2)
            """,
            tid,
            [ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT, ItemStatus.FAILED],
        )
        return ORJSONResponse({"deleted": result})

    @app.post("/api/threads/removed/reset-all")
    async def reset_all_removed_threads() -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where status = $1
            """,
            ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT,
        )
        return ORJSONResponse({"deleted": result})

    @app.post("/api/node/{node_id}/reset-jobs")
    async def reset_node_jobs(node_id: str) -> ORJSONResponse:
        node_row = await pool.fetchrow("select id from node where id = $1", node_id)
        if node_row is None:
            return ORJSONResponse({"error": "node not found"}, status_code=404)
        result = await pool.execute(
            """
            delete from job
            where node_id = $1 and status = $2
            """,
            node_id,
            ItemStatus.DOWNLOADING,
        )
        return ORJSONResponse({"deleted": result})

    @app.post("/api/node/{node_id}/rpc")
    async def node_rpc(node_id: str, body: RpcRequest) -> ORJSONResponse:
        payload_cls = PAYLOAD_TYPES.get(body.method)
        if payload_cls is None:
            return ORJSONResponse({"error": f"unknown method: {body.method}"}, status_code=400)

        try:
            parse_obj(payload_cls, body.payload)
        except Exception as e:
            return ORJSONResponse({"error": f"invalid payload: {e}"}, status_code=400)

        node_row = await pool.fetchrow("select id from node where id = $1", node_id)
        if node_row is None:
            return ORJSONResponse({"error": "node not found"}, status_code=404)

        cmd_id = await enqueue_command(pool, node_id, body.method, body.payload)
        return ORJSONResponse({"id": cmd_id})

    @app.post("/api/node/{node_id}/alias")
    async def set_node_alias(node_id: str, request: Request) -> ORJSONResponse:
        body = await request.json()
        alias = (body.get("alias") or "").strip()
        node_row = await pool.fetchrow("select id from node where id = $1", node_id)
        if node_row is None:
            return ORJSONResponse({"error": "node not found"}, status_code=404)
        await pool.execute(
            "update node set alias = $1 where id = $2",
            alias,
            node_id,
        )
        return ORJSONResponse({"ok": True})

    @app.post("/api/daily-stats/clear")
    async def clear_daily_stats() -> ORJSONResponse:
        result = await pool.execute("delete from daily_stats")
        return ORJSONResponse({"deleted": result})

    @app.get("/api/config")
    async def list_config() -> ORJSONResponse:
        rows = await pool.fetch("select key, value from config order by key")
        return ORJSONResponse([{"key": r["key"], "value": r["value"]} for r in rows])

    @app.post("/api/config")
    async def upsert_config(body: ConfigUpsertRequest) -> ORJSONResponse:
        key = body.key.strip()
        value = body.value.strip()
        if not key:
            return ORJSONResponse({"error": "key is required"}, status_code=422)
        if not value:
            return ORJSONResponse({"error": "value is required"}, status_code=422)
        if key == "schema_version":
            return ORJSONResponse({"error": "key is protected"}, status_code=403)
        await pool.execute(
            "insert into config (key, value) values ($1, $2)"
            " on conflict (key) do update set value = excluded.value",
            key,
            value,
        )
        return ORJSONResponse({"ok": True})

    @app.delete("/api/config/{key}")
    async def delete_config(key: str) -> ORJSONResponse:
        if key == "schema_version":
            return ORJSONResponse({"error": "key is protected"}, status_code=403)
        result = await pool.execute("delete from config where key = $1", key)
        if result == "DELETE 0":
            return ORJSONResponse({"error": "key not found"}, status_code=404)
        return ORJSONResponse({"ok": True})

    @app.get("/admin")
    async def admin_page(render: Render) -> HTMLResponse:
        config_rows = await pool.fetch("select key, value from config order by key")
        return render("admin.html.j2", ctx={"config": [dict(r) for r in config_rows]})

    @app.get("/nodes")
    async def nodes_page(render: Render) -> HTMLResponse:
        node_rows = await pool.fetch(
            "select id, last_seen, alias, version from node order by id asc"
        )
        job_rows = await pool.fetch(
            "select node_id, status, count(1) as cnt, coalesce(sum(dlspeed), 0) as total_dlspeed from job group by node_id, status"
        )

        counts: dict[str, dict[str, int]] = {}
        speeds: dict[str, int] = {}
        for r in job_rows:
            nid = str(r["node_id"])
            counts.setdefault(nid, {})
            counts[nid][r["status"]] = r["cnt"]
            if r["status"] == ItemStatus.DOWNLOADING:
                speeds[nid] = r["total_dlspeed"]

        nodes_data = [
            {
                "id": str(n["id"]),
                "alias": n["alias"],
                "last_seen": n["last_seen"],
                "version": n["version"],
                "downloading": counts.get(str(n["id"]), {}).get(ItemStatus.DOWNLOADING, 0),
                "dlspeed_fmt": human_readable_byte_rate(speeds.get(str(n["id"]), 0)),
                "done": counts.get(str(n["id"]), {}).get(ItemStatus.DONE, 0),
                "failed": counts.get(str(n["id"]), {}).get(ItemStatus.FAILED, 0),
                "removed": counts.get(str(n["id"]), {}).get(
                    ItemStatus.REMOVED_FROM_DOWNLOAD_CLIENT, 0
                ),
                "total": sum(counts.get(str(n["id"]), {}).values()),
            }
            for n in node_rows
        ]

        return render("nodes.html.j2", ctx={"nodes": sorted(nodes_data, key=itemgetter("alias"))})

    @app.get("/nodes/{node_id}")
    async def node_jobs_page(
        node_id: str,
        render: Render,
        status: Annotated[
            Literal[ItemStatus.DOWNLOADING, ItemStatus.DONE], Query()
        ] = ItemStatus.DOWNLOADING,
        page: Annotated[int, Query()] = 1,
    ) -> HTMLResponse:
        node_row = await pool.fetchrow(
            "select id, last_seen, alias, version from node where id = $1", node_id
        )
        if node_row is None:
            return render(
                "node_jobs.html.j2",
                ctx={"node_id": node_id, "node_name": "", "jobs": [], "status": status},
                status_code=404,
            )

        total_count, rows = await asyncio.gather(
            pool.fetchval(
                "select count(1)::int from job where node_id = $1 and status = $2",
                node_id,
                status,
            ),
            pool.fetch(
                f"""
                select job.tid, job.status, job.progress, job.failed_reason,
                       job.start_download_time, job.updated_at,
                       job.dlspeed, job.eta, job.info_hash,
                       job.completed_at,
                       thread.size, thread.selected_size, thread.seeders
                from job
                join thread on (thread.tid = job.tid)
                where job.node_id = $1 and job.status = $2
                order by {"job.completed_at desc nulls last" if status == ItemStatus.DONE else "job.eta asc nulls last"}
                limit $3 offset $4
                """,
                node_id,
                status,
                PAGE_SIZE,
                (page - 1) * PAGE_SIZE,
            ),
        )

        pager = _pagination(page, cast(int, total_count))

        now = datetime.now(tz=_tz_shanghai)

        def _calc_speed_eta(r: asyncpg.Record) -> dict[str, Any]:
            dlspeed: int = r["dlspeed"]
            eta: int = r["eta"]
            updated: datetime | None = r["updated_at"]
            if eta < 0:
                return {
                    "speed_fmt": "-" if dlspeed <= 0 else human_readable_byte_rate(dlspeed),
                    "eta_fmt": "-",
                    "eta_seconds": float("inf"),
                }
            elapsed_since = (now - updated).total_seconds() if updated else 0
            eta_seconds = max(0.0, float(eta) - elapsed_since)
            return {
                "speed_fmt": human_readable_byte_rate(dlspeed) if dlspeed > 2 else "-",
                "eta_fmt": _fmt_eta(eta_seconds) if dlspeed > 2 else "-",
                "eta_seconds": eta_seconds if dlspeed > 2 else float("inf"),
            }

        jobs = [
            dict(r)
            | {
                "size_fmt": human_readable_size(r["size"]),
                "selected_size_fmt": human_readable_size(r["selected_size"])
                if r["selected_size"] > 0
                else "-",
                "progress_fmt": f"{int(r['progress'] * 1000) / 10:.1f}",
            }
            | _calc_speed_eta(r)
            for r in rows
        ]

        return render(
            "node_jobs.html.j2",
            ctx={
                "node_id": str(node_row["id"]),
                "node_name": node_row["alias"] or str(node_row["id"])[:8],
                "last_seen": node_row["last_seen"],
                "version": node_row["version"],
                "jobs": jobs,
                "status": status,
            }
            | {k: v for k, v in pager.items() if k != "offset"},
        )

    @app.get("/rpc")
    async def rpc_history_page(render: Render) -> HTMLResponse:
        rows, alias_rows = await asyncio.gather(
            pool.fetch(
                """select id, node_id, method, payload, result, error, created_at, executed_at
                   from node_command
                   order by id desc
                   limit 200"""
            ),
            pool.fetch("select id, alias from node where alias != ''"),
        )
        node_aliases = {str(r["id"]): str(r["alias"]) for r in alias_rows}
        commands = [
            {
                "id": r["id"],
                "node_id": str(r["node_id"]),
                "node_name": node_aliases.get(str(r["node_id"]), str(r["node_id"])[:8]),
                "method": r["method"],
                "payload": r["payload"],
                "result": r["result"],
                "error": r["error"],
                "created_at": r["created_at"],
                "executed_at": r["executed_at"],
                "status": "pending"
                if r["executed_at"] is None
                else ("error" if r["error"] else "done"),
            }
            for r in rows
        ]
        return render("rpc.html.j2", ctx={"commands": commands})

    return app
