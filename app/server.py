from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Protocol
from zoneinfo import ZoneInfo

import asyncpg
import fastapi
import orjson
import polars as pl
from fastapi import Depends, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, JSONResponse

from app.config import Config, load_config
from app.const import (
    ITEM_STATUS_DONE,
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
    ITEM_STATUS_SKIPPED,
    SELECTED_CATEGORY,
)
from app.utils import human_readable_byte_rate, human_readable_size


class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_INDENT_2, default=str)


class _Render(Protocol):
    def __call__(
        self,
        name: str,
        ctx: dict[str, Any] | None = ...,
        status_code: int = ...,
        headers: Mapping[str, str] | None = ...,
        media_type: str | None = ...,
    ) -> HTMLResponse: ...


def create_app() -> fastapi.FastAPI:
    cfg: Config = load_config()

    pool = asyncpg.create_pool(cfg.pg_dsn())

    @asynccontextmanager
    async def lifespan(_app: fastapi.FastAPI) -> AsyncGenerator[None, None]:
        await pool
        yield
        await pool.close()

    app = fastapi.FastAPI(debug=cfg.debug, lifespan=lifespan)

    templates = Jinja2Templates(
        directory=str(Path(__file__).parent.joinpath("templates").resolve())
    )

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

    @app.get("/nodes")
    async def nodes() -> ORJSONResponse:
        torrents = await pool.fetch("""select * from node order by last_seen desc""")

        return ORJSONResponse([dict(t) for t in torrents])

    @app.get("/threads")
    async def threads() -> ORJSONResponse:
        torrents = await pool.fetch(
            """
            select thread.* from thread
            left join job on (job.tid = thread.tid)
            where
              deleted = false and
              seeders != 0 and
              mediainfo_at is not null and
              mediainfo = '' and
              category = any($1) and
              job.tid is null
            order by thread.tid desc
            """,
            SELECTED_CATEGORY,
        )

        return ORJSONResponse([
            dict(x) | {"size": human_readable_size(x["size"])} for x in torrents
        ])

    @app.get("/overview")
    async def overview() -> ORJSONResponse:
        pending_size = await pool.fetchval(
            """
            select sum(size) from thread
            where
              deleted = false and
              seeders != 0 and
              info_hash != '' and
              mediainfo = '' and
              category = any($1)
            """,
            SELECTED_CATEGORY,
        )

        pending_count = await pool.fetchval(
            """
            select count(1) from thread
            where
              deleted = false and
              seeders != 0 and
              info_hash != '' and
              mediainfo = '' and
              category = any($1)
            """,
            SELECTED_CATEGORY,
        )

        return ORJSONResponse({
            "pending_size": human_readable_size(pending_size),
            "pending_count": pending_count,
        })

    @app.get("/")
    async def index(render: Annotated[_Render, Depends(__render)]) -> HTMLResponse:
        torrents = await pool.fetch(
            """select * from job where (not status = any($1)) order by updated_at desc""",
            [ITEM_STATUS_SKIPPED, ITEM_STATUS_DONE],
        )

        return render(
            "index.html.j2",
            ctx={"torrents": torrents},
        )

    @app.get("/thread/{tid}")
    async def rss_item(tid: int) -> ORJSONResponse:
        rows = await pool.fetch(
            """select * from job where tid = $1""",
            tid,
        )

        return ORJSONResponse([dict(t) for t in rows])

        # return render(
        #     "rss-item.html.j2",
        #     ctx={"torrent": torrent},
        # )

    _tz_shanghai = ZoneInfo("Asia/Shanghai")

    def _today_start() -> datetime:
        now = datetime.now(_tz_shanghai)
        return datetime(now.year, now.month, now.day, tzinfo=_tz_shanghai)

    def _week_num(ts: datetime, today: datetime) -> int:
        """Week number relative to today. -1 = current week (today + last 6 days), -2 = 7-13 days ago, etc."""
        ref = today + timedelta(days=1)
        return int((ts - ref).total_seconds() // (7 * 86400))

    def _week_range() -> tuple[int, int]:
        """Return (min_week_num, max_week_num) for last 52 weeks."""
        return -52, -1

    def _fill_week_gaps_count(df: pl.DataFrame) -> pl.DataFrame:
        grouped = df.group_by("week_num").len(name="count")
        min_week, max_week = _week_range()
        all_weeks = pl.DataFrame({"week_num": list(range(min_week, max_week + 1))})
        return (
            all_weeks
            .join(grouped, on="week_num", how="left")
            .with_columns(pl.col("count").fill_null(0))
            .sort("week_num")
        )

    def _compute_week_num_col(today: datetime) -> pl.Expr:
        ref = today + timedelta(days=1)
        return (
            (
                (pl.col("ts").cast(pl.Datetime("us", "Asia/Shanghai")) - ref).dt.total_seconds()
                // (7 * 86400)
            )
            .cast(pl.Int64)
            .alias("week_num")
        )

    def _week_label(today: datetime, week_num: int) -> str:
        ref = today + timedelta(days=1)
        start = ref + timedelta(weeks=int(week_num))
        return start.strftime("%Y-%m-%d")

    @app.get("/stats/weekly-byte-rate")
    async def weekly_byte_rate() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select
                coalesce(job.completed_at, job.updated_at) as ts,
                thread.selected_size
            from job
            join thread on (thread.tid = job.tid)
            where
                job.status = $1 and thread.selected_size > 0
                and coalesce(job.completed_at, job.updated_at) >= current_timestamp - interval '1 year'
            """,
            ITEM_STATUS_DONE,
        )
        if not rows:
            return ORJSONResponse([])

        today = _today_start()
        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "selected_size": [row["selected_size"] for row in rows],
        })
        df = df.with_columns(_compute_week_num_col(today))
        grouped = df.group_by("week_num").agg(pl.col("selected_size").sum().alias("total_size"))
        min_week, max_week = _week_range()
        all_weeks = pl.DataFrame({"week_num": list(range(min_week, max_week + 1))})
        result = (
            all_weeks
            .join(grouped, on="week_num", how="left")
            .with_columns(pl.col("total_size").fill_null(0))
            .with_columns((pl.col("total_size") / (7.0 * 86400)).alias("byte_rate"))
            .sort("week_num")
        )
        return ORJSONResponse([
            {
                "week": _week_label(today, r["week_num"]),
                "byte_rate": r["byte_rate"],
                "byte_rate_fmt": human_readable_byte_rate(r["byte_rate"]),
                "total_size": int(r["total_size"]),
                "total_size_fmt": human_readable_size(r["total_size"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/weekly-thread-count")
    async def weekly_thread_count() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select created_at as ts
            from thread
            where created_at >= current_timestamp - interval '1 year'
            """
        )
        if not rows:
            return ORJSONResponse([])

        today = _today_start()
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return ORJSONResponse([
            {
                "week": _week_label(today, r["week_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/weekly-torrent-count")
    async def weekly_torrent_count() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select created_at as ts
            from torrent
            where created_at >= current_timestamp - interval '1 year'
            """
        )
        if not rows:
            return ORJSONResponse([])

        today = _today_start()
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return ORJSONResponse([
            {
                "week": _week_label(today, r["week_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/weekly-done-count")
    async def weekly_done_count() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select coalesce(completed_at, updated_at) as ts
            from job
            where
                status = $1 and
                coalesce(completed_at, updated_at) >= current_timestamp - interval '1 year'
            """,
            ITEM_STATUS_DONE,
        )
        if not rows:
            return ORJSONResponse([])

        today = _today_start()
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return ORJSONResponse([
            {
                "week": _week_label(today, r["week_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/weekly-mediainfo-count")
    async def weekly_mediainfo_count() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select mediainfo_at as ts
            from thread
            where
                mediainfo_at is not null and
                mediainfo_at >= current_timestamp - interval '1 year'
            """
        )
        if not rows:
            return ORJSONResponse([])

        today = _today_start()
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return ORJSONResponse([
            {
                "week": _week_label(today, r["week_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    def _day_num(ts: datetime, today: datetime) -> int:
        return int((ts - today).total_seconds() // 86400)

    def _compute_day_num_col(today: datetime) -> pl.Expr:
        return (
            (
                (pl.col("ts").cast(pl.Datetime("us", "Asia/Shanghai")) - today).dt.total_seconds()
                // 86400
            )
            .cast(pl.Int64)
            .alias("day_num")
        )

    def _day_label(today: datetime, day_num: int) -> str:
        return (today + timedelta(days=int(day_num))).strftime("%Y-%m-%d")

    def _fill_day_gaps_count(df: pl.DataFrame, days_back: int = 364) -> pl.DataFrame:
        grouped = df.group_by("day_num").len(name="count")
        all_days = pl.DataFrame({"day_num": list(range(-days_back, 1))})
        return (
            all_days
            .join(grouped, on="day_num", how="left")
            .with_columns(pl.col("count").fill_null(0))
            .sort("day_num")
        )

    @app.get("/stats/daily-byte-rate")
    async def daily_byte_rate(start: datetime | None = None) -> ORJSONResponse:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select coalesce(job.completed_at, job.updated_at) as ts,
                   thread.selected_size
            from job
            join thread on (thread.tid = job.tid)
            where
                job.status = $1 and thread.selected_size > 0
                and coalesce(job.completed_at, job.updated_at) >= $2
            """,
            ITEM_STATUS_DONE,
            start,
        )
        if not rows:
            return ORJSONResponse([])

        elapsed_today = (datetime.now(_tz_shanghai) - today).total_seconds()

        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "selected_size": [row["selected_size"] for row in rows],
        })
        df = df.with_columns(_compute_day_num_col(today))
        grouped = df.group_by("day_num").agg(pl.col("selected_size").sum().alias("total_size"))
        all_days = pl.DataFrame({"day_num": list(range(-days_back, 1))})
        result = (
            all_days
            .join(grouped, on="day_num", how="left")
            .with_columns(pl.col("total_size").fill_null(0))
            .with_columns((pl.col("total_size") / 86400.0).alias("byte_rate"))
            .sort("day_num")
        )
        return ORJSONResponse([
            {
                "day": _day_label(today, r["day_num"]),
                "byte_rate": r["total_size"] / elapsed_today
                if r["day_num"] == 0
                else r["byte_rate"],
                "byte_rate_fmt": human_readable_byte_rate(
                    r["total_size"] / elapsed_today if r["day_num"] == 0 else r["byte_rate"]
                ),
                "total_size": int(r["total_size"]),
                "total_size_fmt": human_readable_size(r["total_size"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/daily-thread-count")
    async def daily_thread_count(start: datetime | None = None) -> ORJSONResponse:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select created_at as ts
            from thread
            where created_at >= $1
            """,
            start,
        )
        if not rows:
            return ORJSONResponse([])

        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return ORJSONResponse([
            {
                "day": _day_label(today, r["day_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/daily-torrent-count")
    async def daily_torrent_count(start: datetime | None = None) -> ORJSONResponse:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select created_at as ts
            from torrent
            where created_at >= $1
            """,
            start,
        )
        if not rows:
            return ORJSONResponse([])

        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return ORJSONResponse([
            {
                "day": _day_label(today, r["day_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/daily-done-count")
    async def daily_done_count(start: datetime | None = None) -> ORJSONResponse:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select coalesce(completed_at, updated_at) as ts
            from job
            where
                status = $1 and
                coalesce(completed_at, updated_at) >= $2
            """,
            ITEM_STATUS_DONE,
            start,
        )
        if not rows:
            return ORJSONResponse([])

        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return ORJSONResponse([
            {
                "day": _day_label(today, r["day_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/stats/daily-mediainfo-count")
    async def daily_mediainfo_count(start: datetime | None = None) -> ORJSONResponse:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select mediainfo_at as ts
            from thread
            where
                mediainfo_at is not null and
                mediainfo_at >= $1
            """,
            start,
        )
        if not rows:
            return ORJSONResponse([])

        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return ORJSONResponse([
            {
                "day": _day_label(today, r["day_num"]),
                "count": int(r["count"]),
            }
            for r in result.iter_rows(named=True)
        ])

    @app.get("/progress")
    async def progress(render: Annotated[_Render, Depends(__render)]) -> HTMLResponse:
        scraped_total = await pool.fetchval("select count(1) from thread") or 0
        search_cursor = await pool.fetchval("select value from config where key = 'search_cursor'")

        total = (
            await pool.fetchval(
                "select count(1) from thread where category = any($1)",
                SELECTED_CATEGORY,
            )
            or 0
        )

        total_size = (
            await pool.fetchval(
                "select coalesce(sum(selected_size), 0) from thread where category = any($1)",
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: pending fetch mediainfo
        pending_fetch_mediainfo = (
            await pool.fetchval(
                """
            select count(1) from thread
            where deleted = false and mediainfo_at is null
              and upload_at >= '2024-01-01' and category = any($1)
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: pending fetch torrent (info_hash)
        pending_fetch_torrent = (
            await pool.fetchval(
                """
            select count(1) from thread
            where deleted = false and mediainfo_at is not null
              and mediainfo = '' and info_hash = '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: pending to download (has info_hash, no job, has seeders)
        pending_to_download = (
            await pool.fetchval(
                """
            select count(1) from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        pending_to_download_size = (
            await pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: downloading
        downloading = (
            await pool.fetchval(
                """
            select count(1) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_DOWNLOADING,
                SELECTED_CATEGORY,
            )
            or 0
        )

        downloading_size = (
            await pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_DOWNLOADING,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: failed
        failed = (
            await pool.fetchval(
                """
            select count(1) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_FAILED,
                SELECTED_CATEGORY,
            )
            or 0
        )

        failed_size = (
            await pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_FAILED,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: done (mediainfo obtained via download)
        done = (
            await pool.fetchval(
                """
            select count(1) from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        done_size = (
            await pool.fetchval(
                """
            select coalesce(sum(selected_size), 0) from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        # Lifecycle: removed by client
        removed_by_client = (
            await pool.fetchval(
                """
            select count(1) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                SELECTED_CATEGORY,
            )
            or 0
        )

        removed_by_client_size = (
            await pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                SELECTED_CATEGORY,
            )
            or 0
        )

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
            "progress.html.j2",
            ctx={
                "scraped_total": scraped_total,
                "search_cursor": search_cursor or "N/A",
                "total": total,
                "total_size": human_readable_size(total_size),
                "done": done,
                "done_size": human_readable_size(done_size),
                "done_pct": pct(done),
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
                "failed": failed,
                "failed_size": human_readable_size(failed_size),
                "failed_pct": pct(failed),
                "removed_by_client": removed_by_client,
                "removed_by_client_size": human_readable_size(removed_by_client_size),
                "removed_by_client_pct": pct(removed_by_client),
                "skipped": skipped,
                "skipped_size": human_readable_size(skipped_size),
                "skipped_pct": pct(skipped),
            },
        )

    @app.get("/detail")
    async def detail(
        render: Annotated[_Render, Depends(__render)],
        start: str | None = None,
    ) -> HTMLResponse:
        today = _today_start()
        default_start = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        start_value = start if start is not None else default_start
        return render("detail.html.j2", ctx={"start": start_value})

    @app.get("/threads/pending-mediainfo")
    async def threads_pending_mediainfo(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where deleted = false and mediainfo_at is null
              and upload_at >= '2024-01-01' and category = any($1)
            order by tid desc
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending Fetch Mediainfo",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/pending-torrent")
    async def threads_pending_torrent(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where deleted = false and mediainfo_at is not null
              and mediainfo = '' and info_hash = '' and category = any($1)
            order by tid desc
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending Fetch Torrent",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/pending-download")
    async def threads_pending_download(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            order by selected_size desc
            limit 100
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Pending to Download",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"]),
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/downloading")
    async def threads_downloading(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.progress, job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_DOWNLOADING,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Downloading",
                "show_progress": True,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/done")
    async def threads_done(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select tid, category, size, selected_size, seeders, created_at from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            order by tid desc
            limit 100
            """,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Done",
                "show_progress": False,
                "show_failed_reason": False,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/failed")
    async def threads_failed(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at,
                   job.failed_reason
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_FAILED,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Failed",
                "show_progress": False,
                "show_failed_reason": True,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.get("/threads/removed")
    async def threads_removed(
        render: Annotated[_Render, Depends(__render)],
    ) -> HTMLResponse:
        rows = await pool.fetch(
            """
            select thread.tid, category, size, selected_size, seeders, thread.created_at
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            order by job.updated_at desc
            """,
            ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
            SELECTED_CATEGORY,
        )
        return render(
            "threads.html.j2",
            ctx={
                "title": "Removed by Client",
                "show_progress": False,
                "show_failed_reason": False,
                "show_reset": True,
                "threads": [
                    dict(r)
                    | {
                        "size_fmt": human_readable_size(r["size"]),
                        "selected_size_fmt": human_readable_size(r["selected_size"])
                        if r["selected_size"] > 0
                        else "-",
                    }
                    for r in rows
                ],
            },
        )

    @app.post("/api/threads/removed/reset")
    async def reset_removed_threads() -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where status = $1
            """,
            ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
        )
        return ORJSONResponse({"deleted": result})

    @app.post("/api/thread/{tid}/reset")
    async def reset_thread(tid: int) -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where tid = $1 and status = $2
            """,
            tid,
            ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
        )
        return ORJSONResponse({"deleted": result})

    return app
