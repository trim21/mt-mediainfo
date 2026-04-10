import asyncio
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Protocol, cast
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

    def _fmt_dt(dt: datetime | None) -> str:
        if dt is None:
            return "-"
        return dt.astimezone(_tz_shanghai).strftime("%Y-%m-%d %H:%M:%S")

    templates.env.filters["fmt_dt"] = _fmt_dt

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

    async def _get_weekly_byte_rate_data() -> dict[str, Any]:
        rows = await pool.fetch(
            """
            select
                coalesce(job.completed_at, job.updated_at) as ts,
                thread.selected_size,
                job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where
                job.status = $1 and thread.selected_size > 0
                and coalesce(job.completed_at, job.updated_at) >= current_timestamp - interval '1 year'
            """,
            ITEM_STATUS_DONE,
        )
        if not rows:
            return {"labels": [], "totals": [], "per_node": {}}

        today = _today_start()
        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "selected_size": [row["selected_size"] for row in rows],
            "node_id": [str(row["node_id"]) for row in rows],
        })
        df = df.with_columns(_compute_week_num_col(today))

        grouped_total = df.group_by("week_num").agg(
            pl.col("selected_size").sum().alias("total_size")
        )
        min_week, max_week = _week_range()
        all_weeks = pl.DataFrame({"week_num": list(range(min_week, max_week + 1))})
        result_total = (
            all_weeks
            .join(grouped_total, on="week_num", how="left")
            .with_columns(pl.col("total_size").fill_null(0))
            .with_columns((pl.col("total_size") / (7.0 * 86400)).alias("byte_rate"))
            .sort("week_num")
        )

        grouped_node = df.group_by(["week_num", "node_id"]).agg(
            pl.col("selected_size").sum().alias("total_size")
        )
        node_ids = sorted(df["node_id"].unique().to_list())

        per_node: dict[str, list[float]] = {}
        for nid in node_ids:
            node_data = grouped_node.filter(pl.col("node_id") == nid)
            node_result = (
                all_weeks
                .join(node_data.select(["week_num", "total_size"]), on="week_num", how="left")
                .with_columns(pl.col("total_size").fill_null(0))
                .with_columns((pl.col("total_size") / (7.0 * 86400)).alias("byte_rate"))
                .sort("week_num")
            )
            per_node[nid] = [r["byte_rate"] for r in node_result.iter_rows(named=True)]

        labels = [_week_label(today, r["week_num"]) for r in result_total.iter_rows(named=True)]
        totals = [
            {
                "byte_rate": r["byte_rate"],
                "byte_rate_fmt": human_readable_byte_rate(r["byte_rate"]),
                "total_size": int(r["total_size"]),
                "total_size_fmt": human_readable_size(r["total_size"]),
            }
            for r in result_total.iter_rows(named=True)
        ]

        return {"labels": labels, "totals": totals, "per_node": per_node}

    async def _get_weekly_count_data(
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        rows = await pool.fetch(sql, *(params or []))
        if not rows:
            return []
        today = _today_start()
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return [
            {"week": _week_label(today, r["week_num"]), "count": int(r["count"])}
            for r in result.iter_rows(named=True)
        ]

    async def _get_weekly_done_count_data() -> dict[str, Any]:
        rows = await pool.fetch(
            """select coalesce(completed_at, updated_at) as ts, node_id from job
            where status = $1 and coalesce(completed_at, updated_at) >= current_timestamp - interval '1 year'""",
            ITEM_STATUS_DONE,
        )
        if not rows:
            return {"labels": [], "totals": [], "per_node": {}}

        today = _today_start()
        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "node_id": [str(row["node_id"]) for row in rows],
        })
        df = df.with_columns(_compute_week_num_col(today))

        min_week, max_week = _week_range()
        all_weeks = pl.DataFrame({"week_num": list(range(min_week, max_week + 1))})

        grouped_total = df.group_by("week_num").len(name="count")
        result_total = (
            all_weeks
            .join(grouped_total, on="week_num", how="left")
            .with_columns(pl.col("count").fill_null(0))
            .sort("week_num")
        )

        grouped_node = df.group_by(["week_num", "node_id"]).len(name="count")
        node_ids = sorted(df["node_id"].unique().to_list())

        per_node: dict[str, list[int]] = {}
        for nid in node_ids:
            node_data = grouped_node.filter(pl.col("node_id") == nid)
            node_result = (
                all_weeks
                .join(node_data.select(["week_num", "count"]), on="week_num", how="left")
                .with_columns(pl.col("count").fill_null(0))
                .sort("week_num")
            )
            per_node[nid] = [int(r["count"]) for r in node_result.iter_rows(named=True)]

        labels = [_week_label(today, r["week_num"]) for r in result_total.iter_rows(named=True)]
        totals = [int(r["count"]) for r in result_total.iter_rows(named=True)]

        return {"labels": labels, "totals": totals, "per_node": per_node}

    async def _get_daily_byte_rate_data(
        start: datetime | None = None,
    ) -> dict[str, Any]:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select coalesce(job.completed_at, job.updated_at) as ts,
                   thread.selected_size,
                   job.node_id
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
            return {"labels": [], "totals": [], "per_node": {}}

        elapsed_today = (datetime.now(_tz_shanghai) - today).total_seconds()

        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "selected_size": [row["selected_size"] for row in rows],
            "node_id": [str(row["node_id"]) for row in rows],
        })
        df = df.with_columns(_compute_day_num_col(today))

        grouped_total = df.group_by("day_num").agg(
            pl.col("selected_size").sum().alias("total_size")
        )
        all_days = pl.DataFrame({"day_num": list(range(-days_back, 1))})
        result_total = (
            all_days
            .join(grouped_total, on="day_num", how="left")
            .with_columns(pl.col("total_size").fill_null(0))
            .with_columns((pl.col("total_size") / 86400.0).alias("byte_rate"))
            .sort("day_num")
        )

        grouped_node = df.group_by(["day_num", "node_id"]).agg(
            pl.col("selected_size").sum().alias("total_size")
        )
        node_ids = sorted(df["node_id"].unique().to_list())

        per_node: dict[str, list[float]] = {}
        for nid in node_ids:
            node_data = grouped_node.filter(pl.col("node_id") == nid)
            node_result = (
                all_days
                .join(node_data.select(["day_num", "total_size"]), on="day_num", how="left")
                .with_columns(pl.col("total_size").fill_null(0))
                .with_columns((pl.col("total_size") / 86400.0).alias("byte_rate"))
                .sort("day_num")
            )
            per_node[nid] = [
                r["total_size"] / elapsed_today if r["day_num"] == 0 else r["byte_rate"]
                for r in node_result.iter_rows(named=True)
            ]

        labels = [_day_label(today, r["day_num"]) for r in result_total.iter_rows(named=True)]
        totals = [
            {
                "byte_rate": r["total_size"] / elapsed_today
                if r["day_num"] == 0
                else r["byte_rate"],
                "byte_rate_fmt": human_readable_byte_rate(
                    r["total_size"] / elapsed_today if r["day_num"] == 0 else r["byte_rate"]
                ),
                "total_size": int(r["total_size"]),
                "total_size_fmt": human_readable_size(r["total_size"]),
            }
            for r in result_total.iter_rows(named=True)
        ]

        return {"labels": labels, "totals": totals, "per_node": per_node}

    async def _get_daily_fetched_size_data(
        start: datetime | None = None,
    ) -> list[dict[str, Any]]:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """
            select torrent_fetched_at as ts, selected_size
            from thread
            where torrent_fetched_at >= $1 and selected_size > 0
              and category = any($2)
            """,
            start,
            SELECTED_CATEGORY,
        )
        if not rows:
            return []
        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "selected_size": [row["selected_size"] for row in rows],
        })
        df = df.with_columns(_compute_day_num_col(today))
        grouped = df.group_by("day_num").agg(
            pl.col("selected_size").sum().alias("total_size"),
            pl.col("selected_size").len().alias("count"),
        )
        all_days = pl.DataFrame({"day_num": list(range(-days_back, 1))})
        result = (
            all_days
            .join(grouped, on="day_num", how="left")
            .with_columns(pl.col("total_size").fill_null(0))
            .with_columns(pl.col("count").fill_null(0))
            .sort("day_num")
        )
        return [
            {
                "day": _day_label(today, r["day_num"]),
                "byte_rate": (r["total_size"] / r["count"]) * 1200 / 86400.0
                if r["day_num"] == 0 and r["count"] > 0
                else r["total_size"] / 86400.0,
            }
            for r in result.iter_rows(named=True)
        ]

    async def _get_daily_count_data(
        sql: str,
        params: list[Any] | None = None,
        start: datetime | None = None,
    ) -> list[dict[str, Any]]:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(sql, *(params or []))
        if not rows:
            return []
        df = pl.DataFrame({"ts": [row["ts"] for row in rows]})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return [
            {"day": _day_label(today, r["day_num"]), "count": int(r["count"])}
            for r in result.iter_rows(named=True)
        ]

    async def _get_daily_done_count_data(
        start: datetime | None = None,
    ) -> dict[str, Any]:
        today = _today_start()
        if start is None:
            start = today - timedelta(days=364)
        start = start.replace(tzinfo=_tz_shanghai) if start.tzinfo is None else start
        days_back = (today - start).days
        rows = await pool.fetch(
            """select coalesce(completed_at, updated_at) as ts, node_id from job
            where status = $1 and coalesce(completed_at, updated_at) >= $2""",
            ITEM_STATUS_DONE,
            start,
        )
        if not rows:
            return {"labels": [], "totals": [], "per_node": {}}

        df = pl.DataFrame({
            "ts": [row["ts"] for row in rows],
            "node_id": [str(row["node_id"]) for row in rows],
        })
        df = df.with_columns(_compute_day_num_col(today))

        all_days = pl.DataFrame({"day_num": list(range(-days_back, 1))})

        grouped_total = df.group_by("day_num").len(name="count")
        result_total = (
            all_days
            .join(grouped_total, on="day_num", how="left")
            .with_columns(pl.col("count").fill_null(0))
            .sort("day_num")
        )

        grouped_node = df.group_by(["day_num", "node_id"]).len(name="count")
        node_ids = sorted(df["node_id"].unique().to_list())

        per_node: dict[str, list[int]] = {}
        for nid in node_ids:
            node_data = grouped_node.filter(pl.col("node_id") == nid)
            node_result = (
                all_days
                .join(node_data.select(["day_num", "count"]), on="day_num", how="left")
                .with_columns(pl.col("count").fill_null(0))
                .sort("day_num")
            )
            per_node[nid] = [int(r["count"]) for r in node_result.iter_rows(named=True)]

        labels = [_day_label(today, r["day_num"]) for r in result_total.iter_rows(named=True)]
        totals = [int(r["count"]) for r in result_total.iter_rows(named=True)]

        return {"labels": labels, "totals": totals, "per_node": per_node}

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

    @app.get("/")
    async def progress(render: Annotated[_Render, Depends(__render)]) -> HTMLResponse:
        (
            scraped_total,
            search_cursor,
            total,
            total_size,
            pending_fetch_mediainfo,
            pending_fetch_torrent,
            pending_to_download,
            pending_to_download_size,
            downloading_rows,
            failed,
            failed_size,
            done,
            done_size,
            done_job_rows,
            removed_by_client,
            removed_by_client_size,
            weekly_byte_rate_data,
            weekly_thread_count_data,
            weekly_torrent_count_data,
            weekly_done_count_data,
            weekly_mediainfo_count_data,
        ) = await asyncio.gather(
            pool.fetchval("select count(1) from thread"),
            pool.fetchval("select value from config where key = 'search_cursor'"),
            pool.fetchval(
                "select count(1) from thread where category = any($1)",
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                "select coalesce(sum(selected_size), 0) from thread where category = any($1)",
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from thread
            where deleted = false and mediainfo_at is null
              and upload_at >= '2024-01-01' and category = any($1)
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from thread
            where deleted = false and mediainfo_at is not null
              and mediainfo = '' and info_hash = '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from thread
            left join job on (job.tid = thread.tid)
            where deleted = false and seeders != 0
              and mediainfo = '' and thread.info_hash != ''
              and selected_size > 0
              and category = any($1) and job.tid is null
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from thread
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
            select job.node_id, thread.selected_size from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_DOWNLOADING,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_FAILED,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_FAILED,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select coalesce(sum(selected_size), 0) from thread
            where mediainfo != '' and info_hash != '' and category = any($1)
            """,
                SELECTED_CATEGORY,
            ),
            pool.fetch(
                """
            select job.node_id, thread.selected_size from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_DONE,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select count(1) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                SELECTED_CATEGORY,
            ),
            pool.fetchval(
                """
            select coalesce(sum(thread.selected_size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1 and thread.category = any($2)
            """,
                ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                SELECTED_CATEGORY,
            ),
            _get_weekly_byte_rate_data(),
            _get_weekly_count_data(
                "select created_at as ts from thread where created_at >= current_timestamp - interval '1 year'"
            ),
            _get_weekly_count_data(
                "select created_at as ts from torrent where created_at >= current_timestamp - interval '1 year'"
            ),
            _get_weekly_done_count_data(),
            _get_weekly_count_data(
                "select mediainfo_at as ts from thread where mediainfo_at is not null and mediainfo_at >= current_timestamp - interval '1 year'"
            ),
        )

        scraped_total = cast(int, scraped_total)
        total = cast(int, total)
        total_size = cast(int, total_size)
        pending_fetch_mediainfo = cast(int, pending_fetch_mediainfo)
        pending_fetch_torrent = cast(int, pending_fetch_torrent)
        pending_to_download = cast(int, pending_to_download)
        pending_to_download_size = cast(int, pending_to_download_size)
        failed = cast(int, failed)
        failed_size = cast(int, failed_size)
        done = cast(int, done)
        done_size = cast(int, done_size)
        removed_by_client = cast(int, removed_by_client)
        removed_by_client_size = cast(int, removed_by_client_size)
        downloading_rows = cast(list[asyncpg.Record], downloading_rows)
        done_job_rows = cast(list[asyncpg.Record], done_job_rows)

        downloading = len(downloading_rows)
        downloading_size = sum(r["selected_size"] for r in downloading_rows)

        # Per-node downloading stats
        downloading_per_node: dict[str, dict[str, Any]] = {}
        for r in downloading_rows:
            nid = str(r["node_id"])
            if nid not in downloading_per_node:
                downloading_per_node[nid] = {"count": 0, "size": 0}
            downloading_per_node[nid]["count"] += 1
            downloading_per_node[nid]["size"] += r["selected_size"]

        downloading_nodes = [
            {"node_id": nid[:8], "count": v["count"], "size_fmt": human_readable_size(v["size"])}
            for nid, v in sorted(downloading_per_node.items())
        ]

        done_per_node: dict[str, dict[str, Any]] = {}
        for r in done_job_rows:
            nid = str(r["node_id"])
            if nid not in done_per_node:
                done_per_node[nid] = {"count": 0, "size": 0}
            done_per_node[nid]["count"] += 1
            done_per_node[nid]["size"] += r["selected_size"]

        done_nodes = [
            {"node_id": nid[:8], "count": v["count"], "size_fmt": human_readable_size(v["size"])}
            for nid, v in sorted(done_per_node.items())
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
                "weekly_byte_rate": weekly_byte_rate_data,
                "weekly_thread_count": weekly_thread_count_data,
                "weekly_torrent_count": weekly_torrent_count_data,
                "weekly_done_count": weekly_done_count_data,
                "weekly_mediainfo_count": weekly_mediainfo_count_data,
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

        start_dt: datetime | None = None
        if start_value:
            start_dt = datetime.strptime(start_value, "%Y-%m-%d").replace(tzinfo=_tz_shanghai)

        (
            daily_byte_rate_data,
            daily_fetched_size_data,
            daily_thread_count_data,
            daily_torrent_count_data,
            daily_done_count_data,
            daily_mediainfo_count_data,
        ) = await asyncio.gather(
            _get_daily_byte_rate_data(start_dt),
            _get_daily_fetched_size_data(start_dt),
            _get_daily_count_data(
                "select created_at as ts from thread where created_at >= $1",
                [start_dt],
                start_dt,
            ),
            _get_daily_count_data(
                "select created_at as ts from torrent where created_at >= $1",
                [start_dt],
                start_dt,
            ),
            _get_daily_done_count_data(start_dt),
            _get_daily_count_data(
                "select mediainfo_at as ts from thread where mediainfo_at is not null and mediainfo_at >= $1",
                [start_dt],
                start_dt,
            ),
        )

        return render(
            "detail.html.j2",
            ctx={
                "start": start_value,
                "daily_byte_rate": daily_byte_rate_data,
                "daily_fetched_size": daily_fetched_size_data,
                "daily_thread_count": daily_thread_count_data,
                "daily_torrent_count": daily_torrent_count_data,
                "daily_done_count": daily_done_count_data,
                "daily_mediainfo_count": daily_mediainfo_count_data,
            },
        )

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

    @app.post("/api/thread/{tid}/reset")
    async def reset_thread(tid: int) -> ORJSONResponse:
        result = await pool.execute(
            """
            delete from job
            where tid = $1 and status = any($2)
            """,
            tid,
            [ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT, ITEM_STATUS_FAILED],
        )
        return ORJSONResponse({"deleted": result})

    @app.get("/nodes")
    async def nodes_page(render: Annotated[_Render, Depends(__render)]) -> HTMLResponse:
        node_rows = await pool.fetch("select id, last_seen from node order by last_seen desc")
        job_rows = await pool.fetch(
            "select node_id, status, count(1) as cnt from job group by node_id, status"
        )

        counts: dict[str, dict[str, int]] = {}
        for r in job_rows:
            nid = str(r["node_id"])
            counts.setdefault(nid, {})
            counts[nid][r["status"]] = r["cnt"]

        nodes_data = [
            {
                "id": str(n["id"]),
                "last_seen": n["last_seen"],
                "downloading": counts.get(str(n["id"]), {}).get(ITEM_STATUS_DOWNLOADING, 0),
                "done": counts.get(str(n["id"]), {}).get(ITEM_STATUS_DONE, 0),
                "failed": counts.get(str(n["id"]), {}).get(ITEM_STATUS_FAILED, 0),
                "removed": counts.get(str(n["id"]), {}).get(
                    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT, 0
                ),
                "total": sum(counts.get(str(n["id"]), {}).values()),
            }
            for n in node_rows
        ]

        return render("nodes.html.j2", ctx={"nodes": nodes_data})

    @app.get("/nodes/{node_id}")
    async def node_jobs_page(
        node_id: str, render: Annotated[_Render, Depends(__render)]
    ) -> HTMLResponse:
        node_row = await pool.fetchrow("select id, last_seen from node where id = $1", node_id)
        if node_row is None:
            return render("nodes.html.j2", ctx={"nodes": []}, status_code=404)

        rows = await pool.fetch(
            """
            select job.tid, job.status, job.progress, job.failed_reason,
                   job.start_download_time, job.updated_at,
                   thread.size, thread.selected_size
            from job
            join thread on (thread.tid = job.tid)
            where job.node_id = $1 and job.status = $2
            order by job.progress desc
            """,
            node_id,
            ITEM_STATUS_DOWNLOADING,
        )

        jobs = [
            dict(r)
            | {
                "size_fmt": human_readable_size(r["size"]),
                "selected_size_fmt": human_readable_size(r["selected_size"])
                if r["selected_size"] > 0
                else "-",
            }
            for r in rows
        ]

        return render(
            "node_jobs.html.j2",
            ctx={
                "node_id": str(node_row["id"]),
                "last_seen": node_row["last_seen"],
                "jobs": jobs,
            },
        )

    return app
