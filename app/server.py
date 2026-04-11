import asyncio
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
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


_tz_shanghai = ZoneInfo("Asia/Shanghai")

templates = Jinja2Templates(directory=str(Path(__file__).parent.joinpath("templates").resolve()))


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


def create_app() -> fastapi.FastAPI:
    cfg: Config = load_config()

    pool = asyncpg.create_pool(cfg.pg_dsn())

    @asynccontextmanager
    async def lifespan(_app: fastapi.FastAPI) -> AsyncGenerator[None, None]:
        await pool
        yield
        await pool.close()

    app = fastapi.FastAPI(debug=cfg.debug, lifespan=lifespan)

    def _fmt_dt(dt: datetime | None) -> str:
        if dt is None:
            return "-"
        return dt.astimezone(_tz_shanghai).strftime("%Y-%m-%d %H:%M:%S")

    templates.env.filters["fmt_dt"] = _fmt_dt

    def _today_start() -> datetime:
        now = datetime.now(_tz_shanghai)
        return datetime(now.year, now.month, now.day, tzinfo=_tz_shanghai)

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

    def _build_weekly_byte_rate_data(
        rows: list[asyncpg.Record],
    ) -> dict[str, Any]:
        filtered = [r for r in rows if r["selected_size"] > 0]
        if not filtered:
            return {"labels": [], "totals": [], "per_node": {}}

        today = _today_start()
        df = pl.DataFrame({
            "ts": [row["ts"] for row in filtered],
            "selected_size": [row["selected_size"] for row in filtered],
            "node_id": [str(row["node_id"]) for row in filtered],
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

    def _build_weekly_count_data(
        ts_values: list[datetime],
    ) -> list[dict[str, Any]]:
        if not ts_values:
            return []
        today = _today_start()
        df = pl.DataFrame({"ts": ts_values})
        df = df.with_columns(_compute_week_num_col(today))
        result = _fill_week_gaps_count(df)
        return [
            {"week": _week_label(today, r["week_num"]), "count": int(r["count"])}
            for r in result.iter_rows(named=True)
        ]

    def _build_weekly_done_count_data(
        rows: list[asyncpg.Record],
    ) -> dict[str, Any]:
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

    def _build_daily_byte_rate_data(
        rows: list[asyncpg.Record],
        days_back: int,
    ) -> dict[str, Any]:
        filtered = [r for r in rows if r["selected_size"] > 0]
        if not filtered:
            return {"labels": [], "totals": [], "per_node": {}}

        today = _today_start()
        elapsed_today = (datetime.now(_tz_shanghai) - today).total_seconds()

        df = pl.DataFrame({
            "ts": [row["ts"] for row in filtered],
            "selected_size": [row["selected_size"] for row in filtered],
            "node_id": [str(row["node_id"]) for row in filtered],
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

    def _build_daily_fetched_size_data(
        rows: list[asyncpg.Record],
        days_back: int,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        today = _today_start()
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

    def _build_daily_count_data(
        ts_values: list[datetime],
        days_back: int,
    ) -> list[dict[str, Any]]:
        if not ts_values:
            return []
        today = _today_start()
        df = pl.DataFrame({"ts": ts_values})
        df = df.with_columns(_compute_day_num_col(today))
        result = _fill_day_gaps_count(df, days_back)
        return [
            {"day": _day_label(today, r["day_num"]), "count": int(r["count"])}
            for r in result.iter_rows(named=True)
        ]

    def _build_daily_done_count_data(
        rows: list[asyncpg.Record],
        days_back: int,
    ) -> dict[str, Any]:
        if not rows:
            return {"labels": [], "totals": [], "per_node": {}}

        today = _today_start()
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
    async def progress(render: Render) -> HTMLResponse:
        (
            thread_stats,
            search_cursor,
            pending_download_stats,
            all_job_rows,
        ) = await asyncio.gather(
            pool.fetchrow(
                """
            select
              count(1) as scraped_total,
              count(1) filter (where category = any($1)) as total,
              coalesce(sum(selected_size) filter (where category = any($1)), 0) as total_size,
              count(1) filter (where deleted = false and mediainfo_at is null
                and upload_at >= '2024-01-01' and category = any($1)) as pending_fetch_mediainfo,
              count(1) filter (where deleted = false and mediainfo_at is not null
                and mediainfo = '' and info_hash = '' and category = any($1)) as pending_fetch_torrent,
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
            select job.status, job.node_id, thread.selected_size from job
            join thread on (thread.tid = job.tid)
            where thread.category = any($1)
              and job.status = any($2)
            """,
                SELECTED_CATEGORY,
                [
                    ITEM_STATUS_DOWNLOADING,
                    ITEM_STATUS_DONE,
                    ITEM_STATUS_FAILED,
                    ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT,
                ],
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

        all_job_rows = cast(list[asyncpg.Record], all_job_rows)
        downloading_rows = [r for r in all_job_rows if r["status"] == ITEM_STATUS_DOWNLOADING]
        done_job_rows = [r for r in all_job_rows if r["status"] == ITEM_STATUS_DONE]
        failed = sum(1 for r in all_job_rows if r["status"] == ITEM_STATUS_FAILED)
        failed_size = sum(
            r["selected_size"] for r in all_job_rows if r["status"] == ITEM_STATUS_FAILED
        )
        removed_by_client = sum(
            1 for r in all_job_rows if r["status"] == ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT
        )
        removed_by_client_size = sum(
            r["selected_size"]
            for r in all_job_rows
            if r["status"] == ITEM_STATUS_REMOVED_FROM_DOWNLOAD_CLIENT
        )

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
            },
        )

    @app.get("/api/weekly-charts")
    async def weekly_charts() -> ORJSONResponse:
        weekly_done_job_rows, weekly_thread_rows, weekly_torrent_rows = await asyncio.gather(
            pool.fetch(
                """
            select coalesce(job.completed_at, job.updated_at) as ts,
                   thread.selected_size, job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1
              and coalesce(job.completed_at, job.updated_at) >= current_timestamp - interval '1 year'
            """,
                ITEM_STATUS_DONE,
            ),
            pool.fetch(
                """
            select created_at, mediainfo_at from thread
            where created_at >= current_timestamp - interval '1 year'
               or (mediainfo_at is not null
                   and mediainfo_at >= current_timestamp - interval '1 year')
            """,
            ),
            pool.fetch(
                "select created_at as ts from torrent where created_at >= current_timestamp - interval '1 year'"
            ),
        )

        weekly_done_job_rows = cast(list[asyncpg.Record], weekly_done_job_rows)
        weekly_byte_rate_data = _build_weekly_byte_rate_data(weekly_done_job_rows)
        weekly_done_count_data = _build_weekly_done_count_data(weekly_done_job_rows)

        weekly_thread_rows = cast(list[asyncpg.Record], weekly_thread_rows)
        weekly_thread_count_data = _build_weekly_count_data([
            r["created_at"] for r in weekly_thread_rows if r["created_at"] is not None
        ])
        weekly_mediainfo_count_data = _build_weekly_count_data([
            r["mediainfo_at"] for r in weekly_thread_rows if r["mediainfo_at"] is not None
        ])

        weekly_torrent_rows = cast(list[asyncpg.Record], weekly_torrent_rows)
        weekly_torrent_count_data = _build_weekly_count_data([r["ts"] for r in weekly_torrent_rows])

        return ORJSONResponse({
            "weekly_byte_rate": weekly_byte_rate_data,
            "weekly_thread_count": weekly_thread_count_data,
            "weekly_torrent_count": weekly_torrent_count_data,
            "weekly_done_count": weekly_done_count_data,
            "weekly_mediainfo_count": weekly_mediainfo_count_data,
        })

    @app.get("/detail")
    async def detail(render: Render, start: str | None = None) -> HTMLResponse:
        today = _today_start()
        default_start = (today - timedelta(days=364)).strftime("%Y-%m-%d")
        start_value = start if start is not None else default_start

        start_dt: datetime | None = None
        if start_value:
            start_dt = datetime.strptime(start_value, "%Y-%m-%d").replace(tzinfo=_tz_shanghai)

        if start_dt is None:
            start_dt = today - timedelta(days=364)
        start_dt = start_dt.replace(tzinfo=_tz_shanghai) if start_dt.tzinfo is None else start_dt
        days_back = (today - start_dt).days

        (
            daily_done_job_rows,
            daily_fetched_rows,
            daily_thread_rows,
            daily_torrent_rows,
        ) = await asyncio.gather(
            pool.fetch(
                """
            select coalesce(job.completed_at, job.updated_at) as ts,
                   thread.selected_size, job.node_id
            from job
            join thread on (thread.tid = job.tid)
            where job.status = $1
              and coalesce(job.completed_at, job.updated_at) >= $2
            """,
                ITEM_STATUS_DONE,
                start_dt,
            ),
            pool.fetch(
                """
            select torrent_fetched_at as ts, selected_size
            from thread
            where torrent_fetched_at >= $1 and selected_size > 0
              and category = any($2)
            """,
                start_dt,
                SELECTED_CATEGORY,
            ),
            pool.fetch(
                """
            select created_at, mediainfo_at from thread
            where created_at >= $1
               or (mediainfo_at is not null and mediainfo_at >= $1)
            """,
                start_dt,
            ),
            pool.fetch(
                "select created_at as ts from torrent where created_at >= $1",
                start_dt,
            ),
        )

        daily_done_job_rows = cast(list[asyncpg.Record], daily_done_job_rows)
        daily_byte_rate_data = _build_daily_byte_rate_data(daily_done_job_rows, days_back)
        daily_done_count_data = _build_daily_done_count_data(daily_done_job_rows, days_back)

        daily_fetched_rows = cast(list[asyncpg.Record], daily_fetched_rows)
        daily_fetched_size_data = _build_daily_fetched_size_data(daily_fetched_rows, days_back)

        daily_thread_rows = cast(list[asyncpg.Record], daily_thread_rows)
        daily_thread_count_data = _build_daily_count_data(
            [r["created_at"] for r in daily_thread_rows if r["created_at"] is not None],
            days_back,
        )
        daily_mediainfo_count_data = _build_daily_count_data(
            [r["mediainfo_at"] for r in daily_thread_rows if r["mediainfo_at"] is not None],
            days_back,
        )

        daily_torrent_rows = cast(list[asyncpg.Record], daily_torrent_rows)
        daily_torrent_count_data = _build_daily_count_data(
            [r["ts"] for r in daily_torrent_rows],
            days_back,
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
    async def threads_pending_mediainfo(render: Render) -> HTMLResponse:
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
    async def threads_pending_torrent(render: Render) -> HTMLResponse:
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
    async def threads_pending_download(render: Render) -> HTMLResponse:
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
    async def threads_downloading(render: Render) -> HTMLResponse:
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
    async def threads_done(render: Render) -> HTMLResponse:
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
    async def threads_failed(render: Render) -> HTMLResponse:
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
    async def threads_removed(render: Render) -> HTMLResponse:
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
    async def nodes_page(render: Render) -> HTMLResponse:
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

    def _fmt_eta(seconds: float) -> str:
        if seconds <= 0:
            return "∞"
        if seconds < 60:
            return f"{int(seconds)}s"
        if seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    @app.get("/nodes/{node_id}")
    async def node_jobs_page(node_id: str, render: Render) -> HTMLResponse:
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
            """,
            node_id,
            ITEM_STATUS_DOWNLOADING,
        )

        now = datetime.now(UTC)

        def _calc_speed_eta(r: asyncpg.Record) -> dict[str, Any]:
            selected_size: int = r["selected_size"]
            progress: float = r["progress"]
            start: datetime | None = r["start_download_time"]
            if not start or selected_size <= 0 or progress <= 0:
                return {"speed_fmt": "-", "eta_fmt": "-", "eta_seconds": float("inf")}
            elapsed = (now - start).total_seconds()
            if elapsed <= 0:
                return {"speed_fmt": "-", "eta_fmt": "-", "eta_seconds": float("inf")}
            bytes_done = selected_size * progress
            speed = bytes_done / elapsed
            remaining = selected_size * (1 - progress)
            eta_seconds = remaining / speed if speed > 0 else float("inf")
            return {
                "speed_fmt": human_readable_byte_rate(speed),
                "eta_fmt": _fmt_eta(eta_seconds),
                "eta_seconds": eta_seconds,
            }

        jobs = sorted(
            [
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
            ],
            key=lambda j: j["eta_seconds"],
        )

        return render(
            "node_jobs.html.j2",
            ctx={
                "node_id": str(node_row["id"]),
                "last_seen": node_row["last_seen"],
                "jobs": jobs,
            },
        )

    return app
