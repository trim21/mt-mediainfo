from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Protocol

import asyncpg
import fastapi
import orjson
from fastapi import Depends, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, JSONResponse

from app.config import Config, load_config
from app.const import (
    ITEM_STATUS_DONE,
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_SKIPPED,
    SELECTED_CATEGORY,
)
from app.scrape import known_max_id
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

    @app.get("/stats/weekly-byte-rate")
    async def weekly_byte_rate() -> ORJSONResponse:
        rows = await pool.fetch(
            """
            select
                date_trunc('week', updated_at) as week_start,
                sum(download_size) as total_size,
                (sum(download_size) / (7.0 * 86400))::float8 as avg_byte_rate
            from job
            where
                status = $1 and
                updated_at >= current_timestamp - interval '2 years'
            group by week_start
            order by week_start
            """,
            ITEM_STATUS_DONE,
        )
        return ORJSONResponse([
            {
                "week": row["week_start"].isoformat(),
                "byte_rate": row["avg_byte_rate"],
                "total_size": int(row["total_size"]),
            }
            for row in rows
        ])

    @app.get("/progress")
    async def progress(render: Annotated[_Render, Depends(__render)]) -> HTMLResponse:
        # Scraping progress: how far along we are in scraping thread details
        scraped_total = await pool.fetchval("select count(1) from thread") or 0
        scraped_max_tid = await pool.fetchval("select max(tid) from thread") or 0

        total = (
            await pool.fetchval(
                "select count(1) from thread where category = any($1)",
                SELECTED_CATEGORY,
            )
            or 0
        )

        done = (
            await pool.fetchval(
                "select count(1) from thread where mediainfo != '' and category = any($1)",
                SELECTED_CATEGORY,
            )
            or 0
        )

        done_size = (
            await pool.fetchval(
                "select coalesce(sum(size), 0) from thread where mediainfo != '' and category = any($1)",
                SELECTED_CATEGORY,
            )
            or 0
        )

        in_progress = (
            await pool.fetchval(
                """select count(1) from job where status = $1""",
                ITEM_STATUS_DOWNLOADING,
            )
            or 0
        )

        failed = (
            await pool.fetchval(
                """select count(1) from job where status = $1""",
                ITEM_STATUS_FAILED,
            )
            or 0
        )

        pending = (
            await pool.fetchval(
                """
            select count(1) from thread
            left join job on (job.tid = thread.tid)
            where
              deleted = false and
              seeders != 0 and
              thread.info_hash != '' and
              mediainfo = '' and
              category = any($1) and
              job.tid is null
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        pending_size = (
            await pool.fetchval(
                """
            select coalesce(sum(size), 0) from thread
            left join job on (job.tid = thread.tid)
            where
              deleted = false and
              seeders != 0 and
              thread.info_hash != '' and
              mediainfo = '' and
              category = any($1) and
              job.tid is null
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        downloading_size = (
            await pool.fetchval(
                """
            select coalesce(sum(thread.size), 0) from job
            join thread on (thread.tid = job.tid)
            where job.status = $1
            """,
                ITEM_STATUS_DOWNLOADING,
            )
            or 0
        )

        # Download byte rate statistics
        rate_stats = await pool.fetchrow(
            """
            select
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '1 day'
                ), 0) as size_1d,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '3 days'
                ), 0) as size_3d,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '7 days'
                ), 0) as size_1w,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '14 days'
                ), 0) as size_2w,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '30 days'
                ), 0) as size_1m,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '90 days'
                ), 0) as size_3m,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '180 days'
                ), 0) as size_6m,
                coalesce(sum(job.download_size) filter (
                    where job.updated_at >= current_timestamp - interval '365 days'
                ), 0) as size_1y
            from job
            where job.status = $1
            """,
            ITEM_STATUS_DONE,
        )
        size_1d = int(rate_stats["size_1d"])
        size_3d = int(rate_stats["size_3d"])
        size_1w = int(rate_stats["size_1w"])
        size_2w = int(rate_stats["size_2w"])
        size_1m = int(rate_stats["size_1m"])
        size_3m = int(rate_stats["size_3m"])
        size_6m = int(rate_stats["size_6m"])
        size_1y = int(rate_stats["size_1y"])
        byte_rate_1d = human_readable_byte_rate(size_1d / 86400)
        byte_rate_3d = human_readable_byte_rate(size_3d / (3 * 86400))
        byte_rate_1w = human_readable_byte_rate(size_1w / (7 * 86400))
        byte_rate_2w = human_readable_byte_rate(size_2w / (14 * 86400))
        byte_rate_1m = human_readable_byte_rate(size_1m / (30 * 86400))
        byte_rate_3m = human_readable_byte_rate(size_3m / (90 * 86400))
        byte_rate_6m = human_readable_byte_rate(size_6m / (180 * 86400))
        byte_rate_1y = human_readable_byte_rate(size_1y / (365 * 86400))

        # API bottleneck: threads scraped but torrent not yet fetched
        missing_torrent = (
            await pool.fetchval(
                """
            select count(1) from thread
            where
              deleted = false and
              seeders != 0 and
              info_hash = '' and
              mediainfo = '' and
              category = any($1)
            """,
                SELECTED_CATEGORY,
            )
            or 0
        )

        skipped = total - done - in_progress - failed - pending

        def pct(n: int) -> str:
            if total == 0:
                return "0.0"
            return f"{n / total * 100:.1f}"

        scrape_pct = f"{scraped_max_tid / known_max_id * 100:.1f}" if known_max_id else "0.0"

        return render(
            "progress.html.j2",
            ctx={
                "scraped_total": scraped_total,
                "scraped_max_tid": scraped_max_tid,
                "known_max_id": known_max_id,
                "scrape_pct": scrape_pct,
                "total": total,
                "done": done,
                "done_size": human_readable_size(done_size),
                "done_pct": pct(done),
                "in_progress": in_progress,
                "in_progress_pct": pct(in_progress),
                "failed": failed,
                "failed_pct": pct(failed),
                "pending": pending,
                "pending_size": human_readable_size(pending_size),
                "skipped": skipped,
                "missing_torrent": missing_torrent,
                "downloading_size": human_readable_size(downloading_size),
                "total_process_size": human_readable_size(cfg.total_process_size),
                "single_torrent_size_limit": human_readable_size(cfg.single_torrent_size_limit),
                "byte_rate_1d": byte_rate_1d,
                "byte_rate_3d": byte_rate_3d,
                "byte_rate_1w": byte_rate_1w,
                "byte_rate_2w": byte_rate_2w,
                "byte_rate_1m": byte_rate_1m,
                "byte_rate_3m": byte_rate_3m,
                "byte_rate_6m": byte_rate_6m,
                "byte_rate_1y": byte_rate_1y,
            },
        )

    return app
