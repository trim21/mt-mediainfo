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

from app.config import load_config
from app.const import (
    ITEM_STATUS_DONE,
    ITEM_STATUS_DOWNLOADING,
    ITEM_STATUS_FAILED,
    ITEM_STATUS_SKIPPED,
    SELECTED_CATEGORY,
)
from app.utils import human_readable_size


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
    cfg = load_config()

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

    Render = Annotated[_Render, Depends(__render)]

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
    async def index(render: Render) -> HTMLResponse:
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

    @app.get("/progress")
    async def progress(render: Render) -> HTMLResponse:
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

        # Download bottleneck: avg progress & avg duration of active downloads
        dl_stats = await pool.fetchrow(
            """
            select
              coalesce(avg(progress), 0) as avg_progress,
              coalesce(avg(extract(epoch from (current_timestamp - start_download_time))), 0) as avg_duration_sec
            from job where status = $1
            """,
            ITEM_STATUS_DOWNLOADING,
        )
        avg_dl_progress = float(dl_stats["avg_progress"]) * 100 if dl_stats else 0
        avg_dl_duration_sec = float(dl_stats["avg_duration_sec"]) if dl_stats else 0

        # Completed job stats: avg time from start to finish
        avg_completed_sec = float(
            await pool.fetchval(
                """
            select coalesce(avg(extract(epoch from (updated_at - start_download_time))), 0)
            from job where status = $1
            """,
                ITEM_STATUS_DONE,
            )
            or 0
        )

        skipped = total - done - in_progress - failed - pending

        def pct(n: int) -> str:
            if total == 0:
                return "0.0"
            return f"{n / total * 100:.1f}"

        def fmt_duration(seconds: float) -> str:
            if seconds <= 0:
                return "-"
            m, s = divmod(int(seconds), 60)
            h, m = divmod(m, 60)
            if h > 0:
                return f"{h}h {m}m {s}s"
            if m > 0:
                return f"{m}m {s}s"
            return f"{s}s"

        return render(
            "progress.html.j2",
            ctx={
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
                "avg_dl_progress": f"{avg_dl_progress:.1f}",
                "avg_dl_duration": fmt_duration(avg_dl_duration_sec),
                "avg_completed_duration": fmt_duration(avg_completed_sec),
            },
        )

    return app
