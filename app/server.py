from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, AsyncGenerator, Protocol

import asyncpg
import fastapi
import orjson
from fastapi import Depends, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse, JSONResponse

from app.config import load_config
from app.const import ITEM_STATUS_SKIPPED as RSS_ITEM_STATUS_SKIPPED


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
    async def threads(render: Render) -> HTMLResponse:
        torrents = await pool.fetch(
            """select * from thread where deleted = true order by tid asc"""
        )

        return render(
            "index.html.j2",
            ctx={"torrents": torrents},
        )

    @app.get("/")
    async def index(render: Render) -> HTMLResponse:
        torrents = await pool.fetch(
            """select * from job where status != $1 order by updated_at desc""",
            RSS_ITEM_STATUS_SKIPPED,
        )

        return render(
            "index.html.j2",
            ctx={"torrents": torrents},
        )

    @app.get("/thread/{tid}")
    async def rss_item(tid: int, render: Render) -> ORJSONResponse:
        rows = await pool.fetch(
            """select * from job where tid = $1""",
            tid,
        )

        return ORJSONResponse([dict(t) for t in rows])

        # return render(
        #     "rss-item.html.j2",
        #     ctx={"torrent": torrent},
        # )

    return app
